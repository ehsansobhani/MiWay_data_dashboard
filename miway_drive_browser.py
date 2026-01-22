import io
import json
from datetime import datetime, date, timedelta, time as dtime
from typing import List, Optional, Dict, Tuple

import pandas as pd
import pyarrow.parquet as pq
import streamlit as st

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

import tempfile
from pathlib import Path


# -----------------------------
# UI CONFIG
# -----------------------------
st.set_page_config(
    page_title="MiWay Transit Data Browser",
    layout="wide",
    page_icon="assets/favicon.ico",  # 
)

# cols = st.columns([1, 6])
# with cols[0]:
#     st.image("assets/logo.png", width=140)
# with cols[1]:
#     st.title("MiWay Transit Data Browser")

st.title("MiWay Transit Data Browser")
st.markdown(
    """
    <style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    </style>
    """,
    unsafe_allow_html=True,
)

FEEDS = ["vehicle_positions", "trip_updates", "alerts"]

# Your Drive structure (as created by your logger):
# <ROOT>/<agency_slug>/raw/<feed>/date=YYYY-MM-DD/hour=HH/part-xxxxx.parquet
AGENCY_SLUG_DEFAULT = "miway"


# -----------------------------
# AUTH + DRIVE HELPERS
# -----------------------------
@st.cache_resource(show_spinner=False)
def get_drive_service(sa_json_str: str):
    # for debug
    # st.write("SA JSON length:", len(sa_json_str))
    # st.write("SA JSON starts with:", sa_json_str[:30])
   creds = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/drive.readonly"],
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def drive_list_children(service, parent_id: str, only_folders: Optional[bool] = None) -> List[Dict]:
    q = [f"'{parent_id}' in parents", "trashed=false"]
    if only_folders is True:
        q.append("mimeType='application/vnd.google-apps.folder'")
    if only_folders is False:
        q.append("mimeType!='application/vnd.google-apps.folder'")

    query = " and ".join(q)

    out = []
    page_token = None
    while True:
        resp = service.files().list(
            q=query,
            fields="nextPageToken, files(id,name,mimeType,size,modifiedTime)",
            pageToken=page_token,
            pageSize=1000,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute()
        out.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return out


@st.cache_data(show_spinner=False, ttl=300)
def drive_find_child_folder_id(_service, parent_id: str, folder_name: str) -> Optional[str]:
    q = (
        f"'{parent_id}' in parents and trashed=false and "
        f"mimeType='application/vnd.google-apps.folder' and name='{folder_name}'"
    )

    resp = _service.files().list(
        q=q,
        fields="files(id,name)",
        pageSize=10,
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()

    files = resp.get("files", [])
    return files[0]["id"] if files else None


def drive_download_bytes(service, file_id: str, progress_cb=None) -> bytes:
    request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request, chunksize=1024 * 1024)  # 1MB chunks
    done = False
    while not done:
        status, done = downloader.next_chunk()
        if progress_cb and status:
            progress_cb(status.progress())
    return fh.getvalue()


def parse_dt(s: str) -> datetime:
    # expects ISO string
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


# -----------------------------
# PATH RESOLUTION (ROOT -> miway -> raw -> feed -> date= -> hour= -> parquet files)
# -----------------------------
@st.cache_data(show_spinner=False, ttl=300)
def resolve_folder_path(_service, root_id: str, agency_slug: str, feed: str) -> Optional[str]:
    # root -> agency_slug -> raw -> feed
    agency_id = drive_find_child_folder_id(_service, root_id, agency_slug)
    if not agency_id:
        return None

    raw_id = drive_find_child_folder_id(_service, agency_id, "raw")
    if not raw_id:
        return None

    feed_id = drive_find_child_folder_id(_service, raw_id, feed)
    return feed_id

def daterange(d0: date, d1: date):
    d = d0
    while d <= d1:
        yield d
        d += timedelta(days=1)


def time_to_hour(t: dtime) -> int:
    return int(t.hour)


def hour_folder_name(h: int) -> str:
    return f"hour={h:02d}"


def date_folder_name(d: date) -> str:
    return f"date={d.isoformat()}"


# -----------------------------
# LOAD DATA (SELECTIVE DOWNLOAD)
# -----------------------------
def list_candidate_parquet_files(
    service,
    feed_root_id: str,
    start_date: date,
    end_date: date,
    start_time: Optional[dtime],
    end_time: Optional[dtime],
) -> List[Tuple[str, str, str]]:
    """
    Returns list of (file_id, file_name, logical_partition) for parquet parts.
    logical_partition is like "date=YYYY-MM-DD/hour=HH"
    """
    files_out = []

    # hour bounds
    if start_time and end_time:
        h0 = time_to_hour(start_time)
        h1 = time_to_hour(end_time)
        hour_range = set(range(min(h0, h1), max(h0, h1) + 1))
    else:
        hour_range = None

    for d in daterange(start_date, end_date):
        dname = date_folder_name(d)
        date_id = drive_find_child_folder_id(service, feed_root_id, dname)
        if not date_id:
            continue

        # list hour folders under date
        hour_folders = drive_list_children(service, date_id, only_folders=True)
        for hf in hour_folders:
            hname = hf["name"]  # hour=HH
            try:
                hh = int(hname.split("=")[1])
            except Exception:
                continue

            if hour_range is not None and hh not in hour_range:
                continue

            hour_id = hf["id"]

            # list parquet parts inside hour folder
            parts = drive_list_children(service, hour_id, only_folders=False)
            for p in parts:
                if p["name"].endswith(".parquet"):
                    logical = f"{dname}/{hname}"
                    files_out.append((p["id"], p["name"], logical))

    # sort by logical partition then filename
    files_out.sort(key=lambda x: (x[2], x[1]))
    return files_out

# restrected parquet download version
# def read_parquet_files_with_progress(
#     service,
#     file_tuples: List[Tuple[str, str, str]],
#     progress_bar,
#     status_box,
#     max_files: int = 300,
# ) -> pd.DataFrame:
def read_parquet_files_with_progress(
    service,
    file_tuples,
    progress_bar,
    status_box,
) -> pd.DataFrame:

    if not file_tuples:
        return pd.DataFrame()
    # this part restricted number of downloaded parquet for safety
    # if len(file_tuples) > max_files:
    #     status_box.warning(
    #         f"Too many Parquet parts selected ({len(file_tuples)}). "
    #         f"Limiting to first {max_files} files to avoid exploding memory."
    #     )
    #     file_tuples = file_tuples[:max_files]

    dfs = []
    n = len(file_tuples)

    for i, (fid, fname, logical) in enumerate(file_tuples, start=1):
        status_box.info(f"Downloading {i}/{n}: {logical}/{fname}")

        def _chunk_progress(p):
            # combine file-level and chunk-level progress
            base = (i - 1) / n
            progress_bar.progress(min(0.999, base + (p / n)))

        b = drive_download_bytes(service, fid, progress_cb=_chunk_progress)

        # read parquet from bytes
        table = pq.read_table(io.BytesIO(b))
        df = table.to_pandas()
        df["_drive_partition"] = logical
        dfs.append(df)

        progress_bar.progress(min(0.999, i / n))

    progress_bar.progress(1.0)
    status_box.success(f"Loaded {len(dfs)} files.")
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True, copy=False)

def export_csv_streaming(
    service,
    file_tuples,
    status_box,
    progress_bar,
    output_path: str,
    keep_cols=None,
) -> str:

# def export_csv_streaming(
#     service,
#     file_tuples: List[Tuple[str, str, str]],
#     status_box,
#     progress_bar,
#     keep_cols: Optional[List[str]] = None,
#     out_name: str = "export.csv",
# ) -> str:
    """
    Downloads each parquet file from Drive, reads it, and appends rows to one CSV file on disk.
    Returns the path to the created CSV file.
    """
    if not file_tuples:
        raise RuntimeError("No parquet files found for selected date range.")

    # Create a temp CSV file (persisted so we can download it)
    # tmp_dir = Path(tempfile.gettempdir())
    # out_path = str(tmp_dir / out_name)

    # # Overwrite if exists
    # try:
    #     Path(out_path).unlink(missing_ok=True)
    # except Exception:
    #     pass
    out_path = Path(output_path).expanduser()

    out_path.parent.mkdir(parents=True, exist_ok=True)

    if out_path.exists():
        out_path.unlink()

    n = len(file_tuples)
    wrote_header = False
    total_rows = 0

    status_box.info(f"Export started. Files to process: {n:,}")
    progress_bar.progress(0.0)

    for i, (fid, fname, logical) in enumerate(file_tuples, start=1):
        # status_box.info(f"Processing {i:,}/{n:,}: {logical}/{fname}")
        status_box.info(f"Processing {i:,}/{n:,}: {logical}")


        def _chunk_progress(p):
            # File-level + chunk-level
            base = (i - 1) / n
            progress_bar.progress(min(0.999, base + (p / n)))

        # Download parquet bytes
        b = drive_download_bytes(service, fid, progress_cb=_chunk_progress)

        # Read parquet with column projection (saves memory)
        table = pq.read_table(io.BytesIO(b), columns=keep_cols if keep_cols else None)
        df = table.to_pandas()

        # Append to CSV
        df.to_csv(
            out_path,
            mode="a",
            index=False,
            header=(not wrote_header),
        )
        wrote_header = True
        total_rows += len(df)

        progress_bar.progress(min(0.999, i / n))

    progress_bar.progress(1.0)
    status_box.success(f"Done. Wrote {total_rows:,} rows into CSV: {out_path}")
    # return out_path
    return str(out_path)



# -----------------------------
# STREAMLIT UI
# -----------------------------

# sa_json_path = st.secrets.get("GDRIVE_SA_JSON_PATH", "")
# if not sa_json_path:
#     st.error("Missing GDRIVE_SA_JSON_PATH in secrets.")
#     st.stop()

# # with open(sa_json_path, "r", encoding="utf-8") as f:
#     # sa_json_str = f.read()
sa_json_str = st.secrets.get("GDRIVE_SA_JSON", "")
if not sa_json_str:
    st.error("Missing GDRIVE_SA_JSON in Streamlit secrets.")
    st.stop()

service = get_drive_service(sa_json_str)

# service = get_drive_service(sa_json_str)

# with st.sidebar:
#     st.header("Google Drive Connection")

#     # --- secrets ---
root_folder_id_default = st.secrets.get("GDRIVE_ROOT_FOLDER_ID", "")
sa_json_path = st.secrets.get("GDRIVE_SA_JSON_PATH", "")


root_folder_id = st.secrets.get("GDRIVE_ROOT_FOLDER_ID", "")
agency_slug = AGENCY_SLUG_DEFAULT  # fixed, not user-editable

if not root_folder_id:
    st.error("Missing GDRIVE_ROOT_FOLDER_ID in .streamlit/secrets.toml")
    st.stop()

#     # --- UI inputs ---
# root_folder_id = st.text_input(
#         "Drive ROOT folder ID",
#         value=root_folder_id_default,
#         help="The folder that contains miway/ ...",
#     )
# agency_slug = st.text_input("Agency slug", value=AGENCY_SLUG_DEFAULT)

    # --- hard checks ---
if not sa_json_path:
        st.error("Missing GDRIVE_SA_JSON_PATH in secrets.toml")
        st.stop()

if not root_folder_id:
        st.error("Missing GDRIVE_ROOT_FOLDER_ID (paste it or put it in secrets.toml)")
        st.stop()

with open(sa_json_path, "r", encoding="utf-8") as f:
    sa_json_str = f.read()

service = get_drive_service(sa_json_str)

colA, colB, colC = st.columns([1, 1, 2])

with colA:
    feed = st.selectbox("Feed", FEEDS, index=0)

with colB:
    today = date.today()
    start_d = st.date_input("Start date", value=today - timedelta(days=1))
    end_d = st.date_input("End date", value=today)

with colC:
    use_time_filter = st.checkbox("Filter by hour range (UTC hour)", value=False)
    if use_time_filter:
        t0 = st.time_input("Start time", value=dtime(0, 0))
        t1 = st.time_input("End time", value=dtime(23, 59))
    else:
        t0 = None
        t1 = None

st.caption(
    "Note: Your folders are partitioned by **UTC time** (`date=.../hour=...`). "
    "If you want local time filtering, convert after loading."
)

# Resolve folder path
feed_root_id = resolve_folder_path(service, root_folder_id, agency_slug, feed)
if not feed_root_id:
    st.error(f"Could not find folder path: ROOT/{agency_slug}/raw/{feed} on Drive. Check IDs/names.")
    st.stop()

with st.expander("Advanced controls", expanded=True):
#     max_files = st.slider("Max parquet files to download (safety)", 10, 1000, 300, step=10)
    preview_rows = st.slider("Preview rows", 10, 500, 50, step=10)
    show_partition_col = st.checkbox("Keep _drive_partition column", value=True)

# Busy state
if "busy" not in st.session_state:
    st.session_state.busy = False

btn_col1, btn_col2 = st.columns([1, 4])
with btn_col1:
    load_clicked = st.button("Load / Refresh", disabled=st.session_state.busy)

status_box = st.empty()
progress_bar = st.progress(0)

df = None


st.subheader("Export to")

if "save_path" not in st.session_state:
    st.session_state["save_path"] = str(Path.home() / f"miway_{feed}_{start_d}_to_{end_d}.csv")

save_path = st.text_input(
    "Save CSV to (full file path)",
    value=st.session_state["save_path"],
    help="Example: C:\\Users\\Ehsan\\Desktop\\miway.csv",
)

st.session_state["save_path"] = save_path

if load_clicked:
    st.session_state.busy = True
    save_path = st.session_state["save_path"]
    try:
        status_box.info("Listing candidate files from Drive...")
        progress_bar.progress(0)

        file_tuples = list_candidate_parquet_files(
            service=service,
            feed_root_id=feed_root_id,
            start_date=min(start_d, end_d),
            end_date=max(start_d, end_d),
            start_time=t0,
            end_time=t1,
        )

        status_box.info(f"Found {len(file_tuples)} files to consider.")




        # restericted parquet download version
        # df = read_parquet_files_with_progress(
        #     service=service,
        #     file_tuples=file_tuples,
        #     progress_bar=progress_bar,
        #     status_box=status_box,
        #     max_files=max_files,
        # )
        # unrestericted parquet download version
        # df = read_parquet_files_with_progress(
        #     service=service,
        #     file_tuples=file_tuples,
        #     progress_bar=progress_bar,
        #     status_box=status_box,
        # )

        # if df.empty:
        #     status_box.warning("No data loaded for the selected range.")
        #     st.session_state["df_loaded"] = None
        # else:
        #     if not show_partition_col and "_drive_partition" in df.columns:
        #         df = df.drop(columns=["_drive_partition"])

        #     st.session_state["df_loaded"] = df




        # csv_path = export_csv_streaming(
        #     service=service,
        #     file_tuples=file_tuples,
        #     status_box=status_box,
        #     progress_bar=progress_bar,
        #     keep_cols=None,  # None = export ALL columns
        #     out_name=f"miway_{feed}_{min(start_d,end_d)}_to_{max(start_d,end_d)}.csv",
        # )
        if not save_path.lower().endswith(".csv"):
            status_box.error("Output path must end with .csv")
            st.stop()

        csv_path = export_csv_streaming(
            service=service,
            file_tuples=file_tuples,
            status_box=status_box,
            progress_bar=progress_bar,
            output_path=save_path,
            keep_cols=None,
        )

        st.session_state["csv_path"] = csv_path

    except Exception as e:
        status_box.error(f"ERROR: {type(e).__name__}: {e}")
        st.session_state["df_loaded"] = None
    finally:
        st.session_state.busy = False
        progress_bar.progress(0)

# Display loaded df from session state
df_loaded = st.session_state.get("df_loaded", None)
if isinstance(df_loaded, pd.DataFrame) and not df_loaded.empty:
    st.subheader("Filter + Export")

    all_cols = list(df_loaded.columns)

    # Column picker
    keep_cols = st.multiselect(
        "Columns to include in CSV",
        options=all_cols,
        default=all_cols,
    )

    # Optional datetime filter inside data (if present)
    # Many of your RT tables have fetched_at_utc as ISO.
    dt_col_candidates = [c for c in ["fetched_at_utc"] if c in df_loaded.columns]
    dt_col = st.selectbox("Datetime column for finer filtering (optional)", ["(none)"] + dt_col_candidates)

    df_work = df_loaded.copy()

    if dt_col != "(none)":
        # parse to datetime (UTC)
        with st.spinner(f"Parsing {dt_col} to datetime..."):
            df_work[dt_col] = pd.to_datetime(df_work[dt_col], errors="coerce", utc=True)

        # finer filter by full datetime range (UTC)
        c1, c2 = st.columns(2)
        with c1:
            dt_start = st.datetime_input("Datetime start (UTC)", value=datetime.combine(min(start_d, end_d), dtime(0, 0)))
        with c2:
            dt_end = st.datetime_input("Datetime end (UTC)", value=datetime.combine(max(start_d, end_d), dtime(23, 59)))

        dt_start = pd.to_datetime(dt_start, utc=True)
        dt_end = pd.to_datetime(dt_end, utc=True)

        before = len(df_work)
        df_work = df_work[(df_work[dt_col] >= dt_start) & (df_work[dt_col] <= dt_end)]
        st.write(f"Datetime filter kept **{len(df_work):,} / {before:,}** rows.")

    # Apply column selection last
    if keep_cols:
        df_work = df_work[keep_cols]
    else:
        st.warning("No columns selected — CSV export disabled.")
        df_work = df_work.iloc[0:0]

    st.write("Preview:")
    st.dataframe(df_work.head(preview_rows), use_container_width=True)

    # Export CSV (in memory)
    if not df_work.empty and keep_cols:
        with st.spinner("Building CSV..."):
            csv_bytes = df_work.to_csv(index=False).encode("utf-8")

        st.download_button(
            label="Download CSV",
            data=csv_bytes,
            file_name=f"miway_{feed}_{min(start_d,end_d)}_to_{max(start_d,end_d)}.csv",
            mime="text/csv",
        )
else:
    st.info("Click **Load / Refresh** to fetch data.")

# csv_path = export_csv_streaming(
#     service=service,
#     file_tuples=file_tuples,
#     status_box=status_box,
#     progress_bar=progress_bar,
#     keep_cols=None,  # None = export ALL columns
#     out_name=f"miway_{feed}_{min(start_d,end_d)}_to_{max(start_d,end_d)}.csv",
# )

# st.session_state["csv_path"] = csv_path

