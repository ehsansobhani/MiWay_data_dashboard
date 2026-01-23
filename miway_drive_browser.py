# ============================
# SINGLE COMPLETE DROP-IN (DEPLOY-SAFE) miway_drive_browser.py
# ============================
# Paste this whole file as your Streamlit app (e.g., miway_drive_browser.py) and deploy.
# Secrets (Streamlit Cloud → Manage app → Settings → Secrets) MUST be:
#
# GDRIVE_ROOT_FOLDER_ID = "1queRR3FYKhiUz-40mU5sa9EUSNNQIq0F"
#
# [GDRIVE_SA_JSON]
# type = "service_account"
# project_id = "thunder-bay-electrical-bus"
# private_key_id = "..."
# private_key = """-----BEGIN PRIVATE KEY-----
# ...
# -----END PRIVATE KEY-----"""
# client_email = "miway-380@thunder-bay-electrical-bus.iam.gserviceaccount.com"
# client_id = "..."
# auth_uri = "https://accounts.google.com/o/oauth2/auth"
# token_uri = "https://oauth2.googleapis.com/token"
# auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
# client_x509_cert_url = "https://www.googleapis.com/robot/v1/metadata/x509/miway-380%40thunder-bay-electrical-bus.iam.gserviceaccount.com"
# universe_domain = "googleapis.com"
#
# requirements.txt:
# streamlit
# pandas
# pyarrow
# google-auth
# google-api-python-client

import io
from datetime import datetime, date, timedelta, time as dtime
from typing import List, Optional, Dict, Tuple
from pathlib import Path
import tempfile

import pandas as pd
import pyarrow.parquet as pq
import streamlit as st

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload


# -----------------------------
# UI CONFIG
# -----------------------------
st.set_page_config(
    page_title="MiWay Transit Data Browser",
    layout="wide",
    page_icon="assets/favicon.ico",
)

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
AGENCY_SLUG_DEFAULT = "miway"


# -----------------------------
# SECRETS (DEPLOY SAFE)
# -----------------------------
def _secret(key: str, default=None):
    # Streamlit Cloud raises when secrets file is missing/invalid; avoid hard crash.
    try:
        return st.secrets.get(key, default)
    except Exception:
        return default


root_folder_id = _secret("GDRIVE_ROOT_FOLDER_ID", "")
sa_info = _secret("GDRIVE_SA_JSON", None)

if not root_folder_id or not isinstance(sa_info, dict):
    st.error("Missing Streamlit secrets on this deployment (or secrets TOML is invalid).")
    st.markdown(
        """
**Fix (Streamlit Community Cloud):**
1. Open your deployed app
2. Click **Manage app** (bottom-right)
3. Go to **Settings → Secrets**
4. Paste valid TOML for:
   - `GDRIVE_ROOT_FOLDER_ID`
   - `[GDRIVE_SA_JSON]` (table) with triple-quoted `private_key`
5. **Save** then **Reboot app**
        """
    )
    try:
        st.write("Detected secret keys:", list(st.secrets.keys()))
    except Exception:
        st.write("Detected secret keys: (none)")
    st.stop()


# -----------------------------
# AUTH + DRIVE HELPERS
# -----------------------------
@st.cache_resource(show_spinner=False)
def get_drive_service(sa_info_dict: dict):
    creds = service_account.Credentials.from_service_account_info(
        sa_info_dict,
        scopes=["https://www.googleapis.com/auth/drive.readonly"],
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)


service = get_drive_service(sa_info)
agency_slug = AGENCY_SLUG_DEFAULT


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
    downloader = MediaIoBaseDownload(fh, request, chunksize=1024 * 1024)  # 1MB
    done = False
    while not done:
        status, done = downloader.next_chunk()
        if progress_cb and status:
            progress_cb(status.progress())
    return fh.getvalue()


# -----------------------------
# PATH RESOLUTION
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


def date_folder_name(d: date) -> str:
    return f"date={d.isoformat()}"


# -----------------------------
# LIST PARQUET PARTS
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
    Returns list of (file_id, file_name, logical_partition)
    logical_partition like "date=YYYY-MM-DD/hour=HH"
    """
    files_out: List[Tuple[str, str, str]] = []

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
            parts = drive_list_children(service, hour_id, only_folders=False)
            for p in parts:
                if p["name"].endswith(".parquet"):
                    logical = f"{dname}/{hname}"
                    files_out.append((p["id"], p["name"], logical))

    files_out.sort(key=lambda x: (x[2], x[1]))
    return files_out


# -----------------------------
# EXPORT CSV (SERVER TEMP FILE) + DOWNLOAD
# -----------------------------
def export_csv_streaming(
    service,
    file_tuples: List[Tuple[str, str, str]],
    status_box,
    progress_bar,
    output_path: str,
    keep_cols=None,
) -> str:
    if not file_tuples:
        raise RuntimeError("No parquet files found for selected date range.")

    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists():
        out_path.unlink()

    n = len(file_tuples)
    wrote_header = False
    total_rows = 0

    status_box.info(f"Export started. Files to process: {n:,}")
    progress_bar.progress(0.0)

    for i, (fid, _fname, logical) in enumerate(file_tuples, start=1):
        status_box.info(f"Processing {i:,}/{n:,}: {logical}")

        def _chunk_progress(p):
            base = (i - 1) / n
            progress_bar.progress(min(0.999, base + (p / n)))

        b = drive_download_bytes(service, fid, progress_cb=_chunk_progress)
        table = pq.read_table(io.BytesIO(b), columns=keep_cols if keep_cols else None)
        df = table.to_pandas()

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
    status_box.success(f"Done. Wrote {total_rows:,} rows into CSV.")
    return str(out_path)


# -----------------------------
# MAIN UI
# -----------------------------
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
    "Folders are partitioned by **UTC** (`date=.../hour=...`). "
    "Export uses Drive partitions; convert to local time later if needed."
)

feed_root_id = resolve_folder_path(service, root_folder_id, agency_slug, feed)
if not feed_root_id:
    st.error(
        f"Could not find folder path ROOT/{agency_slug}/raw/{feed}. "
        "Check folder names and that the Drive folder is shared with the service account."
    )
    st.stop()

with st.expander("Advanced controls", expanded=True):
    preview_rows = st.slider("Preview rows (after export)", 10, 500, 50, step=10)

if "busy" not in st.session_state:
    st.session_state.busy = False

btn_col1, _ = st.columns([1, 4])
with btn_col1:
    export_clicked = st.button("Export CSV (build + download)", disabled=st.session_state.busy)

status_box = st.empty()
progress_bar = st.progress(0)

if export_clicked:
    st.session_state.busy = True
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

        if not file_tuples:
            status_box.warning("No parquet files found for the selected range.")
        else:
            tmp_dir = Path(tempfile.gettempdir())
            out_path = tmp_dir / f"miway_{feed}_{min(start_d,end_d)}_to_{max(start_d,end_d)}.csv"

            csv_path = export_csv_streaming(
                service=service,
                file_tuples=file_tuples,
                status_box=status_box,
                progress_bar=progress_bar,
                output_path=str(out_path),
                keep_cols=None,
            )
            st.session_state["csv_path"] = csv_path

    except Exception as e:
        status_box.error(f"ERROR: {type(e).__name__}: {e}")
    finally:
        st.session_state.busy = False
        progress_bar.progress(0)

csv_path = st.session_state.get("csv_path", None)
if csv_path and Path(csv_path).exists():
    st.success("CSV is ready.")
    # Optional tiny preview (read first few lines to avoid loading huge file)
    try:
        preview_df = pd.read_csv(csv_path, nrows=preview_rows)
        st.dataframe(preview_df, use_container_width=True)
    except Exception:
        pass

    with open(csv_path, "rb") as f:
        st.download_button(
            label="Download CSV",
            data=f,
            file_name=Path(csv_path).name,
            mime="text/csv",
        )
else:
    st.info("Click **Export CSV (build + download)** to generate a CSV.")
