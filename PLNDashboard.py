import re
from typing import Optional, List, Tuple
from pathlib import Path
from datetime import datetime
import calendar
import time

import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO, StringIO
import toml
import requests

# gspread + google-auth
import gspread
from google.oauth2.service_account import Credentials as GoogleCredentials

from PIL import Image
from streamlit_autorefresh import st_autorefresh

# BASE = folder tempat PLNDashboard.py berada (root repo app)
BASE = Path(__file__).parent

def load_asset_image(fname):
    p = BASE / "assets" / fname
    if not p.exists():
        try:
            st.sidebar.warning(f"Asset tidak ditemukan: {p.name}")
        except Exception:
            pass
        return None
    try:
        return Image.open(p)
    except Exception as e:
        try:
            st.sidebar.warning(f"Gagal membuka gambar {p.name}: {e}")
        except Exception:
            pass
        return None

# -------- timezone imports (fixed for Pylance) --------
ZoneInfo = None  # type: ignore
pytz = None  # type: ignore
_HAS_ZONEINFO = False
try:
    from zoneinfo import ZoneInfo  # type: ignore
    _HAS_ZONEINFO = True
except Exception:
    try:
        import pytz  # type: ignore
        _HAS_ZONEINFO = False
    except Exception:
        # neither zoneinfo nor pytz available; will fallback to naive datetime
        _HAS_ZONEINFO = False

def now_jakarta():
    """Return timezone-aware datetime in Asia/Jakarta if possible."""
    if _HAS_ZONEINFO and ZoneInfo is not None:
        return datetime.now(tz=ZoneInfo("Asia/Jakarta"))
    else:
        try:
            if pytz is not None:
                return datetime.now(tz=pytz.timezone("Asia/Jakarta"))
        except Exception:
            pass
    # last-resort: naive local time (not ideal but safe)
    return datetime.now()

# -----------------------------------------------------

st.set_page_config(page_title="Analisis Penurunan & Rumah Kosong", layout="wide")
logo = load_asset_image("logo_pln.png")
if logo is not None:
    st.image(logo, width=220, use_column_width=False)
else:
    st.title("Analisis Penurunan & Rumah Kosong (Gabungan 2 Sheet)")
    
# ====== CONFIG: ganti dengan Sheet ID/GID kamu ======
SHEET_ID_CONS = "1mvYcJ8LMFkPwMN6SshPmRMwSwlkMuxmEHjm46dJyWDw"
GID_CONS = "595704292"

SHEET_ID_LBKB = "1TYAz6N3wAkk2NFu1tzPxvFyTeTioQLqFsZV78QDKMik"
GID_LBKB = "1169371683"
# =====================================================

HISTORY_SHEET_NAME = "HISTORY_LOG"

# ---------------- Utilities ----------------
@st.cache_data(ttl=3600)
def fetch_sheet_csv_header(sheet_id: str, gid: str = "0", timeout: int = 15) -> List[str]:
    """
    Fetch only header row (nrows=0) quickly to inspect column names.
    Cached for 1 hour by default.
    """
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
    sess = requests.Session()
    resp = sess.get(url, timeout=timeout)
    resp.raise_for_status()
    # read only header with pandas (nrows=0)
    s = StringIO(resp.text)
    try:
        df0 = pd.read_csv(s, nrows=0, dtype=str)
        return list(df0.columns)
    except Exception:
        # fallback: try manual split of first line
        first_line = resp.text.splitlines()[0] if resp.text else ""
        cols = [c.strip() for c in first_line.split(',')]
        return cols

@st.cache_data(ttl=3600)
def fetch_sheet_csv(sheet_id: str, gid: str = "0", usecols: Optional[List[str]] = None, timeout: int = 30) -> pd.DataFrame:
    """
    Fetch CSV from Google Sheets via requests and parse with pandas.
    usecols can be provided to limit columns.
    Cached for 1 hour by default.
    """
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
    sess = requests.Session()
    resp = sess.get(url, timeout=timeout)
    resp.raise_for_status()
    s = StringIO(resp.text)
    # read with low_memory=False for consistent dtypes, dtype=str to avoid type inference cost
    if usecols:
        try:
            df = pd.read_csv(s, usecols=usecols, dtype=str, low_memory=False)
            return df
        except ValueError:
            # if usecols contains names not present, fallback to reading all columns
            s.seek(0)
            df = pd.read_csv(s, dtype=str, low_memory=False)
            return df
    else:
        df = pd.read_csv(s, dtype=str, low_memory=False)
        return df

def find_column_by_keywords(df_or_cols, keywords_list):
    """
    Accept either DataFrame or list-of-columns.
    Return the best matched column name or None.
    """
    cols = df_or_cols.columns if hasattr(df_or_cols, "columns") else list(df_or_cols)
    # first try exact phrase (all keywords in column name)
    for col in cols:
        up = col.upper()
        if all(k.upper() in up for k in keywords_list):
            return col
    # fallback: any keyword matches (prefer longer matches)
    best = None
    best_count = 0
    for col in cols:
        up = col.upper()
        count = sum(1 for k in keywords_list if k.upper() in up)
        if count > best_count:
            best_count = count
            best = col
    return best if best_count > 0 else None

def normalize_value_for_compare(s):
    if pd.isna(s):
        return ""
    s = str(s).strip().lower()
    s = re.sub(r'[^a-z0-9\s]', ' ', s)
    s = re.sub(r'\btdk\b', 'tidak', s)
    s = re.sub(r'\bt\b', 'tidak', s)
    s = re.sub(r'\by\b', 'ya', s)
    s = re.sub(r'\byes\b', 'ya', s)
    s = re.sub(r'\bno\b', 'tidak', s)
    s = re.sub(r'\s+', ' ', s).strip()
    if 'tidak' in s and 'sesuai' in s:
        return 'tidak sesuai'
    if 'tidak' in s and 'terawat' in s:
        return 'tidak terawat'
    if 'sesuai' in s:
        return 'sesuai'
    if 'terawat' in s:
        return 'terawat'
    if s in ['ya','tidak']:
        return s
    return s

# ---------------- UI helper ----------------
def display_header_with_index(header_list, style: str = "compact"):
    if not header_list:
        st.write("Header kosong.")
        return

    style = (style or "compact").lower()
    if style == "table":
        try:
            df_hdr = pd.DataFrame({
                "No": list(range(1, len(header_list) + 1)),
                "Kolom": header_list
            })
            st.dataframe(df_hdr, use_container_width=True)
            return
        except Exception:
            # fallback to compact if table display fails
            style = "compact"

    try:
        lines = []
        width_no = len(str(len(header_list)))  # column width for numbers
        for i, h in enumerate(header_list, start=1):
            # format: padded number + " : " + header (quoted like JSON)
            lines.append(f"{str(i).rjust(width_no)} : \"{h}\"")
        # show as code block to preserve monospace & alignment
        st.code("\n".join(lines), language="text")
    except Exception:
        # last-resort simple write
        for i, h in enumerate(header_list, start=1):
            st.write(f"{i} : {h}")

# ---------------- gspread helpers (write) ----------------
GS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
             "https://www.googleapis.com/auth/drive"]

# Local fallback path (project root .streamlit)
LOCAL_SECRETS_PATH = Path.cwd() / ".streamlit" / "secrets.toml"
SECRETS_KEY = "gcp_service_account"

def load_service_account_from_st_secrets() -> Optional[dict]:
    try:
        if SECRETS_KEY in st.secrets:
            val = st.secrets[SECRETS_KEY]
            try:
                return dict(val)
            except Exception:
                return val
        return None
    except Exception:
        return None

def load_service_account_from_file(path: Path) -> Optional[dict]:
    try:
        if not path.exists():
            return None
        data = toml.load(path)
        if isinstance(data, dict) and SECRETS_KEY in data:
            return data[SECRETS_KEY]
        if isinstance(data, dict) and "client_email" in data:
            # JSON content pasted into TOML root
            return data
        return None
    except Exception as e:
        try:
            st.warning(f"Gagal membaca secrets dari file {path}: {e}")
        except Exception:
            pass
        return None

def normalize_service_account(sa: dict) -> dict:
    pk = sa.get("private_key")
    if not pk:
        return sa
    if isinstance(pk, str) and "\\n" in pk:
        pk = pk.replace("\\n", "\n")
    pk = pk.strip()
    if "-----BEGIN PRIVATE KEY-----" not in pk:
        # last-resort: try to add header/footer
        pk = "-----BEGIN PRIVATE KEY-----\n" + pk + "\n-----END PRIVATE KEY-----"
    sa["private_key"] = pk
    return sa

def have_write_creds() -> bool:
    if load_service_account_from_st_secrets():
        return True
    if load_service_account_from_file(LOCAL_SECRETS_PATH):
        return True
    return False

@st.cache_resource
def get_gspread_client():
    sa = load_service_account_from_st_secrets()
    if sa is None:
        sa = load_service_account_from_file(LOCAL_SECRETS_PATH)
    if sa is None:
        raise RuntimeError(
            "Service account credentials tidak ditemukan. "
            "Pastikan .streamlit/secrets.toml berisi [gcp_service_account]."
        )
    sa = normalize_service_account(dict(sa))
    creds = GoogleCredentials.from_service_account_info(sa, scopes=GS_SCOPES)
    return gspread.authorize(creds)

def get_worksheet_by_gid(sheet_id: str, gid: str):
    client = get_gspread_client()
    sh = client.open_by_key(sheet_id)
    try:
        gid_int = int(gid)
    except Exception:
        gid_int = None
    for ws in sh.worksheets():
        try:
            if gid_int is not None and int(ws.id) == gid_int:
                return ws
            if str(ws.id) == str(gid):
                return ws
        except Exception:
            continue
    return sh.get_worksheet(0)

# ----- History helpers -----
def _ensure_history_ws(sh: gspread.Spreadsheet):
    """Return worksheet object for history log; create with header if missing."""
    try:
        ws = sh.worksheet(HISTORY_SHEET_NAME)
    except Exception:
        ws = sh.add_worksheet(title=HISTORY_SHEET_NAME, rows=1000, cols=20)
        headers = ["Timestamp", "User", "Action", "TargetSheetTitle", "TargetSheetId", "Details", "Status"]
        ws.append_row(headers, value_input_option="USER_ENTERED")  # type: ignore[arg-type]
    # ensure headers present
    try:
        hdr = ws.row_values(1)
        if not hdr or hdr[0].strip() == "":
            headers = ["Timestamp", "User", "Action", "TargetSheetTitle", "TargetSheetId", "Details", "Status"]
            ws.update('1:1', [headers])  # type: ignore[arg-type]
    except Exception:
        pass
    return ws

def log_history(target_sheet_id: str, action: str, details: str, user: str = "anonymous", status: str = "SUCCESS"):
    """Append a history row to HISTORY_LOG sheet in the target spreadsheet."""
    try:
        client = get_gspread_client()
        sh = client.open_by_key(target_sheet_id)
        hist_ws = _ensure_history_ws(sh)
        ts_dt = now_jakarta()
        # store ISO with offset if available
        try:
            ts = ts_dt.isoformat(sep=' ', timespec='seconds')
        except TypeError:
            ts = ts_dt.isoformat(sep=' ')
        row = [ts, user, action, sh.title, target_sheet_id, details, status]
        hist_ws.append_row(row, value_input_option="USER_ENTERED")  # type: ignore[arg-type]
    except Exception as e:
        try:
            st.warning(f"Gagal mencatat history: {e}")
        except Exception:
            pass

# ---------------- Sidebar menu ----------------
logo = load_asset_image("logo_pln.png")
if logo is not None:
    st.sidebar.image(logo, width=120)
else:
    st.sidebar.write("Logo PLN (tidak tersedia)")
st.sidebar.title("PLN ULP DINOYO")
st.sidebar.markdown(
    "<a href='https://maps.app.goo.gl/CnhdCBrhz3mihieL9' "
    "style='text-decoration:none; color:#0000FF;' target='_blank'>"
    "📍 Jl. Pandan No.15, Gading Kasri, Kec. Klojen, Kota Malang, Jawa Timur 65115"
    "</a>",
    unsafe_allow_html=True
)
st.sidebar.markdown("<hr style='border:1px solid #04aceb;'>", unsafe_allow_html=True)

# CHANGES: use a non-empty label so Streamlit doesn't warn
menu = st.sidebar.selectbox("📊 Pilih Menu", ["Analisis Data", "Tambah Data","History"], index=0)

st.sidebar.markdown("<hr style='border:1px solid #04aceb;'>", unsafe_allow_html=True)
st.sidebar.subheader("🕒 Waktu Akses")
st.sidebar.write(f"📅 {now_jakarta().strftime('%d-%m-%Y')}")
st.sidebar.write(f"⏰ {now_jakarta().strftime('%H:%M:%S')}")

st.sidebar.markdown("<hr style='border:1px solid #04aceb;'>", unsafe_allow_html=True)
st.sidebar.subheader("👨‍💻 Developed By")
st.sidebar.write("@Yudaneru Vebrianto**")
st.sidebar.markdown(
    "<a href='https://www.linkedin.com/in/yudaneru-vebrianto/' "
    "style='text-decoration:none; color:#0000FF;' target='_blank'>"
    "LinkedIn / Yudaneru Vebrianto"
    "</a>",
    unsafe_allow_html=True
)
st.sidebar.write("@Muhammad Aqil Fauzi**")
st.sidebar.markdown(
    "<a href='https://www.linkedin.com/in/muhammad-aqil-fauzi/' "
    "style='text-decoration:none; color:#0000FF;' target='_blank'>"
    "LinkedIn / Muhammad Aqil Fauzi"
    "</a>",
    unsafe_allow_html=True
)
st.sidebar.title("🏫 Brawijaya University 2025")

# ---------------- MENU: Analisis Data ----------------
if menu == "Analisis Data":
    # we'll lazily load df_cons and df_lbkb only when needed
    df_cons = pd.DataFrame()
    df_lbkb = pd.DataFrame()

    # ---------------- Mode selection with lock ----------------
    if "analysis_mode_locked" not in st.session_state:
        st.session_state["analysis_mode_locked"] = False
    if "analysis_mode_choice" not in st.session_state:
        st.session_state["analysis_mode_choice"] = None

    st.write("#### Mode Analisis")
    if not st.session_state["analysis_mode_locked"]:
        # allow user to choose mode (three options)
        analysis_mode = st.radio(
            "Pilih mode analisis:",
            ["Penurunan saja", "Rumah kosong (LBKB) saja", "Penurunan + Rumah Kosong (LBKB)"],
            index=0
        )
        col_confirm, col_info = st.columns([1,3])
        with col_confirm:
            if st.button("Confirm Mode"):
                # lock and save
                st.session_state["analysis_mode_locked"] = True
                st.session_state["analysis_mode_choice"] = analysis_mode
                st.rerun()
        with col_info:
            st.caption("Klik **Confirm Mode** untuk mengunci pilihan. Anda bisa mengubahnya nanti dengan tombol 'Ubah Mode'.")
    else:
        analysis_mode = st.session_state["analysis_mode_choice"]
        st.info(f"Mode saat ini: **{analysis_mode}**")
        if st.button("Ubah Mode"):
            st.session_state["analysis_mode_locked"] = False
            st.session_state["analysis_mode_choice"] = None
            st.rerun()

    use_lbkb = analysis_mode in ("Rumah kosong (LBKB) saja", "Penurunan + Rumah Kosong (LBKB)")
    use_penurunan = analysis_mode in ("Penurunan saja", "Penurunan + Rumah Kosong (LBKB)")

    # show/hide threshold/operator only when penurunan is relevant
    if use_penurunan:
        threshold = st.number_input(
            "Masukkan threshold penurunan (%)",
            min_value=0.0, max_value=100.0, value=20.0, step=5.0,
            help="Step menentukan nilai kenaikan/penurunan saat klik panah. Gunakan step kecil (mis. 0.1) untuk presisi."
        )

        operator = st.selectbox("Pilih operator filter", options=["<=", ">=", "=="], index=0)

        tol = 0.1
        if operator == "==":
            tol = st.number_input(
                "Toleransi untuk '==' (± persen)", min_value=0.0, max_value=100.0, value=0.1, step=0.1,
                help="Tolerance digunakan untuk membandingkan float. Contoh tol=0.1 akan mencocokkan nilai yang berada dalam ±0.1% dari threshold."
            )
    else:
        threshold = 0.0
        operator = "<="
        tol = 0.1

    id_input = st.text_input("Masukkan ID Pelanggan (opsional):")

    # === PILIHAN BULAN DINAMIS + PRESET BUTTONS ===
    def parse_date_from_colname(colname: str):
        c = str(colname).strip()
        # pattern YYYY-MM/MM-YYYY or contiguous YYYYMM
        m = re.search(r'(?P<y>20\d{2})\s*[-_/]?\s*(?P<m>0?[1-9]|1[0-2])', c)
        if m:
            y = int(m.group('y')); mm = int(m.group('m'))
            return datetime(y, mm, 1)
        m = re.search(r'(?P<m>0?[1-9]|1[0-2])\s*[-_/]?\s*(?P<y>20\d{2})', c)
        if m:
            mm = int(m.group('m')); y = int(m.group('y'))
            return datetime(y, mm, 1)
        m = re.search(r'(20\d{2})(0[1-9]|1[0-2])', c)
        if m:
            y = int(m.group(1)); mm = int(m.group(2))
            return datetime(y, mm, 1)
        m = re.search(r'(?P<mon>[A-Za-z]{3,9})[^\d]*(?P<y>20\d{2})', c)
        if m:
            monraw = m.group('mon').strip().lower()
            abbr_map = { (calendar.month_abbr[i] or "").lower(): i for i in range(len(calendar.month_abbr)) }
            name_map = { (calendar.month_name[i] or "").lower(): i for i in range(len(calendar.month_name)) }
            mm = None
            if monraw[:3] in [k[:3] for k in abbr_map.keys()]:
                for k,v in abbr_map.items():
                    if k and k.startswith(monraw[:3]):
                        mm = v
                        break
            if mm is None:
                for k,v in name_map.items():
                    if k and k.startswith(monraw):
                        mm = v
                        break
            try:
                y = int(m.group('y'))
                if mm and mm > 0:
                    return datetime(y, mm, 1)
            except Exception:
                pass
        return None

    # ---------------- Lazy load df_cons (only if needed) ----------------
    all_columns = []
    col_to_date = {}
    sorted_cols = []
    header_lbkb = []
    header = []  # ensure defined early to avoid NameError

    if use_penurunan:
        t0 = time.perf_counter()
        with st.spinner("Mengecek header sheet konsumsi..."):
            try:
                header = fetch_sheet_csv_header(SHEET_ID_CONS, GID_CONS)
            except Exception as e:
                st.error(f"Gagal mengambil header sheet konsumsi: {e}")
                st.stop()
        t1 = time.perf_counter()
        st.info(f"Header konsumsi diambil dalam {t1-t0:.2f}s (kolom: {len(header)})")

        # detect REK columns from header
        all_columns = [col for col in header if "REK" in col.upper()]
        # prepare usecols: include candidate ID columns + REK columns
        id_candidates = [c for c in header if "ID" in c.upper()]
        usecols = []
        if id_candidates:
            usecols.extend(id_candidates)
        if all_columns:
            usecols.extend(all_columns)

        # fetch only needed columns (if none found, fetch all)
        t0 = time.perf_counter()
        with st.spinner("Mengunduh data konsumsi (kolom terpilih)..."):
            try:
                if usecols:
                    df_cons = fetch_sheet_csv(SHEET_ID_CONS, GID_CONS, usecols=list(dict.fromkeys(usecols)))
                else:
                    df_cons = fetch_sheet_csv(SHEET_ID_CONS, GID_CONS)
            except Exception as e:
                st.error(f"Gagal mengambil sheet konsumsi: {e}")
                st.stop()
        t1 = time.perf_counter()
        st.info(f"Sheet konsumsi diambil dalam {t1-t0:.2f}s (baris: {len(df_cons)}, kolom: {len(df_cons.columns)})")

    # ---------------- Lazy load df_lbkb (only if needed) ----------------
    if use_lbkb:
        t0 = time.perf_counter()
        with st.spinner("Mengecek header sheet LBKB..."):
            try:
                header_lbkb = fetch_sheet_csv_header(SHEET_ID_LBKB, GID_LBKB)
            except Exception as e:
                st.error(f"Gagal mengambil header sheet LBKB: {e}")
                st.stop()
        t1 = time.perf_counter()
        st.info(f"Header LBKB diambil dalam {t1-t0:.2f}s (kolom: {len(header_lbkb)})")

    # ---------------- UI: Preview small samples (faster) ----------------
    with st.expander("Preview Data (sample)", expanded=False):
        st.write("Preview data konsumsi (sample):")
        if use_penurunan and not df_cons.empty:
            st.dataframe(df_cons.head(50), use_container_width=True)
        else:
            st.write("Belum memuat data konsumsi atau tidak dipilih.")
        st.write("Preview data LBKB (header):")
        if header_lbkb:
            # tampilkan dalam style compact (kamu bisa ubah ke table bila ingin)
            display_header_with_index(header_lbkb[:50], style="compact")
        else:
            st.write("Belum memuat header LBKB atau tidak dipilih.")

    # ---------------- Now continue original logic that depends on df_cons/header ----------------

    # session state for presets
    if "preset_months_set1" not in st.session_state:
        st.session_state["preset_months_set1"] = []
    if "preset_months_set2" not in st.session_state:
        st.session_state["preset_months_set2"] = []
    if "preset_range_label" not in st.session_state:
        st.session_state["preset_range_label"] = ""

    def fmt_month_year(dt: datetime) -> str:
        try:
            return f"{MONTHS_ID[dt.month]} {dt.year}"
        except Exception:
            return str(dt.date()) if isinstance(dt, datetime) else str(dt)

    MONTHS_ID = [
        "", "Januari", "Februari", "Maret", "April", "Mei", "Juni",
        "Juli", "Agustus", "September", "Oktober", "November", "Desember"
    ]

    def make_range_label(months_set1: List[str], months_set2: List[str]) -> str:
        if not months_set1 or not months_set2:
            return ""

        def range_from_cols(cols: List[str]) -> Tuple[str, str]:
            parsed_dates: List[datetime] = []
            for c in cols:
                d = col_to_date.get(c) if isinstance(col_to_date, dict) else None
                if isinstance(d, datetime):
                    parsed_dates.append(d)
            if parsed_dates:
                start_dt = min(parsed_dates)
                end_dt = max(parsed_dates)
                return fmt_month_year(start_dt), fmt_month_year(end_dt)
            if cols:
                return cols[0], cols[-1]
            return "", ""

        a1, a2 = range_from_cols(months_set1)
        b1, b2 = range_from_cols(months_set2)

        if not (a1 or a2 or b1 or b2):
            return ""

        return f"Perbandingan: {a1} - {a2}  vs  {b1} - {b2}"

    def apply_preset(n_months: int):
        if len(sorted_cols) < n_months:
            window = sorted_cols.copy()
        else:
            window = sorted_cols[-n_months:]
        half = n_months // 2
        if len(window) <= half:
            split = len(window) // 2
        else:
            split = half
        period1 = window[:split]
        period2 = window[split:]
        st.session_state["preset_months_set1"] = period1
        st.session_state["preset_months_set2"] = period2
        st.session_state["preset_range_label"] = make_range_label(period1, period2)

    # Build sorted_cols & col_to_date from df_cons header if available
    if use_penurunan and not df_cons.empty:
        all_columns = [col for col in df_cons.columns if "REK" in col.upper()]
        col_dates = []
        for col in all_columns:
            dt = parse_date_from_colname(col)
            col_dates.append((col, dt))
        parsed = [(c,d) for c,d in col_dates if d is not None]
        unparsed = [c for c,d in col_dates if d is None]
        if parsed:
            parsed_sorted = sorted(parsed, key=lambda x: x[1])
            sorted_cols = [c for c,_ in parsed_sorted] + unparsed
            col_to_date = {c:d for c,d in parsed_sorted}
        else:
            sorted_cols = all_columns.copy()
            col_to_date = {}
    else:
        sorted_cols = []
        col_to_date = {}

    # Show presets and multiselect when penurunan is needed
    if use_penurunan:
        st.write("#### Pilih Preset Periode (otomatis memilih kolom bulan)")
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            if st.button("24 bulan"):
                apply_preset(24)
        with c2:
            if st.button("12 bulan"):
                apply_preset(12)
        with c3:
            if st.button("6 bulan"):
                apply_preset(6)
        with c4:
            if st.button("3 bulan"):
                apply_preset(3)

        if st.session_state.get("preset_range_label"):
            st.info(st.session_state["preset_range_label"])

        default1 = st.session_state.get("preset_months_set1") or (sorted_cols[:6] if len(sorted_cols)>=6 else sorted_cols[:len(sorted_cols)//2])
        default2 = st.session_state.get("preset_months_set2") or (sorted_cols[6:12] if len(sorted_cols)>=12 else sorted_cols[len(sorted_cols)//2:])

        col1_ui, col2_ui = st.columns(2)
        with col1_ui:
            months_set1 = st.multiselect(
                "Pilih bulan untuk Rata-rata Periode 1",
                options=sorted_cols,
                default=default1
            )
        with col2_ui:
            months_set2 = st.multiselect(
                "Pilih bulan untuk Rata-rata Periode 2",
                options=sorted_cols,
                default=default2
            )

        current_label = make_range_label(months_set1, months_set2)
        if current_label:
            st.info(current_label)
    else:
        months_set1 = []
        months_set2 = []

    # validate months if penurunan used
    if use_penurunan and (not months_set1 or not months_set2):
        st.warning("Pilih setidaknya 1 kolom untuk Periode 1 dan Periode 2.")
        st.stop()

    # Pastikan kolom IDPEL ada di df_cons (only relevant when use_penurunan)
    ID_COL = "IDPEL"
    if use_penurunan:
        if ID_COL not in df_cons.columns:
            candidates = [c for c in df_cons.columns if "ID" in c.upper()]
            if candidates:
                st.info(f"Kolom 'IDPEL' tidak ditemukan, menggunakan kandidat: {candidates[0]}")
                df_cons = df_cons.rename(columns={candidates[0]: ID_COL})
            else:
                st.error("Kolom IDPEL tidak ditemukan di sheet konsumsi. Sesuaikan nama kolom.")
                st.stop()

    # Hitung rata-rata dan selisih (hanya jika pakai penurunan)
    df = pd.DataFrame()
    df_filtered = pd.DataFrame()
    if use_penurunan:
        df = df_cons.copy()
        missing1 = [m for m in months_set1 if m not in df.columns]
        missing2 = [m for m in months_set2 if m not in df.columns]
        if missing1 or missing2:
            st.error(f"Kolom terpilih tidak ditemukan di sheet: {missing1 + missing2}")
            st.stop()

        # Convert to numeric safely
        for c in months_set1 + months_set2:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors='coerce')

        df["Rata2_Periode1"] = df[months_set1].mean(axis=1, skipna=True)
        df["Rata2_Periode2"] = df[months_set2].mean(axis=1, skipna=True)

        df["Selisih_Rata2"] = (
            (df["Rata2_Periode1"] - df["Rata2_Periode2"])
            / df["Rata2_Periode1"].replace(0, pd.NA) * 100
        )

        df = df[df["Selisih_Rata2"].notna() & (df["Selisih_Rata2"] >= 0)].copy()

        if operator == "<=":
            df_filtered = df[df["Selisih_Rata2"] <= float(threshold)].copy()
        elif operator == ">=":
            df_filtered = df[df["Selisih_Rata2"] >= float(threshold)].copy()
        else:
            df_filtered = df[np.isclose(df["Selisih_Rata2"].astype(float), float(threshold), atol=float(tol))].copy()

    # ---------------- LBKB processing (CHANGES: allow multi-category + multi-values) ----------------
    found_cols_map = {}   # map category -> column name found in LBKB
    nilai_pilihan_map = {}  # selected values for each category
    lbkb_categories = [
        ("KONDISI BANGUNAN", ["Terawat", "Tidak Terawat"], ["KONDISI", "BANGUN"]),
        ("ANGKA STAN VS FOTO METER", ["Sesuai", "Tidak Sesuai"], ["ANGKA", "STAN", "FOTO", "METER"]),
        ("TERLIHAT PEMAKAIAN", ["Ya", "Tidak"], ["TERLIHAT", "PEMAKAIAN"])
    ]
    df_lbkb_filtered = pd.DataFrame()

    if use_lbkb:
        st.write("#### Filter LBKB tambahan (pilih satu atau beberapa kategori dan nilainya)")
        # let user select which categories to filter (multi-select)
        cat_names = [c[0] for c in lbkb_categories]
        selected_cats = st.multiselect("Pilih kategori LBKB untuk filter (boleh lebih dari satu):", options=cat_names, default=cat_names)

        # prepare UI layout similar to three columns of checkboxes / multiselects
        cols_ui = st.columns(3)
        for i, (cat, opts, keywords) in enumerate(lbkb_categories):
            with cols_ui[i]:
                if cat in selected_cats:
                    # default select all values
                    sel = st.multiselect(f"{cat}", options=opts, default=opts, key=f"lbkb_{i}")
                    nilai_pilihan_map[cat] = sel
                else:
                    # keep empty if not selected
                    st.write(f"{cat} (tidak dipakai)")
                    nilai_pilihan_map[cat] = []

        # Ensure we have header for LBKB to find columns
        if not header_lbkb:
            t0 = time.perf_counter()
            with st.spinner("Mengecek header sheet LBKB..."):
                try:
                    header_lbkb = fetch_sheet_csv_header(SHEET_ID_LBKB, GID_LBKB)
                except Exception as e:
                    st.error(f"Gagal mengambil header sheet LBKB: {e}")
                    st.stop()
            t1 = time.perf_counter()
            st.info(f"Header LBKB diambil dalam {t1-t0:.2f}s (kolom: {len(header_lbkb)})")

        # find actual column names for each selected category
        for cat, opts, keywords in lbkb_categories:
            if cat in selected_cats:
                col_found = find_column_by_keywords(header_lbkb, keywords)
                found_cols_map[cat] = col_found
                if col_found is None:
                    st.error(f"Tidak menemukan kolom LBKB untuk kategori '{cat}'. Periksa nama kolom di sheet LBKB.")
                    st.stop()

        # Now fetch only needed columns from LBKB: ID candidate + all found columns
        try:
            cols_lbkb_use = []
            # pick candidate id in header_lbkb
            id_cands = [c for c in (header_lbkb or []) if "ID" in c.upper()]
            if id_cands:
                cols_lbkb_use.append(id_cands[0])
            # add found columns (filter out None)
            for col in [c for c in found_cols_map.values() if c]:
                if col not in cols_lbkb_use:
                    cols_lbkb_use.append(col)

            if not cols_lbkb_use:
                st.info("Tidak ada kolom LBKB yang akan diambil (tidak ada kategori dipilih).")
                df_lbkb = pd.DataFrame()
            else:
                t0 = time.perf_counter()
                with st.spinner("Mengunduh data LBKB (kolom terpilih)..."):
                    df_lbkb = fetch_sheet_csv(SHEET_ID_LBKB, GID_LBKB, usecols=list(dict.fromkeys(cols_lbkb_use)))
                t1 = time.perf_counter()
                st.info(f"Sheet LBKB diambil dalam {t1-t0:.2f}s (baris: {len(df_lbkb)}, kolom: {len(df_lbkb.columns)})")
        except Exception as e:
            st.error(f"Gagal mengambil sheet LBKB: {e}")
            st.stop()

        if not df_lbkb.empty:
            # normalize and build mask
            mask = pd.Series([True] * len(df_lbkb))
            for cat, opts, keywords in lbkb_categories:
                if cat in selected_cats:
                    col = found_cols_map.get(cat)
                    if not col:
                        continue
                    sel_values = nilai_pilihan_map.get(cat, [])
                    # normalize column
                    df_lbkb[col] = df_lbkb[col].astype(str).str.strip().str.lower()
                    norm_col = f"{col}_norm"
                    df_lbkb[norm_col] = df_lbkb[col].apply(normalize_value_for_compare)
                    # map human-readable sel_values to normalized keys
                    map_sel: List[str] = []
                    for v in sel_values:
                        v_norm = normalize_value_for_compare(v)
                        map_sel.append(v_norm)
                    # if nothing selected (shouldn't happen because default is all), skip
                    if map_sel:
                        mask = mask & df_lbkb[norm_col].isin(map_sel)

            df_lbkb_filtered = df_lbkb[mask].copy()

            # Pastikan kolom ID di LBKB ada, jika tidak coba cari kandidat lalu rename
            if ID_COL not in df_lbkb_filtered.columns:
                candidates = [c for c in df_lbkb_filtered.columns if "ID" in c.upper()]
                if candidates:
                    df_lbkb_filtered = df_lbkb_filtered.rename(columns={candidates[0]: ID_COL})
                    st.info(f"Menggunakan kolom '{candidates[0]}' sebagai IDPEL di LBKB")
                else:
                    st.warning("Tidak menemukan kolom ID di LBKB; beberapa operasi gabungan mungkin gagal jika membutuhkan IDPEL.")

    # Build final result based on chosen mode
    df_merged = pd.DataFrame()
    if analysis_mode == "Penurunan saja":
        df_merged = df_filtered.copy()
    elif analysis_mode == "Rumah kosong (LBKB) saja":
        df_merged = df_lbkb_filtered.copy()
    else:  # Penurunan + LBKB
        if df_filtered.empty:
            st.warning("Tidak ada data penurunan yang memenuhi kriteria; gabungan akan kosong.")
        if df_lbkb_filtered.empty:
            st.warning("Tidak ada data LBKB yang memenuhi kriteria; gabungan akan kosong.")
        if not df_filtered.empty and not df_lbkb_filtered.empty:
            df_filtered[ID_COL] = df_filtered[ID_COL].astype(str).str.strip()
            df_lbkb_filtered[ID_COL] = df_lbkb_filtered[ID_COL].astype(str).str.strip()
            # merge, include only found columns (filter out None)
            cols_to_include = [c for c in found_cols_map.values() if c]
            df_merged = pd.merge(
                df_filtered,
                df_lbkb_filtered[[ID_COL] + cols_to_include],
                on=ID_COL,
                how="inner"
            )
        else:
            df_merged = pd.DataFrame()

    # Apply ID filter if provided
    if id_input and not df_merged.empty:
        df_merged = df_merged[df_merged[ID_COL].astype(str).str.contains(id_input)]

    # Display results + period label
    op_text = {
        "<=": f"penurunan ≤ {threshold}%",
        ">=": f"penurunan ≥ {threshold}%",
        "==": f"penurunan ≈ {threshold}% (tol ±{tol}%)"
    }[operator]

    header_suffix = ""
    if analysis_mode == "Penurunan saja":
        header_suffix = " (Penurunan saja)"
    elif analysis_mode == "Rumah kosong (LBKB) saja":
        # show which categories were used
        cats_shown = ", ".join([c for c in found_cols_map.keys()]) if found_cols_map else ""
        header_suffix = f" (LBKB: {cats_shown})" if cats_shown else " (LBKB saja)"
    else:
        cats_shown = ", ".join([c for c in found_cols_map.keys()]) if found_cols_map else ""
        header_suffix = f" (Gabungan: LBKB {cats_shown})" if cats_shown else " (Gabungan)"

    st.write(f"### Hasil Analisis{header_suffix}")

    period_label = ""
    try:
        period_label = make_range_label(months_set1, months_set2)
    except Exception:
        period_label = ""
    if period_label and use_penurunan:
        st.caption(period_label)

    st.write(f"Total baris hasil: {len(df_merged)}")

    if analysis_mode == "Rumah kosong (LBKB) saja":
        display_cols = [c for c in [ID_COL, "NAMA"] + list(found_cols_map.values()) if c and c in df_merged.columns]
    else:
        display_cols = [ID_COL, "NAMA", "Rata2_Periode1", "Rata2_Periode2", "Selisih_Rata2"]
        if analysis_mode == "Penurunan + Rumah Kosong (LBKB)" and found_cols_map:
            display_cols += [c for c in found_cols_map.values() if c]
        display_cols = [c for c in display_cols if c in df_merged.columns]

    if df_merged.empty:
        st.info("Tidak ada data untuk ditampilkan berdasarkan pilihan saat ini.")
    else:
        if "Selisih_Rata2" in df_merged.columns:
            st.dataframe(df_merged[display_cols].sort_values("Selisih_Rata2", ascending=False), use_container_width=True)
        else:
            st.dataframe(df_merged[display_cols], use_container_width=True)

    # Download hasil
    output = BytesIO()
    try:
        df_merged.to_excel(output, index=False)
        output.seek(0)

        excel_icon = load_asset_image("logo_excel.jpg")
        col1, col2 = st.columns([1,15])
        with col1:
            if excel_icon is not None:
                st.image(excel_icon, width=40)
            else:
                # fallback: kosongkan atau tampilkan teks kecil
                st.write("")
        with col2:
            st.download_button(
                label="Download Hasil ke Excel",
                data=output,
                file_name="hasil_analisis.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
    except Exception as e:
        st.warning(f"Gagal membuat file excel: {e}")

    # Distribusi per quarter (jika relevan) ... (sama seperti sebelumnya)
    if "Selisih_Rata2" in df_merged.columns and len(df_merged) > 0:
        if operator == "<=":
            quarter = float(threshold) / 4 if float(threshold) > 0 else 1
            bins = [0, quarter, 2*quarter, 3*quarter, float(threshold) + 1e-6]
            labels = [
                f"≤ {quarter:.2f}%",
                f"{quarter:.2f}% - {2*quarter:.2f}%",
                f"{2*quarter:.2f}% - {3*quarter:.2f}%",
                f"{3*quarter:.2f}% - {float(threshold):.2f}%"
            ]
            df_plot = df_merged.copy()
            df_plot["Quarter"] = pd.cut(df_plot["Selisih_Rata2"], bins=bins, labels=labels, include_lowest=True)
            quarter_counts = df_plot["Quarter"].value_counts().sort_index()
            st.write("### Distribusi per Quarter (0 → threshold)")
            for label, count in quarter_counts.items():
                st.write(f"{label}: {count} pelanggan")
            st.bar_chart(quarter_counts)
        elif operator == ">=":
            max_val = df_merged["Selisih_Rata2"].max()
            span = max_val - float(threshold)
            if span <= 0:
                st.info("Semua nilai sama dengan atau sedikit di atas threshold; distribusi per quarter tidak tersedia.")
            else:
                quarter = span / 4
                bins = [float(threshold), float(threshold) + quarter, float(threshold) + 2*quarter, float(threshold) + 3*quarter, max_val + 1e-6]
                labels = [
                    f"{float(threshold):.2f}% - {float(threshold)+quarter:.2f}%",
                    f"{float(threshold)+quarter:.2f}% - {float(threshold)+2*quarter:.2f}%",
                    f"{float(threshold)+2*quarter:.2f}% - {float(threshold)+3*quarter:.2f}%",
                    f"{float(threshold)+3*quarter:.2f}% - {max_val:.2f}%"
                ]
                df_plot = df_merged.copy()
                df_plot["Quarter"] = pd.cut(df_plot["Selisih_Rata2"], bins=bins, labels=labels, include_lowest=True)
                quarter_counts = df_plot["Quarter"].value_counts().sort_index()
                st.write(f"### Distribusi per Quarter (threshold → max {max_val:.2f}%)")
                for label, count in quarter_counts.items():
                    st.write(f"{label}: {count} pelanggan")
                st.bar_chart(quarter_counts)
        else:
            st.info(f"Operator '==' menggunakan toleransi ±{tol}%. Distribusi per-quarter tidak ditampilkan untuk kondisi '==' karena biasanya menghasilkan satu grup kecil.")

# ---------------- MENU: Tambah Data ----------------
elif menu == "Tambah Data":
    st.subheader("Tambah Data ke Google Sheets")

    if not have_write_creds():
        st.warning(
            "Kredensial *Service Account* belum tersedia di st.secrets['gcp_service_account'].\n"
            "Tambahkan JSON service account Anda ke .streamlit/secrets.toml atau lewat UI Streamlit Cloud."
        )
        st.stop()

    target_sheet = st.selectbox("Pilih sheet tujuan:", ["Konsumsi (SHEET_ID_CONS)", "LBKB (SHEET_ID_LBKB)"], index=0)
    if "Konsumsi" in target_sheet:
        target_sheet_id = SHEET_ID_CONS
        target_gid = GID_CONS
    else:
        target_sheet_id = SHEET_ID_LBKB
        target_gid = GID_LBKB

    try:
        ws = get_worksheet_by_gid(target_sheet_id, target_gid)
    except Exception as e:
        st.error(f"Gagal mengambil worksheet: {e}")
        st.stop()

    # optional user identifier to include in history
    user_name = st.text_input("Nama pengguna (untuk log) — opsional:", value="anonymous")

    aksi = st.radio("Aksi", ["Tambah Kolom Baru", "Tambah Baris Baru"], index=0, horizontal=True)

    if aksi == "Tambah Kolom Baru":
        st.info("Aksi ini hanya menambahkan nama kolom pada baris header (baris 1).")
        current_header = ws.row_values(1) or []
        with st.expander("Header saat ini", expanded=False):
            display_header_with_index(current_header)

        new_col_name = st.text_input("Masukkan nama kolom baru:")
        add_position = st.number_input(
            "Posisi sisip (opsional, 1 = kolom pertama). Biarkan 0 untuk menambahkan di akhir.",
            min_value=0, max_value=9999, value=0, step=1
        )

        if st.button("Tambahkan Kolom"):
            if not new_col_name.strip():
                st.error("Nama kolom tidak boleh kosong.")
            else:
                header = ws.row_values(1) or []
                try:
                    if add_position and 1 <= add_position <= max(1, len(header)+1):
                        idx = add_position - 1
                        if len(header) < idx:
                            header += [""] * (idx - len(header))
                        header.insert(idx, new_col_name.strip())
                    else:
                        header.append(new_col_name.strip())

                    ws.update('1:1', [header])  # type: ignore[arg-type]
                    st.success(f"Kolom '{new_col_name}' berhasil ditambahkan.")

                    # log history
                    details = f"Added column '{new_col_name}' at position {add_position if add_position!=0 else 'end'}"
                    try:
                        log_history(target_sheet_id, action="Tambah Kolom", details=details, user=user_name, status="SUCCESS")
                    except Exception as e:
                        st.warning(f"Gagal mencatat history: {e}")

                    st.rerun()
                except Exception as e:
                    st.error(f"Gagal menambahkan kolom: {e}")
                    try:
                        log_history(target_sheet_id, action="Tambah Kolom", details=f"Attempt failed: {new_col_name}", user=user_name, status=f"FAILED: {e}")
                    except Exception:
                        pass

    else:  # Tambah Baris Baru
        st.info("Isi data untuk setiap kolom yang ada. Baris baru akan ditambahkan di akhir sheet.")
        header: list = ws.row_values(1) or []
        if not header:
            st.error("Header (baris 1) kosong. Tambahkan header terlebih dahulu sebelum menambah baris.")
            st.stop()

        with st.expander("Header saat ini", expanded=False):
            display_header_with_index(header)

        # Form input dinamis
        with st.form("form_tambah_baris", clear_on_submit=False):
            new_row_inputs = []
            cols = st.columns(2)
            for i, colname in enumerate(header):
                with cols[i % 2]:
                    val = st.text_input(f"{colname}", key=f"row_{i}")
                new_row_inputs.append(val)

            submitted = st.form_submit_button("Tambahkan Baris")
            if submitted:
                if len(new_row_inputs) < len(header):
                    new_row_inputs += [""] * (len(header) - len(new_row_inputs))
                elif len(new_row_inputs) > len(header):
                    new_row_inputs = new_row_inputs[:len(header)]

                try:
                    ws.append_row(new_row_inputs, value_input_option="USER_ENTERED")  # type: ignore[arg-type]
                    st.success("Baris baru berhasil ditambahkan.")

                    # log history
                    details = f"Added row with values: {new_row_inputs[:5]}{'...' if len(new_row_inputs)>5 else ''}"
                    try:
                        log_history(target_sheet_id, action="Tambah Baris", details=details, user=user_name, status="SUCCESS")
                    except Exception as e:
                        st.warning(f"Gagal mencatat history: {e}")

                    st.rerun()
                except Exception as e:
                    st.error(f"Gagal menambahkan baris: {e}")
                    try:
                        log_history(target_sheet_id, action="Tambah Baris", details=f"Attempt failed: {e}", user=user_name, status=f"FAILED: {e}")
                    except Exception:
                        pass

# ---------------- MENU: History ----------------
else:
    st.subheader("History Log (Hasil Pencatatan Aksi Tambah Data)")

    if not have_write_creds():
        st.warning("Kredensial service account belum tersedia. History akan gagal dimuat tanpa kredensial.")
        st.stop()

    hist_target = st.selectbox("Pilih spreadsheet untuk lihat history:", ["Konsumsi (SHEET_ID_CONS)", "LBKB (SHEET_ID_LBKB)"], index=0)
    hist_sheet_id = SHEET_ID_CONS if "Konsumsi" in hist_target else SHEET_ID_LBKB

    try:
        client = get_gspread_client()
        sh = client.open_by_key(hist_sheet_id)
        try:
            hist_ws = sh.worksheet(HISTORY_SHEET_NAME)
        except Exception:
            st.info("Belum ada history untuk spreadsheet ini.")
            if st.button("Buat sheet history sekarang"):
                hist_ws = _ensure_history_ws(sh)
                st.success("Sheet history dibuat. Aksi selanjutnya akan dicatat otomatis.")
                st.rerun()
            st.stop()

        data = hist_ws.get_all_records()
        df_hist = pd.DataFrame(data)
    except Exception as e:
        st.error(f"Gagal memuat history: {e}")
        st.stop()

    if df_hist.empty:
        st.info("Tidak ada entri history.")
        st.stop()

    # Normalize Timestamp column into timezone-aware datetimes (Asia/Jakarta)
    df_hist['Timestamp_dt'] = pd.to_datetime(df_hist['Timestamp'], errors='coerce')

    # If parsed datetimes are tz-naive, localize them to Asia/Jakarta for consistent filtering
    try:
        if not df_hist['Timestamp_dt'].isna().all():
            sample = df_hist['Timestamp_dt'].dropna().iloc[0]
            if sample.tzinfo is None:
                try:
                    # pandas tz_localize supports string tz
                    df_hist['Timestamp_dt'] = df_hist['Timestamp_dt'].dt.tz_localize('Asia/Jakarta')
                except Exception:
                    # fallback: leave as naive
                    pass
    except Exception:
        pass

    # Filters
    st.write("### Filter History")
    col1, col2, col3 = st.columns([2,2,2])
    with col1:
        user_filter = st.text_input("Filter by user (contains):")
    with col2:
        action_filter = st.multiselect("Action", options=sorted(df_hist['Action'].unique().tolist()), default=df_hist['Action'].unique().tolist())
    with col3:
        # default date inputs: use min/max from Timestamp_dt converted to Asia/Jakarta dates if possible
        try:
            date_series = df_hist['Timestamp_dt']
            try:
                date_min_def = date_series.dt.tz_convert('Asia/Jakarta').dt.date.min()
                date_max_def = date_series.dt.tz_convert('Asia/Jakarta').dt.date.max()
            except Exception:
                date_min_def = date_series.dt.date.min()
                date_max_def = date_series.dt.date.max()
            date_min = st.date_input("Dari tanggal:", value=pd.to_datetime(date_min_def))
            date_max = st.date_input("Sampai tanggal:", value=pd.to_datetime(date_max_def))
        except Exception:
            date_min = st.date_input("Dari tanggal:")
            date_max = st.date_input("Sampai tanggal:")

    # build mask using Asia/Jakarta local dates
    try:
        try:
            ts_dates = df_hist['Timestamp_dt'].dt.tz_convert('Asia/Jakarta').dt.date
        except Exception:
            ts_dates = df_hist['Timestamp_dt'].dt.date
        mask = (ts_dates >= pd.to_datetime(date_min).date()) & (ts_dates <= pd.to_datetime(date_max).date())
    except Exception:
        mask = pd.Series([True] * len(df_hist))

    if user_filter:
        mask &= df_hist['User'].str.contains(user_filter, case=False, na=False)
    if action_filter:
        mask &= df_hist['Action'].isin(action_filter)

    df_shown = df_hist[mask].copy()
    st.write(f"Menampilkan {len(df_shown)} entri history")
    # drop helper column for display
    display_df = df_shown.drop(columns=['Timestamp_dt'], errors='ignore')
    st.dataframe(display_df, use_container_width=True)

    # Download
    buf = BytesIO()
    display_df.to_excel(buf, index=False)
    buf.seek(0)
    st.download_button(label="Download History (Excel)", data=buf, file_name="history_log.xlsx", mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

    st.caption("Kolom history: Timestamp (Asia/Jakarta), User, Action, TargetSheetTitle, TargetSheetId, Details, Status")
