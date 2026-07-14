# -*- coding: ascii -*-
import os
import time
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, IO, List, Optional, Set, Tuple, Union

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# =========================
# CAU HINH
# =========================

FOLDER_PATHS = [
    r"D:\FTP_files\TDNAMSOI",
    r"D:\FTP_files\TDHANGDONGA1",
    r"D:\FTP_files\TDMUONGHUNG",
    r"D:\FTP_files\TDPACHIEN",
    r"D:\FTP_files\TLBANMONG",
    r"D:\FTP_files\TDSUOISAP1",
    r"D:\FTP_files\TLCHIENGDONG",
    r"D:\FTP_files\TDHANGDONGA",
    r"D:\FTP_files\TDNAMHONG2",
    r"D:\FTP_files\TDMUONGSANG2",
    r"D:\FTP_files\TDNAMCHIM1A",
    r"D:\FTP_files\TDNAMCHIEN2",
    r"D:\FTP_files\TDNAMTRAI4",
    r"D:\FTP_files\TDTATNGOANG",
    r"D:\FTP_files\TDNAMBU",
    r"D:\FTP_files\TDCHIENGCONG1",
    r"D:\FTP_files\TDNAMCONG5",
    r"D:\FTP_files\TDNAMGION",
    r"D:\FTP_files\TDNAMPIA",
    r"D:\FTP_files\TDCHIENGCONG2",
    r"D:\FTP_files\TDNAMXA",
    r"D:\FTP_files\TDNAMCONG4",
    r"D:\FTP_files\TDXIMVANG2",
    r"D:\FTP_files\TDNAMCHANH",
    r"D:\FTP_files\TDSAPVIET",
    r"D:\FTP_files\TDPHIENGCON",
r"D:\FTP_files\TDTACO",
    r"D:\FTP_files\TDNAMCONG3",
    r"D:\FTP_files\TDSUOISAP3",
]


BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "LOGS"
LOG_DIR.mkdir(parents=True, exist_ok=True)

LOG_PATH = LOG_DIR / "error_log.txt"
PROCESSED_PATH = LOG_DIR / "processed_files.txt"

LOGIN_URL = "https://tnn.sonnmt.sonla.gov.vn/api/authen/login"
DATA_URL = "https://tnn.sonnmt.sonla.gov.vn/api/StoragePreData/save"

# Nen dat bien moi truong tren may chu thay vi ghi mat khau truc tiep trong code.
# CMD:
#   setx TNN_USERNAME "dev.locntx"
#   setx TNN_PASSWORD "mat_khau_cua_ban"
USERNAME = os.getenv("TNN_USERNAME", "dev.locntx")
PASSWORD = os.getenv("TNN_PASSWORD", "loc@vtv175")
LOGIN_METHOD = os.getenv("TNN_LOGIN_METHOD", "GET").upper()
LOGIN_PAYLOAD_FORMAT = os.getenv("TNN_LOGIN_PAYLOAD_FORMAT", "params").lower()

REQUEST_TIMEOUT = (10, 60)
SLEEP_SECONDS = 900

# Giam spam console de tang toc khi thu muc co nhieu file da xu ly.
PRINT_SKIPPED_FILES = False


# =========================
# LOGGING
# =========================

logging.basicConfig(
    filename=str(LOG_PATH),
    level=logging.INFO,
    encoding="ascii",
    errors="backslashreplace",
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def log_error(message: str) -> None:
    logging.error(message)


def log_info(message: str) -> None:
    logging.info(message)


# =========================
# HTTP
# =========================

def create_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": "WaterFuseClient/2.1 (+python-requests)",
        "Accept": "application/json, text/plain, */*",
    })

    retry = Retry(
        total=3,
        connect=3,
        read=3,
        status=3,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )

    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def try_parse_json(response: requests.Response) -> Optional[Any]:
    try:
        return response.json()
    except ValueError:
        return None


def login(session: requests.Session) -> bool:
    if not USERNAME or not PASSWORD:
        print("Thieu TNN_USERNAME hoac TNN_PASSWORD. Vui long dat bien moi truong truoc khi chay.")
        log_error("Thieu bien moi truong TNN_USERNAME hoac TNN_PASSWORD.")
        return False

    print(f"Dang dang nhap bang {LOGIN_METHOD} (payload={LOGIN_PAYLOAD_FORMAT})...")

    try:
        if LOGIN_METHOD == "GET":
            response = session.get(
                LOGIN_URL,
                params={"Username": USERNAME, "Password": PASSWORD},
                timeout=REQUEST_TIMEOUT,
            )
        elif LOGIN_PAYLOAD_FORMAT == "json":
            response = session.post(
                LOGIN_URL,
                json={"Username": USERNAME, "Password": PASSWORD},
                timeout=REQUEST_TIMEOUT,
            )
        else:
            response = session.post(
                LOGIN_URL,
                data={"Username": USERNAME, "Password": PASSWORD},
                timeout=REQUEST_TIMEOUT,
            )
    except requests.exceptions.ConnectionError as exc:
        print(f"Loi ket noi khi dang nhap: {exc}")
        log_error(f"Loi ket noi khi dang nhap: {repr(exc)}")
        return False
    except requests.exceptions.Timeout as exc:
        print(f"Timeout khi dang nhap: {exc}")
        log_error(f"Timeout khi dang nhap: {repr(exc)}")
        return False
    except requests.exceptions.RequestException as exc:
        print(f"Loi request khi dang nhap: {exc}")
        log_error(f"Loi request khi dang nhap: {repr(exc)}")
        return False

    body_json = try_parse_json(response)
    body_text = response.text[:500] if response.text else ""

    if response.status_code != 200:
        print(f"Dang nhap that bai: HTTP {response.status_code} - {body_text}")
        log_error(f"Dang nhap that bai: HTTP {response.status_code} - {body_text}")
        return False

    if body_json is True:
        print("Dang nhap thanh cong!")
        return True

    if isinstance(body_json, dict):
        token = (
            body_json.get("token")
            or body_json.get("Token")
            or body_json.get("accessToken")
            or body_json.get("AccessToken")
            or body_json.get("jwt")
            or body_json.get("JWT")
        )

        success_value = body_json.get("success", body_json.get("Success", None))

        if token:
            session.headers.update({"Authorization": f"Bearer {token}"})
            print("Dang nhap thanh cong, da nhan token!")
            return True

        if success_value is True:
            print("Dang nhap thanh cong!")
            return True

        print(f"Dang nhap HTTP 200 nhung noi dung khong xac nhan thanh cong: {body_json}")
        log_error(f"Dang nhap HTTP 200 nhung noi dung khong xac nhan thanh cong: {body_json}")
        return False

    if body_text.strip().lower() == "true":
        print("Dang nhap thanh cong!")
        return True

    print(f"Dang nhap HTTP 200 nhung response khong ro rang. Noi dung: {body_text}")
    log_error(f"Dang nhap HTTP 200 nhung response khong ro rang: {body_text}")
    return True


def post_items(session: requests.Session, filename: str, items: List[Dict[str, Any]]) -> bool:
    try:
        response = session.post(
            DATA_URL,
            json=items,
            timeout=REQUEST_TIMEOUT,
            headers={"Content-Type": "application/json"},
        )
    except requests.exceptions.ConnectionError as exc:
        print(f"Loi ket noi khi gui du lieu file {filename}: {exc}")
        log_error(f"Loi ket noi khi gui du lieu file {filename}: {repr(exc)}")
        return False
    except requests.exceptions.Timeout as exc:
        print(f"Timeout khi gui du lieu file {filename}: {exc}")
        log_error(f"Timeout khi gui du lieu file {filename}: {repr(exc)}")
        return False
    except requests.exceptions.RequestException as exc:
        print(f"Loi request khi gui du lieu file {filename}: {exc}")
        log_error(f"Loi request khi gui du lieu file {filename}: {repr(exc)}")
        return False

    if response.status_code == 200:
        print(f"Gui thanh cong file: {filename} ({len(items)} ban ghi)")
        return True

    print(f"Gui that bai file {filename}: HTTP {response.status_code} - {response.text[:500]}")
    log_error(f"Loi gui file {filename}: HTTP {response.status_code} - {response.text[:1000]}")
    return False


# =========================
# XU LY FILE
# =========================

def normalize_path(file_path: str) -> str:
    return os.path.normpath(file_path)


def load_processed_files() -> Set[str]:
    if not PROCESSED_PATH.exists():
        return set()

    with PROCESSED_PATH.open("r", encoding="utf-8", errors="ignore") as f:
        return {line.strip() for line in f if line.strip()}


def mark_processed(file_path: str, processed_writer: IO[str]) -> str:
    normalized = normalize_path(file_path)
    processed_writer.write(normalized + "\n")
    return normalized


def is_processed(file_path: str, filename: str, processed_files: Set[str]) -> bool:
    normalized = normalize_path(file_path)
    return normalized in processed_files or filename in processed_files


def parse_time_from_filename(time_str: str, filename: str) -> Optional[str]:
    try:
        return datetime.strptime(time_str, "%Y%m%d%H%M%S").isoformat() + "Z"
    except ValueError:
        print(f"Khong phan tich duoc thoi gian tu ten file: {filename}")
        log_error(f"{filename}: Khong phan tich duoc thoi gian '{time_str}' theo dinh dang YYYYMMDDHHMMSS.")
        return None


def normalize_value(raw_value: str, filename: str, line_num: int) -> Optional[Union[float, int]]:
    try:
        numeric_value = float(raw_value)
    except ValueError:
        print(f"{filename} - dong {line_num}: Khong chuyen duoc '{raw_value}' thanh so")
        log_error(f"{filename} - dong {line_num}: Khong chuyen duoc '{raw_value}' thanh so.")
        return None

    return int(numeric_value) if numeric_value.is_integer() else round(numeric_value, 2)


def read_normal_file(file_path: str, filename: str, construction_code: str, time_iso: str) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    append_item = items.append

    with open(file_path, "r", encoding="utf-8-sig", errors="replace") as file:
        for line_num, line in enumerate(file, start=1):
            if not line.strip():
                continue

            parts = line.split()
            if len(parts) < 4:
                log_error(f"{filename} - dong {line_num}: Thieu cot, noi dung: {line.rstrip()}")
                continue

            value = normalize_value(parts[2], filename, line_num)
            if value is None:
                continue

            append_item({
                "ConstructionCode": construction_code,
                "Time": time_iso,
                "StationCode": parts[0],
                "Value": value,
                "Unit": parts[3],
                "DeviceStatus": 0,
                "Status": True,
            })

    return items



def parse_file_info(filename: str) -> Optional[Tuple[str, str]]:
    basename = os.path.splitext(filename)[0]
    name_parts = basename.split("_")

    if len(name_parts) != 3:
        print(f"Ten file khong hop le: {filename}")
        log_error(f"Ten file khong hop le: {filename}")
        return None

    _, construction_code, time_str = name_parts
    return construction_code, time_str


def process_file(
    session: requests.Session,
    file_path: str,
    filename: str,
    processed_files: Set[str],
    processed_writer: IO[str],
) -> bool:
    if is_processed(file_path, filename, processed_files):
        if PRINT_SKIPPED_FILES:
            print(f"Da xu ly roi: {filename}")
        return False

    file_info = parse_file_info(filename)
    if not file_info:
        return False

    construction_code, time_str = file_info
    time_iso = parse_time_from_filename(time_str, filename)
    if not time_iso:
        return False

    items = read_normal_file(file_path, filename, construction_code, time_iso)

    if not items:
        print(f"Khong co ban ghi hop le trong file {filename}")
        return False

    if post_items(session, filename, items):
        normalized = mark_processed(file_path, processed_writer)
        processed_files.add(normalized)
        return True

    return False


def iter_candidate_files(folder_path: str):
    try:
        with os.scandir(folder_path) as entries:
            for entry in entries:
                if not entry.is_file():
                    continue

                filename = entry.name
                if filename.startswith("SL_") and filename.endswith(".txt"):
                    yield entry.path, filename

    except OSError as exc:
        raise OSError(f"Khong doc duoc thu muc {folder_path}: {repr(exc)}") from exc


def process_single_folder(
    session: requests.Session,
    folder_path: str,
    processed_files: Set[str],
    processed_writer: IO[str],
) -> Tuple[str, int]:
    folder_name = os.path.basename(folder_path.rstrip("\\/"))

    if not os.path.isdir(folder_path):
        print(f"Thu muc khong ton tai: {folder_path}")
        log_error(f"Thu muc khong ton tai: {folder_path}")
        return folder_name, 0

    print(f"Dang xu ly thu muc: {folder_path}")
    success_count = 0

    try:
        candidate_files = iter_candidate_files(folder_path)
        for file_path, filename in candidate_files:
            if process_file(session, file_path, filename, processed_files, processed_writer):
                success_count += 1

    except OSError as exc:
        print(str(exc))
        log_error(str(exc))
        return folder_name, 0

    return folder_name, success_count


def process_folders() -> List[Tuple[str, int]]:
    session = create_session()

    if not login(session):
        return []

    processed_files = load_processed_files()
    processed_results: List[Tuple[str, int]] = []

    with PROCESSED_PATH.open("a", encoding="utf-8", errors="ignore", buffering=1) as processed_writer:
        for folder_path in FOLDER_PATHS:
            processed_results.append(
                process_single_folder(session, folder_path, processed_files, processed_writer)
            )

    return processed_results


def print_run_summary(processed_results: List[Tuple[str, int]]) -> None:
    valid_results = [(name, count) for name, count in processed_results if count > 0]

    if valid_results:
        print("Cac thu muc da gui du lieu thanh cong:")
        for name, count in valid_results:
            print(f"- {name}: {count} file")
    else:
        print("Khong co file moi nao duoc gui thanh cong trong lan chay nay.")


def main() -> None:
    while True:
        started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        processed_results = process_folders()
        print_run_summary(processed_results)

        print(f"Bat dau xu ly luc: {started_at}")
        print("Doi 15 phut de chay lai...")
        print("============VUI LONG KHONG TAT CUA SO NAY==============")
        time.sleep(SLEEP_SECONDS)


if __name__ == "__main__":
    main()
