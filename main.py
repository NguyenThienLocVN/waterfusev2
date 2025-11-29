import os
import json
import requests
from datetime import datetime
import time


folder_paths = [
    r"D:\FTP_files\TDTANIET",
    r"D:\FTP_files\TDPACHIEN",
    r"D:\FTP_files\TDNAMXA",
    r"D:\FTP_files\TDSUOISAP1",
    r"D:\FTP_files\TDHANGDONGA",
    r"D:\FTP_files\TDNAMTRAI4",
    r"D:\FTP_files\TDNAMSOI",
    r"D:\FTP_files\TDHANGDONGA1",
    r"D:\FTP_files\TDMUONGHUNG",
    r"D:\FTP_files\TDSUOISAP2A",
    r"D:\FTP_files\TDNAMHONG1",
    r"D:\FTP_files\TDNAMHONG2",
    r"D:\FTP_files\TDNAMGION",
    r"D:\FTP_files\TDNAMPIA",
    r"D:\FTP_files\TDCHIENGCONG1",
    r"D:\FTP_files\TDCHIENGCONG2",
    r"D:\FTP_files\TDMUONGSANG",
	r"D:\FTP_files\TLBANMONG",
    r"D:\FTP_files\NDDCTTAKIIVN"
]
log_path = r"LOGS\error_log.txt"
processed_path = r"LOGS\processed_files.txt"

login_url = "https://tnn.sonnmt.sonla.gov.vn/api/authen/login?Username=dev.locntx&Password=loc@vtv175"
data_url = "https://tnn.sonnmt.sonla.gov.vn/api/StoragePreData/save"

def process_folders():
    session = requests.Session()
    try:
        login_res = session.get(login_url)
        if login_res.ok or login_res.json() is True:
            print("🔓 Đăng nhập thành công!")
            processed_files = set()
            if os.path.exists(processed_path):
                with open(processed_path, "r", encoding="utf-8") as f:
                    processed_files = set(line.strip() for line in f if line.strip())

            processed_folder_names = []
            for folder_path in folder_paths:
                if not os.path.isdir(folder_path):
                    print(f"Thư mục không tồn tại: {folder_path}")
                    continue

                # Điều kiện đặc biệt cho công trình NDDCTTAKIIVN
                if folder_path == r"D:\FTP_files\NDDCTTAKIIVN":
                    sub_folders = [
                        r"D:\FTP_files\NDDCTTAKIIVN\TK01",
                        r"D:\FTP_files\NDDCTTAKIIVN\TK02",
                        r"D:\FTP_files\NDDCTTAKIIVN\TK03",
                        r"D:\FTP_files\NDDCTTAKIIVN\TK04",
                    ]
                    for sub_folder in sub_folders:
                        if not os.path.isdir(sub_folder):
                            print(f"Thư mục không tồn tại: {sub_folder}")
                            continue
                        print(f"📂 Đang xử lý thư mục con: {sub_folder}")
                        for filename in os.listdir(sub_folder):
                            if not (filename.endswith(".txt") and filename.startswith("SL_")):
                                continue
                            if filename in processed_files:
                                print(f"Đã xử lý rồi: {filename}")
                                continue

                            file_path = os.path.join(sub_folder, filename)
                            basename = os.path.splitext(filename)[0]
                            parts = basename.split("_")
                            if len(parts) != 4:
                                print(f"⚠️ Tên file không hợp lệ: {filename}")
                                continue

                            _, construction_code, sub_code, time_str = parts
                            construction_code = f"{construction_code}_{sub_code}"
                            try:
                                time_iso = datetime.strptime(time_str, "%Y%m%d%H%M%S").isoformat() + "Z"
                            except ValueError:
                                print(f"⚠️ Không phân tích được thời gian: {filename}")
                                continue

                            items = []
                            with open(file_path, "r", encoding="utf-8") as file:
                                for line_num, line in enumerate(file, start=1):
                                    parts = line.strip().split()
                                    if len(parts) >= 3:
                                        station_code = parts[0]
                                        raw_value = parts[2]
                                        unit = parts[3]
                                        try:
                                            numeric_value = float(raw_value)
                                            value = int(numeric_value) if numeric_value.is_integer() else round(numeric_value, 2)
                                            items.append({
                                                "ConstructionCode": construction_code,
                                                "Time": time_iso,
                                                "StationCode": station_code,
                                                "Value": value,
                                                "Unit": unit,  
                                                "DeviceStatus": 0,
                                                "Status": True
                                            })
                                        except ValueError:
                                            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                            with open(log_path, "a", encoding="utf-8") as log_file:
                                                log_file.write(f"[{timestamp}] ❌ {filename} - dòng {line_num}: Không chuyển được '{raw_value}' thành số\n")
                            if items:
                                try:
                                    headers = { "Content-Type": "application/json" }
                                    response = session.post(data_url, data=json.dumps(items), headers=headers)
                                    if response.status_code == 200:
                                        print(f"✅ Đọc thành công file: {filename} ({len(items)} bản ghi)")
                                        with open(processed_path, "a", encoding="utf-8") as f:
                                            f.write(filename + "\n")
                                    else:
                                        print(f"Gửi thất bại: {response.status_code} - {response.text}")
                                        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                        with open(log_path, "a", encoding="utf-8") as log_file:
                                            log_file.write(f"[{timestamp}] ❌ Lỗi gửi file {filename}: {response.status_code} - {response.text}\n")
                                except Exception as e:
                                    print(f"Lỗi gửi dữ liệu từ {filename}: {e}")
                            else:
                                print(f"Không có bản ghi hợp lệ trong file {filename}")
                    continue

                print(f"📂 Đang xử lý thư mục: {folder_path}")
                # Extract the folder name part after the last backslash
                try:
                    folder_name = os.path.basename(folder_path.rstrip('\\/'))
                    processed_folder_names.append(folder_name)
                except Exception:
                    # Fallback to full path if basename extraction fails
                    processed_folder_names.append(folder_path)
                for filename in os.listdir(folder_path):
                    if not (filename.endswith(".txt") and filename.startswith("SL_")):
                        continue
                    if filename in processed_files:
                        print(f"Đã xử lý rồi: {filename}")
                        continue

                    file_path = os.path.join(folder_path, filename)
                    basename = os.path.splitext(filename)[0]
                    parts = basename.split("_")
                    if len(parts) != 3:
                        print(f"⚠️ Tên file không hợp lệ: {filename}")
                        continue

                    _, construction_code, time_str = parts
                    try:
                        time_iso = datetime.strptime(time_str, "%Y%m%d%H%M%S").isoformat() + "Z"
                    except ValueError:
                        print(f"⚠️ Không phân tích được thời gian: {filename}")
                        continue

                    items = []
                    with open(file_path, "r", encoding="utf-8") as file:
                        for line_num, line in enumerate(file, start=1):
                            parts = line.strip().split()
                            if len(parts) >= 3:
                                station_code = parts[0]
                                raw_value = parts[2]
                                unit = parts[3]
                                try:
                                    numeric_value = float(raw_value)
                                    value = int(numeric_value) if numeric_value.is_integer() else round(numeric_value, 2)
                                    items.append({
                                        "ConstructionCode": construction_code,
                                        "Time": time_iso,
                                        "StationCode": station_code,
                                        "Value": value,
                                        "Unit": unit, 
                                        "DeviceStatus": 0,
                                        "Status": True
                                    })
                                except ValueError:
                                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                    with open(log_path, "a", encoding="utf-8") as log_file:
                                        log_file.write(f"[{timestamp}] ❌ {filename} - dòng {line_num}: Không chuyển được '{raw_value}' thành số\n")
                    if items:
                        try:
                            headers = { "Content-Type": "application/json" }
                            response = session.post(data_url, data=json.dumps(items), headers=headers)
                            if response.status_code == 200:
                                print(f"✅ Đọc thành công file: {filename} ({len(items)} bản ghi)")
                                with open(processed_path, "a", encoding="utf-8") as f:
                                    f.write(filename + "\n")
                            else:
                                print(f"Gửi thất bại: {response.status_code} - {response.text}")
                                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                with open(log_path, "a", encoding="utf-8") as log_file:
                                    log_file.write(f"[{timestamp}] ❌ Lỗi gửi file {filename}: {response.status_code} - {response.text}\n")
                        except Exception as e:
                            print(f"Lỗi gửi dữ liệu từ {filename}: {e}")
                    else:
                        print(f"Không có bản ghi hợp lệ trong file {filename}")
        else:
            print(f"Đăng nhập thất bại: {login_res.status_code} - {login_res.text}")
    except Exception as e:
        print(f"⚠️ Lỗi khi gửi request đăng nhập: {e}")
    return processed_folder_names if 'processed_folder_names' in locals() else []

if __name__ == "__main__":
    while True:
        processed = process_folders()
        print(f"Bắt đầu xử lý lúc {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        if processed:
            print("Các thư mục đã thực hiện đọc:")
            for name in processed:
                print(f"- {name}")
        else:
            print("Không có thư mục hợp lệ được đọc trong lần chạy này.")
        print("Đợi 15 phút để chạy lại...")
        print("============VUI LÒNG KHÔNG TẮT CỬA SỔ NÀY==============")
        time.sleep(900)

