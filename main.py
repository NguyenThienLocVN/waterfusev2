import os
import json
import requests
from datetime import datetime

# --- Cấu hình ---
folder_input = input("Nhập các tên thư mục, cách nhau bởi dấu phẩy: ").strip()
folder_paths = [f.strip() for f in folder_input.split(",") if f.strip()]
log_path = r"LOGS\error_log.txt"
processed_path = r"LOGS\processed_files.txt"

# Tạo session giữ trạng thái login
session = requests.Session()

login_url = "https://tnn.sonnmt.sonla.gov.vn/api/authen/login?Username=dev.locntx&Password=loc@vtv175"
data_url = "https://tnn.sonnmt.sonla.gov.vn/api/StoragePreData/save"

# --- Đăng nhập ---
try:
    login_res = session.get(login_url)
    if login_res.ok or login_res.json() is True:
        print("🔓 Đăng nhập thành công!")
        # --- Đọc danh sách file đã xử lý ---
        processed_files = set()
        if os.path.exists(processed_path):
            with open(processed_path, "r", encoding="utf-8") as f:
                processed_files = set(line.strip() for line in f if line.strip())

        # --- Xử lý từng thư mục ---
        for folder_path in folder_paths:
            if not os.path.isdir(folder_path):
                print(f"Thư mục không tồn tại: {folder_path}")
                continue
            print(f"📂 Đang xử lý thư mục: {folder_path}")
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
                            try:
                                numeric_value = float(raw_value)
                                value = int(numeric_value) if numeric_value.is_integer() else round(numeric_value, 2)

                                items.append({
                                    "ConstructionCode": construction_code,
                                    "Time": time_iso,
                                    "StationCode": station_code,
                                    "Value": value,
                                    "DeviceStatus": 0,
                                    "Status": True
                                })
                            except ValueError:
                                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                with open(log_path, "a", encoding="utf-8") as log_file:
                                    log_file.write(f"[{timestamp}] ❌ {filename} - dòng {line_num}: Không chuyển được '{raw_value}' thành số\n")
                # print(json.dumps(items, indent=2, ensure_ascii=False))
                # --- Gửi mảng dữ liệu sau đăng nhập ---
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
        exit()
except Exception as e:
    print(f"⚠️ Lỗi khi gửi request đăng nhập: {e}")
    exit()

