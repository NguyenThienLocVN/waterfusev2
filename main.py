import os
import json
import requests
from datetime import datetime

filename = "SL_TDDAKDRINK_20241125001500.txt"
basename = os.path.splitext(filename)[0]
_, construction_code, time_str = basename.split("_")
time_iso = datetime.strptime(time_str, "%Y%m%d%H%M%S").isoformat() + "Z"

log_path = "error_log.txt"
api_url = "https://tnn.sonnmt.sonla.gov.vn/api/StoragePreData/save"

file_path = os.path.join(os.getcwd(), filename)
with open(file_path, "r", encoding="utf-8") as file:
    for line_num, line in enumerate(file, start=1):
        parts = line.strip().split()
        if len(parts) >= 3:
            station_code = parts[0]
            raw_value = parts[2]

            try:
                numeric_value = float(raw_value)
                value = int(numeric_value) if numeric_value.is_integer() else round(numeric_value, 2)

                item = {
                    "ConstructionCode": construction_code,
                    "Time": time_iso,
                    "StationCode": station_code,
                    "Value": value,
                    "DeviceStatus": 0,
                    "Status": True
                }

                # Gửi đến API
                response = requests.post(api_url, json=item)
                if response.status_code == 200:
                    print(f"✅ Gửi thành công: {station_code}")
                else:
                    print(f"⚠️ Gửi thất bại: {station_code} - Mã lỗi {response.status_code}")
                    with open(log_path, "a", encoding="utf-8") as log_file:
                        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        log_file.write(f"[{timestamp}] ❌ Lỗi gửi {station_code}: {response.status_code} - {response.text}\n")

            except ValueError:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                with open(log_path, "a", encoding="utf-8") as log_file:
                    log_file.write(f"[{timestamp}] ❌ Lỗi dòng {line_num} trong file {filename}: Không chuyển được '{raw_value}' thành số\n")
