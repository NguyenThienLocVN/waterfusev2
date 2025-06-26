import os
import json
import requests
from datetime import datetime

# --- Lấy thông tin từ tên file ---
filename = "SL_TDDAKDRINK_20241125001500.txt"
basename = os.path.splitext(filename)[0]
_, construction_code, time_str = basename.split("_")

# --- Khởi tạo biến data_dict trước khi sử dụng ---
data_dict = {}

# Chuyển đổi thời gian sang định dạng ISO 8601 (YYYY-MM-DDTHH:mm:ss.sssZ)
time_iso = datetime.strptime(time_str, "%Y%m%d%H%M%S").isoformat() + "Z"

result_array = []

log_path = "error_log.txt"

# --- Đọc và phân tích nội dung file ---
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

                result_array.append({
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
                    log_file.write(f"Loi dong {line_num} trong file {filename}: Không chuyển được '{raw_value}' thành số\n")

# --- Gửi dữ liệu qua API (nếu cần) ---
# url = "https://your-api-endpoint.com/store"
# headers = {"Content-Type": "application/json"}
# response = requests.post(url, headers=headers, data=json.dumps(storePreData))
# print(f"Status: {response.status_code} - {response.text}")

# In ra dữ liệu để kiểm tra
print(json.dumps(result_array, indent=2, ensure_ascii=False))