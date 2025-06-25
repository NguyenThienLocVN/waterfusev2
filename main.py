import os
import json
import requests

# --- BƯỚC 1: Lấy thông tin từ tên file ---
filename = "SL_TDDAKDRINK_20241125001500.txt"
basename = os.path.splitext(filename)[0]
_, construction_code, time_str = basename.split("_")

# --- Khởi tạo biến data_dict trước khi sử dụng ---
data_dict = {}

# --- BƯỚC 2: Đọc và phân tích nội dung file ---
file_path = os.path.join(os.getcwd(), filename)

with open(file_path, "r", encoding="utf-8") as file:
    for line in file:
        parts = line.strip().split()
        if len(parts) >= 3:
            key = parts[0]
            type_ = parts[1]
            value = parts[2]
            data_dict[key] = value

# --- BƯỚC 3: Tạo JSON theo yêu cầu ---
storePreData = {
    "ConstructionCode": construction_code,
    "Time": time_str,
    "Data": data_dict
}

# --- BƯỚC 4: Gửi dữ liệu qua API (nếu cần) ---
# url = "https://your-api-endpoint.com/store"
# headers = {"Content-Type": "application/json"}
# response = requests.post(url, headers=headers, data=json.dumps(storePreData))
# print(f"Status: {response.status_code} - {response.text}")

# In ra dữ liệu để kiểm tra
print(json.dumps(storePreData, indent=2, ensure_ascii=False))