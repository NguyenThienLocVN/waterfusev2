import requests
import os
from datetime import datetime
import json

def login_to_api():
    login_url = "https://tnn.sonnmt.sonla.gov.vn/api/authen/login?Username=dev.locntx&Password=loc@vtv175"

    try:
        session = requests.Session()
        login_res = session.get(login_url)

        if login_res.status_code == 200:
            session_data = login_res.json()
            print("Login successful!.")
            return session
        else:
            print(f"Login failed with status code: {login_res.status_code}")
            print(f"Response: {login_res.text}")
            return None

    except Exception as e:
        print(f"An error occurred during login: {e}")
        return None

def process_and_post_data(session):
    data_file = "data/SL_NMNBANMONG.txt"
    data_url = "https://tnn.sonnmt.sonla.gov.vn/api/StoragePreData/save"
    success_log = "LOGS/NMNBANMONG_processed.txt"
    error_log = "LOGS/NMNBANMONG_errors.txt"

    headers = {"Content-Type": "application/json"}  # Added headers

    if not os.path.exists(data_file):
        print(f"Data file not found: {data_file}")
        return

    try:
        with open(data_file, "r") as file:
            lines = file.readlines()

        success_count = 0
        error_count = 0

        for index, line in enumerate(lines, start=1):
            print(f"Processing line {index}: {line.strip()}")  # Display the current line being processed
            try:
                # Parse the line into fields
                fields = line.strip().split()
                if len(fields) != 7:
                    print(f"Invalid line format: {line.strip()}")
                    raise ValueError("Invalid data format")

                ConstructionCode, StationCode, ParameterName, Value, Unit, Time, DeviceStatus = fields

                formatted_time = datetime.strptime(Time, "%Y%m%d%H%M%S").strftime("%Y-%m-%d %H:%M:%S.000")

                payload = {
                    "ConstructionCode": ConstructionCode,
                    "StationCode": StationCode,
                    "ParameterName": ParameterName,
                    "Value": Value,
                    "Unit": Unit,
                    "Time": formatted_time,
                    "DeviceStatus": DeviceStatus
                }

                # Send the POST request using the session
                response = session.post(data_url, data=json.dumps(payload), headers=headers)

                print(f"API response: {response.status_code}, {response.text}")

                if response.status_code == 200:
                    success_count += 1
                    with open(success_log, "a") as log_file:
                        log_file.write(f"SUCCESS: {line.strip()}\n")
                else:
                    error_count += 1
                    with open(error_log, "a") as log_file:
                        log_file.write(f"ERROR: {line.strip()} | Response: {response.text}\n")

            except Exception as e:
                error_count += 1
                with open(error_log, "a") as log_file:
                    log_file.write(f"ERROR: {line.strip()} | Exception: {e}\n")

        print(f"Processing completed. Success: {success_count}, Errors: {error_count}")

    except Exception as e:
        print(f"An error occurred while processing the file: {e}")

# Example usage
if __name__ == "__main__":
    session = login_to_api()
    if session:
        print("Session established.")
        process_and_post_data(session)
    else:
        print("Login failed. Data processing will not proceed.")