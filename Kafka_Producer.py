import pandas as pd
from confluent_kafka import Producer
import json
import sys
import time

# Hàm callback để xác nhận thông điệp đã được gửi
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Cấu hình Kafka producer
conf = {
    'bootstrap.servers': 'localhost:9092',  # Địa chỉ Kafka broker
}

# Khởi tạo Kafka producer
producer = Producer(conf)

# Đường dẫn tới file CSV
csv_file_path = '/Users/macbook/Downloads/log_action.csv'

# Đọc dữ liệu từ file CSV
data = pd.read_csv(csv_file_path)

# Duyệt qua từng dòng dữ liệu và gửi tới Kafka topic
for index, row in data.iterrows():
    record = {
        "student_code": row[0],
        "activity": row[1],
        "numberofFile": row[2],
        "timestamps": row[3]
    }

    # Chuyển đổi record sang định dạng JSON
    record_json = json.dumps(record)

    # Gửi thông điệp tới Kafka topic 'vdt2024'
    producer.produce('vdt_2024', value=record_json, callback=delivery_report)

    # Đảm bảo rằng tất cả thông điệp đã được gửi
    producer.flush()
    print(record_json)
    # Tùy chọn: Thêm thời gian nghỉ giữa các lần gửi thông điệp (ví dụ: 1 giây)
    time.sleep(1)

print("All messages sent!")