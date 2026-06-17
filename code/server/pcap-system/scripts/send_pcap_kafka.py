#!/home/john/scripts/venv/bin/python

from kafka import KafkaProducer
import os
import math
import uuid
import time
import shutil

KAFKA_BROKER = "localhost:9092"
TOPIC = "safe-athens-pcap"
MERGED_DIR = "/home/john/safe-athens-server/pcap-system/merged"
SENT_DIR = "/home/john/safe-athens-server/pcap-system/sent"
LOG_FILE = "/home/john/safe-athens-server/pcap-system/logs/kafka_send.log"
CHUNK_SIZE = 512 * 1024

os.makedirs(MERGED_DIR, exist_ok=True)
os.makedirs(SENT_DIR, exist_ok=True)
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    max_request_size=10485760,
    request_timeout_ms=10000,
    api_version_auto_timeout_ms=10000
)

def log(msg: str):
    line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {msg}"
    print(line)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")

while True:
    files = sorted(
        f for f in os.listdir(MERGED_DIR)
        if f.endswith(".pcap")
    )

    for filename in files:
        file_path = os.path.join(MERGED_DIR, filename)

        if not os.path.isfile(file_path):
            continue

        try:
            file_id = str(uuid.uuid4())
            file_size = os.path.getsize(file_path)
            total_chunks = math.ceil(file_size / CHUNK_SIZE)

            log(f"Sending {filename} ({file_size} bytes) in {total_chunks} chunk(s)")

            with open(file_path, "rb") as f:
                for idx in range(total_chunks):
                    chunk = f.read(CHUNK_SIZE)
                    producer.send(
                        TOPIC,
                        key=file_id.encode(),
                        value=chunk,
                        headers=[
                            ("file_id", file_id.encode()),
                            ("filename", filename.encode()),
                            ("chunk_index", str(idx).encode()),
                            ("total_chunks", str(total_chunks).encode())
                        ]
                    )

            producer.flush()
            shutil.move(file_path, os.path.join(SENT_DIR, filename))
            log(f"Sent successfully and moved to sent/: {filename}")

        except Exception as e:
            log(f"ERROR sending {filename}: {e}")

    time.sleep(10)
