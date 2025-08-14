#consumer

import os, time, json
from kafka import KafkaConsumer, errors
from datetime import datetime

bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
topic = os.getenv('KAFKA_TOPIC', 'jobs')
output_dir  = os.getenv("OUTPUT_DIR", "/data/raw")

os.makedirs(output_dir, exist_ok=True)

consumer = KafkaConsumer(
    topic,
    bootstrap_servers=bootstrap,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
)

print(f"Starting consumer for topic '{topic}' at {bootstrap}... writing to '{output_dir}'")

for message in consumer:
    payload = message.value
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    filename  = f"{output_dir}/job_{ts}_{message.partition}_{message.offset}.json"
    with open(filename, 'w') as f:
        json.dump(payload, f, indent=2)
    print(f"Written message to {filename}")


