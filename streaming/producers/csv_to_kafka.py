#!/usr/bin/env python3
"""
Publish rows from bronze_combined.csv (combined Bronze layer file) to Kafka topics
so that the streaming pipeline can pick them up.

Usage (inside container):
    python csv_to_kafka.py /app/data/bronze_combined.csv [sleep_min sleep_max]

sleep_min/max are seconds for random interval between each message (default 0.2-1.0).
"""
import sys, csv, json, random, time, os
from datetime import datetime
from kafka import KafkaProducer

CSV_PATH = sys.argv[1] if len(sys.argv) > 1 else "bronze_combined.csv"
SLEEP_MIN = float(sys.argv[2]) if len(sys.argv) > 2 else 0.2
SLEEP_MAX = float(sys.argv[3]) if len(sys.argv) > 3 else 1.0

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

topic_map = {
    "sales": "sales-events",
    "inventory": "inventory-updates",
    "metrics": "equipment-metrics",
}

def classify(row: dict) -> str:
    if row.get("event_id"):
        return "sales"
    if row.get("update_id"):
        return "inventory"
    if row.get("metric_id"):
        return "metrics"
    return "unknown"

print(f"CSV → Kafka | bootstrap={BOOTSTRAP} | file={CSV_PATH}")
producer = KafkaProducer(bootstrap_servers=BOOTSTRAP,
                         value_serializer=lambda v: json.dumps(v).encode())

with open(CSV_PATH, newline='', encoding='utf-8') as fh:
    reader = csv.DictReader(fh)
    sent = 0
    for row in reader:
        typ = classify(row)
        if typ == "unknown":
            continue
        topic = topic_map[typ]
        producer.send(topic, row)
        sent += 1
        if sent % 100 == 0:
            print(f"{datetime.now().isoformat()} Sent {sent} rows…")
        time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))

producer.flush()
print(f"✓ Finished streaming {sent} records from {CSV_PATH}") 