import json
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "lakehouse_raw_stream"

REGIONS = ["BKK-Central", "CNX-North", "HKT-South", "KKC-Northeast"]
CATEGORIES = ["Solar-Inverter", "EV-Charger", "Battery-Pack", "Smart-Meter"]
DEVICES = [f"GRID-NODE-{i:02d}" for i in range(1, 11)]


def ensure_topic():
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    md = admin.list_topics(timeout=5.0)
    if TOPIC not in md.topics:
        fs = admin.create_topics([NewTopic(TOPIC, num_partitions=1, replication_factor=1)])
        for _, f in fs.items():
            try:
                f.result(timeout=5.0)
            except Exception:
                pass


def main():
    ensure_topic()
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "linger.ms": 5})

    print("=" * 74)
    print(f"🚀 [LAKEHOUSE PRODUCER] กำลังส่งข้อมูลเข้า Kafka Topic: '{TOPIC}'")
    print("💡 ข้อมูลชุดนี้จะถูกแบ่งเป็น Hot Tier (ClickHouse) และ Cold Tier (MinIO S3 Parquet)")
    print("=" * 74)

    seq_no = 0
    try:
        while True:
            seq_no += 1
            device_id = random.choice(DEVICES)
            region = REGIONS[seq_no % len(REGIONS)]
            category = CATEGORIES[seq_no % len(CATEGORIES)]
            voltage = round(random.uniform(215.0, 242.0), 2)
            output_kw = round(random.uniform(4.5, 48.0), 3)
            revenue_thb = round(output_kw * 4.5, 2)
            event_id = f"LKH-{seq_no:06d}"

            payload = {
                "event_id": event_id,
                "seq_no": seq_no,
                "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                "region": region,
                "device_id": device_id,
                "category": category,
                "voltage": voltage,
                "output_kw": output_kw,
                "revenue_thb": revenue_thb,
            }

            producer.produce(TOPIC, key=device_id, value=json.dumps(payload).encode("utf-8"))
            producer.poll(0)

            if seq_no % 10 == 0:
                producer.flush(timeout=1.0)
                print(
                    f"📤 [PRODUCER] Sent #{seq_no} ({event_id}) | {region} | {device_id} ({category}) | {output_kw} kW"
                )

            time.sleep(0.15)  # ~6-7 events/sec (1 Parquet snapshot every ~3 seconds)
    except KeyboardInterrupt:
        print("\n🛑 [LAKEHOUSE PRODUCER] หยุดส่งข้อมูลแล้ว")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()

