import json
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

KAFKA_BOOTSTRAP = "localhost:9092"
RAW_TOPIC = "raw_payments"
OUT_TOPIC = "flink_windowed_output"

MERCHANTS = [
    ("Apple Store IconSiam", "Electronics"),
    ("Starbucks Reserve", "Food & Beverage"),
    ("Agoda Luxury Booking", "Travel & Hotel"),
    ("Shopee Mall Official", "E-Commerce"),
    ("Binance P2P Gateway", "Crypto & Finance"),
]
COUNTRIES = ["TH", "SG", "JP", "US", "GB"]


def ensure_topics():
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    md = admin.list_topics(timeout=5.0)
    to_create = []
    for t in [RAW_TOPIC, OUT_TOPIC]:
        if t not in md.topics:
            to_create.append(NewTopic(t, num_partitions=1, replication_factor=1))
    if to_create:
        fs = admin.create_topics(to_create)
        for _, f in fs.items():
            try:
                f.result(timeout=5.0)
            except Exception:
                pass


def main():
    ensure_topics()
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "linger.ms": 5})

    print("=" * 74)
    print(f"💳 [PAYMENT PRODUCER] กำลังส่งธุรกรรมบัตรเครดิตเข้า Kafka Topic: '{RAW_TOPIC}'")
    print("💡 Apache Flink จะอ่าน Topic นี้ไปทำ 10-Second Tumbling Window กลางอากาศ!")
    print("=" * 74)

    seq_no = 0
    try:
        while True:
            seq_no += 1
            merchant, category = random.choice(MERCHANTS)
            country = random.choice(COUNTRIES)
            # 10% chance of High-Risk Transaction (>= 25,000 THB)
            if random.random() < 0.10:
                amount = round(random.uniform(25000.0, 85000.0), 2)
            else:
                amount = round(random.uniform(120.0, 9500.0), 2)

            tx_id = f"TX-{seq_no:06d}"
            event_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            payload = {
                "tx_id": tx_id,
                "event_ts": event_ts,
                "merchant": merchant,
                "category": category,
                "amount": amount,
                "country": country,
            }

            producer.produce(RAW_TOPIC, key=category, value=json.dumps(payload).encode("utf-8"))
            producer.poll(0)

            if seq_no % 10 == 0:
                producer.flush(timeout=1.0)
                risk_tag = "🚨 HIGH-RISK" if amount >= 25000 else "🟢 NORMAL"
                print(
                    f"📤 [PRODUCER] Sent #{seq_no} ({tx_id}) | {category} | ฿{amount:,.2f} [{risk_tag}]"
                )

            time.sleep(0.1)  # 10 tx/sec
    except KeyboardInterrupt:
        print("\n🛑 [PAYMENT PRODUCER] หยุดส่งข้อมูลแล้ว")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()

