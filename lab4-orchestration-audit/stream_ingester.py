import json
import random
import time
from datetime import datetime, timezone

import clickhouse_connect
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "medallion_bronze_stream"
GROUP_ID = "bronze_ingester_group_v1"

CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_DB = "audit_olap"

PLANTS = ["Plant-Rayong", "Plant-Chonburi", "Plant-Saraburi"]
DEVICES = [f"BOILER-{i:02d}" for i in range(1, 9)]


def init_environment():
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    md = admin.list_topics(timeout=5.0)
    if TOPIC not in md.topics:
        fs = admin.create_topics([NewTopic(TOPIC, num_partitions=1, replication_factor=1)])
        for _, f in fs.items():
            try:
                f.result(timeout=5.0)
            except Exception:
                pass

    ch = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)
    ch.command(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB}")

    # 1. Bronze Raw Table (Unfiltered stream from Kafka)
    ch.command(
        f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.bronze_raw_events (
            seq_no UInt64,
            event_id String,
            event_time DateTime64(3),
            plant LowCardinality(String),
            device_id LowCardinality(String),
            temperature Float64,
            revenue_thb Float64,
            processed_by_dag UInt8 DEFAULT 0,
            ingested_at DateTime64(3) DEFAULT now64(3)
        ) ENGINE = ReplacingMergeTree(ingested_at)
        ORDER BY (seq_no, event_id)
        """
    )
    return ch


def main():
    ch = init_environment()
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "linger.ms": 5})
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])

    res = ch.query(f"SELECT max(seq_no) FROM {CLICKHOUSE_DB}.bronze_raw_events")
    seq_no = int(res.result_rows[0][0] or 0)

    print("=" * 76)
    print(f"🌊 [BRONZE STREAM INGESTER] Kafka ('{TOPIC}') -> ClickHouse (bronze_raw_events)")
    print("💡 ตัว Orchestrator DAG จะตื่นมาตรวจสอบ Data Quality และสร้างตาราง Gold ทุก 15 วินาที!")
    print("=" * 76)

    try:
        while True:
            seq_no += 1
            dev = random.choice(DEVICES)
            plant = PLANTS[seq_no % len(PLANTS)]
            # 8% chance of naturally occurring dirty/corrupt sensor data
            if random.random() < 0.08:
                temp = round(random.choice([999.9, -45.0]), 2)
                rev = round(random.uniform(-5000.0, -100.0), 2)
            else:
                temp = round(random.uniform(65.0, 118.0), 2)
                rev = round(random.uniform(250.0, 1800.0), 2)

            ev_id = f"BRZ-{seq_no:06d}"
            payload = {
                "seq_no": seq_no,
                "event_id": ev_id,
                "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                "plant": plant,
                "device_id": dev,
                "temperature": temp,
                "revenue_thb": rev,
            }
            producer.produce(TOPIC, key=dev, value=json.dumps(payload).encode("utf-8"))
            producer.poll(0)

            msg = consumer.poll(0.05)
            if msg is not None and not msg.error():
                data = json.loads(msg.value().decode("utf-8"))
                dt_obj = datetime.strptime(data["event_time"], "%Y-%m-%d %H:%M:%S.%f").replace(
                    tzinfo=timezone.utc
                )
                ch.insert(
                    f"{CLICKHOUSE_DB}.bronze_raw_events",
                    [
                        [
                            int(data["seq_no"]),
                            data["event_id"],
                            dt_obj,
                            data["plant"],
                            data["device_id"],
                            float(data["temperature"]),
                            float(data["revenue_thb"]),
                            0,
                        ]
                    ],
                    column_names=[
                        "seq_no",
                        "event_id",
                        "event_time",
                        "plant",
                        "device_id",
                        "temperature",
                        "revenue_thb",
                        "processed_by_dag",
                    ],
                )
                consumer.commit(message=msg, asynchronous=False)
                if int(data["seq_no"]) % 10 == 0:
                    print(
                        f"📥 [BRONZE INGEST] Seq #{data['seq_no']} ({data['event_id']}) | {data['plant']} | Temp: {data['temperature']}°C | ฿{data['revenue_thb']}"
                    )

            time.sleep(0.25)
    except KeyboardInterrupt:
        print("\n🛑 [BRONZE INGESTER] หยุดการทำงานแล้ว")
    finally:
        producer.flush()
        consumer.close()


if __name__ == "__main__":
    main()

