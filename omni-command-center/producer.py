import json
import random
import time
from datetime import datetime, timezone

import clickhouse_connect
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

KAFKA_BOOTSTRAP = "localhost:9092"
CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_DB = "omni_olap"
TOPIC = "omni_stream_kappa"
NUM_PARTITIONS = 3

ZONES = ["Zone-A (Assembly)", "Zone-B (Packaging)", "Zone-C (Cooling)"]
DEVICES = [f"SENSOR-{i:03d}" for i in range(1, 11)]


def init_environment():
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    md = admin.list_topics(timeout=5.0)
    if TOPIC not in md.topics:
        fs = admin.create_topics(
            [NewTopic(TOPIC, num_partitions=NUM_PARTITIONS, replication_factor=1)]
        )
        for _, f in fs.items():
            try:
                f.result(timeout=5.0)
            except Exception:
                pass

    ch = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)
    ch.command(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB}")
    ch.command(
        f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.source_audit_kappa (
            event_id String,
            seq_no UInt64,
            device_id LowCardinality(String),
            energy_kwh Float64,
            sent_at DateTime64(3) DEFAULT now64(3)
        ) ENGINE = ReplacingMergeTree(sent_at)
        ORDER BY (event_id)
        """
    )
    ch.command(
        f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.sensor_events_kappa (
            event_id String,
            seq_no UInt64,
            event_time DateTime64(3),
            zone LowCardinality(String),
            device_id LowCardinality(String),
            device_model LowCardinality(String),
            temperature Float64,
            energy_kwh Float64,
            cost_thb Float64,
            cep_status LowCardinality(String),
            kafka_partition UInt8,
            kafka_offset UInt64,
            ingested_at DateTime64(3) DEFAULT now64(3)
        ) ENGINE = ReplacingMergeTree(ingested_at)
        ORDER BY (event_id)
        """
    )
    ch.command(
        f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.windowed_5s_olap (
            window_start DateTime,
            window_end DateTime,
            device_id LowCardinality(String),
            device_model LowCardinality(String),
            event_count UInt32,
            min_temp Float64,
            max_temp Float64,
            total_energy_kwh Float64,
            total_cost_thb Float64,
            cep_alerts UInt32,
            flushed_at DateTime64(3) DEFAULT now64(3)
        ) ENGINE = ReplacingMergeTree(flushed_at)
        ORDER BY (window_start, device_id)
        """
    )
    ch.command(
        f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.scenario_control (
            id UInt8,
            active_scenario LowCardinality(String),
            drop_until Float64,
            updated_at DateTime64(3) DEFAULT now64(3)
        ) ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY (id)
        """
    )
    res = ch.query(
        f"SELECT active_scenario FROM {CLICKHOUSE_DB}.scenario_control FINAL WHERE id = 1"
    )
    if not res.result_rows:
        ch.insert(
            f"{CLICKHOUSE_DB}.scenario_control",
            [[1, "HAPPY_PATH", 0.0]],
            column_names=["id", "active_scenario", "drop_until"],
        )
    return ch


def get_active_scenario(ch) -> str:
    try:
        res = ch.query(
            f"SELECT active_scenario FROM {CLICKHOUSE_DB}.scenario_control FINAL WHERE id = 1"
        )
        if res.result_rows:
            return str(res.result_rows[0][0])
    except Exception:
        pass
    return "HAPPY_PATH"


def main():
    ch = init_environment()
    producer = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "linger.ms": 5,
            "batch.num.messages": 5000,
            "queue.buffering.max.messages": 200000,
            "enable.idempotence": True,
            "acks": "all",
        }
    )

    res = ch.query(f"SELECT max(seq_no) FROM {CLICKHOUSE_DB}.source_audit_kappa")
    seq_no = int(res.result_rows[0][0] or 0)

    print("=" * 76)
    print(f"🚀 [DDIA PRODUCER - KAPPA ARCHITECTURE] Connected to Kafka ({KAFKA_BOOTSTRAP})")
    print(f"📡 Topic: {TOPIC} (Partitions: P0, P1, P2) | Devices: SENSOR-001..010")
    print(f"🛡️  Durability: acks=all | enable.idempotence=True")
    print("=" * 76)

    batch_audit = []
    last_mode_check = 0.0
    current_mode = get_active_scenario(ch)
    last_flush_time = time.time()

    def on_delivery(err, msg):
        if err is not None:
            print(f"❌ Delivery failed: {err}")
        else:
            data = json.loads(msg.value().decode("utf-8"))
            batch_audit.append(
                [data["event_id"], int(data["seq_no"]), data["device_id"], float(data["energy_kwh"])]
            )

    try:
        while True:
            now = time.time()
            if now - last_mode_check >= 0.4:
                last_mode_check = now
                new_mode = get_active_scenario(ch)
                if new_mode != current_mode:
                    print(f"\n🔄 [SCENARIO SWITCH] เปลี่ยนโหมดเป็น: {new_mode}")
                    current_mode = new_mode
                    if current_mode == "PAUSED" and batch_audit:
                        producer.flush(timeout=2.0)
                        ch.insert(
                            f"{CLICKHOUSE_DB}.source_audit_kappa",
                            batch_audit,
                            column_names=["event_id", "seq_no", "device_id", "energy_kwh"],
                        )
                        batch_audit.clear()

            if current_mode == "PAUSED":
                time.sleep(0.1)
                continue

            burst_size = 150 if current_mode == "STRESS_TEST" else 1

            for _ in range(burst_size):
                seq_no += 1
                device_id = random.choice(DEVICES)
                zone = ZONES[int(device_id.split("-")[1]) % 3]
                temp = round(random.uniform(36.0, 78.0), 2)
                energy = round(random.uniform(0.15, 1.85), 4)
                event_id = f"EVT-{seq_no:07d}"

                payload = {
                    "event_id": event_id,
                    "seq_no": seq_no,
                    "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                    "zone": zone,
                    "device_id": device_id,
                    "temperature": temp,
                    "energy_kwh": energy,
                }
                raw_bytes = json.dumps(payload).encode("utf-8")

                if current_mode == "PARTITION_SKEW":
                    target_partition = 0 if random.random() < 0.90 else random.choice([1, 2])
                    part_key = f"HOT-KEY-{device_id}"
                    producer.produce(
                        TOPIC,
                        partition=target_partition,
                        key=part_key,
                        value=raw_bytes,
                        on_delivery=on_delivery,
                    )
                elif current_mode == "KEY_SALTING":
                    salt = seq_no % NUM_PARTITIONS
                    part_key = f"{device_id}#salt-{salt}"
                    producer.produce(
                        TOPIC,
                        partition=salt,
                        key=part_key,
                        value=raw_bytes,
                        on_delivery=on_delivery,
                    )
                else:
                    producer.produce(
                        TOPIC,
                        key=device_id,
                        value=raw_bytes,
                        on_delivery=on_delivery,
                    )

                if current_mode == "IDEMPOTENCY" and (seq_no % 5 == 0):
                    producer.produce(TOPIC, key=device_id, value=raw_bytes)
                    producer.produce(TOPIC, key=device_id, value=raw_bytes)
                    print(
                        f"🔁 [IDEMPOTENCY RETRY] Sent duplicate {event_id} ({device_id}) x2 -> Kafka"
                    )

            producer.poll(0)

            flush_threshold = 400 if current_mode == "STRESS_TEST" else 15
            if len(batch_audit) >= flush_threshold or (
                batch_audit and time.time() - last_flush_time >= 0.4
            ):
                producer.flush(timeout=2.0)
                if batch_audit:
                    ch.insert(
                        f"{CLICKHOUSE_DB}.source_audit_kappa",
                        batch_audit,
                        column_names=["event_id", "seq_no", "device_id", "energy_kwh"],
                    )
                    print(
                        f"📤 [{current_mode}] Produced up to #{seq_no} ({event_id}) | Batch: {len(batch_audit)}"
                    )
                    batch_audit.clear()
                    last_flush_time = time.time()

            if current_mode == "STRESS_TEST":
                time.sleep(0.08)
            else:
                time.sleep(0.025)

    except KeyboardInterrupt:
        print("\n🛑 [PRODUCER] Flushing final audit records...")
    finally:
        producer.flush(timeout=3.0)
        if batch_audit:
            ch.insert(
                f"{CLICKHOUSE_DB}.source_audit_kappa",
                batch_audit,
                column_names=["event_id", "seq_no", "device_id", "energy_kwh"],
            )
            batch_audit.clear()
        print("✅ [PRODUCER] Closed cleanly.")


if __name__ == "__main__":
    main()
