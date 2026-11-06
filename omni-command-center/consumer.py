import json
import time
from datetime import datetime, timezone

import clickhouse_connect
from confluent_kafka import Consumer

KAFKA_BOOTSTRAP = "localhost:9092"
CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_DB = "omni_olap"
TOPIC = "omni_stream_kappa"
GROUP_ID = "omni_kappa_group_v1"

# [DDIA Ch.11: Stream-Table Join Master Lookup Table]
DEVICE_SPEC_TABLE = {
    f"SENSOR-{i:03d}": {
        "model": "Turbine-X1" if i <= 5 else "Compressor-Z9",
        "max_safe_temp": 68.0 if i <= 5 else 72.0,
        "tariff_thb": 4.20,
    }
    for i in range(1, 11)
}


def init_tables():
    ch = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)
    ch.command(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB}")
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
    return ch


def get_drop_until(ch) -> float:
    try:
        res = ch.query(f"SELECT drop_until FROM {CLICKHOUSE_DB}.scenario_control FINAL WHERE id = 1")
        if res.result_rows:
            return float(res.result_rows[0][0])
    except Exception:
        pass
    return 0.0


def main():
    ch = init_tables()
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )

    def on_assign(cons, partitions):
        p_list = ", ".join([f"P{p.partition}" for p in partitions])
        print(f"⚖️  [DDIA REBALANCE - ASSIGNED] Stream Worker received partitions: [{p_list}]")

    def on_revoke(cons, partitions):
        p_list = ", ".join([f"P{p.partition}" for p in partitions])
        print(f"🔄 [DDIA REBALANCE - REVOKED] Releasing partitions for rebalance: [{p_list}]")

    consumer.subscribe([TOPIC], on_assign=on_assign, on_revoke=on_revoke)

    print("=" * 78)
    print(f"🧠 [STATEFUL STREAM PROCESSOR - KAPPA ARCHITECTURE] Subscribed to '{TOPIC}'")
    print("   ├─ 1. Stream-Table Join : Enriching Device Model & Electricity Tariff (THB)")
    print("   ├─ 2. Stateful CEP      : Tracking consecutive Overheat Streaks per Device")
    print("   └─ 3. Tumbling Window   : 5-Second Stateful Window Aggregation -> ClickHouse")
    print("=" * 78)

    # In-Memory State Stores (Stateful Stream Processing)
    device_cep_streak = {}  # {device_id: consecutive_high_temp_count}
    seen_event_ids_window = set()  # Deduplicate within active tumbling window
    window_state = {}  # {device_id: {count, min_temp, max_temp, energy, cost, alerts}}
    window_start_epoch = int(time.time() // 5) * 5

    batch_rows = []
    last_flush = time.time()
    last_control_check = 0.0
    drop_until = 0.0
    was_paused = False

    def flush_tumbling_window(w_start_epoch: int):
        if not window_state:
            return
        w_start_dt = datetime.fromtimestamp(w_start_epoch, tz=timezone.utc)
        w_end_dt = datetime.fromtimestamp(w_start_epoch + 5, tz=timezone.utc)
        win_rows = []
        for dev_id, st in window_state.items():
            win_rows.append(
                [
                    w_start_dt,
                    w_end_dt,
                    dev_id,
                    st["model"],
                    st["count"],
                    round(st["min_temp"], 2),
                    round(st["max_temp"], 2),
                    round(st["energy"], 4),
                    round(st["cost"], 4),
                    st["alerts"],
                ]
            )
        ch.insert(
            f"{CLICKHOUSE_DB}.windowed_5s_olap",
            win_rows,
            column_names=[
                "window_start",
                "window_end",
                "device_id",
                "device_model",
                "event_count",
                "min_temp",
                "max_temp",
                "total_energy_kwh",
                "total_cost_thb",
                "cep_alerts",
            ],
        )
        print(
            f"⏱️  [5s TUMBLING WINDOW FLUSHED] Window {w_start_dt.strftime('%H:%M:%S')}..{w_end_dt.strftime('%H:%M:%S')} | Aggregated {len(win_rows)} devices -> windowed_5s_olap"
        )
        window_state.clear()
        seen_event_ids_window.clear()

    try:
        while True:
            now = time.time()
            if now - last_control_check >= 0.25:
                last_control_check = now
                drop_until = get_drop_until(ch)

            if now < drop_until:
                if not was_paused:
                    if batch_rows:
                        ch.insert(
                            f"{CLICKHOUSE_DB}.sensor_events_kappa",
                            batch_rows,
                            column_names=[
                                "event_id",
                                "seq_no",
                                "event_time",
                                "zone",
                                "device_id",
                                "device_model",
                                "temperature",
                                "energy_kwh",
                                "cost_thb",
                                "cep_status",
                                "kafka_partition",
                                "kafka_offset",
                            ],
                        )
                        consumer.commit(asynchronous=False)
                        batch_rows.clear()
                    was_paused = True
                    rem = round(drop_until - now, 1)
                    print(f"⚠️  [SCENARIO 2: NETWORK GAP] Stream Worker paused ({rem}s)...")
                time.sleep(0.05)
                continue

            if was_paused:
                was_paused = False
                print("🔄 [SCENARIO 2: OFFSET RECOVERY] Resuming Stateful Stream Worker...")

            # Check if 5-Second Tumbling Window boundary crossed
            current_bucket = int(now // 5) * 5
            if current_bucket > window_start_epoch:
                flush_tumbling_window(window_start_epoch)
                window_start_epoch = current_bucket

            msgs = consumer.consume(num_messages=400, timeout=0.04)
            for msg in msgs:
                if msg is None or msg.error():
                    continue
                data = json.loads(msg.value().decode("utf-8"))
                ev_id = data["event_id"]
                dev_id = data["device_id"]
                temp = float(data["temperature"])
                energy = float(data["energy_kwh"])
                dt_obj = datetime.strptime(data["event_time"], "%Y-%m-%d %H:%M:%S.%f").replace(
                    tzinfo=timezone.utc
                )

                # 1. [Stream-Table Enrichment Join]
                spec = DEVICE_SPEC_TABLE.get(
                    dev_id, {"model": "Generic-IoT", "max_safe_temp": 70.0, "tariff_thb": 4.20}
                )
                model_name = spec["model"]
                cost_thb = round(energy * spec["tariff_thb"], 4)

                # 2. [Stateful CEP: Consecutive Overheat Detection]
                is_first_seen = ev_id not in seen_event_ids_window
                if is_first_seen:
                    seen_event_ids_window.add(ev_id)
                    if temp > spec["max_safe_temp"]:
                        device_cep_streak[dev_id] = device_cep_streak.get(dev_id, 0) + 1
                    else:
                        device_cep_streak[dev_id] = 0

                streak = device_cep_streak.get(dev_id, 0)
                if streak >= 2:
                    cep_status = "CRITICAL_STREAK"
                elif streak == 1:
                    cep_status = "WARN_HIGH"
                else:
                    cep_status = "NORMAL"

                # 3. [Update 5-Second Tumbling Window State in Memory]
                if is_first_seen:
                    if dev_id not in window_state:
                        window_state[dev_id] = {
                            "model": model_name,
                            "count": 0,
                            "min_temp": temp,
                            "max_temp": temp,
                            "energy": 0.0,
                            "cost": 0.0,
                            "alerts": 0,
                        }
                    st = window_state[dev_id]
                    st["count"] += 1
                    st["min_temp"] = min(st["min_temp"], temp)
                    st["max_temp"] = max(st["max_temp"], temp)
                    st["energy"] += energy
                    st["cost"] += cost_thb
                    if cep_status == "CRITICAL_STREAK":
                        st["alerts"] += 1

                batch_rows.append(
                    [
                        ev_id,
                        int(data["seq_no"]),
                        dt_obj,
                        data["zone"],
                        dev_id,
                        model_name,
                        temp,
                        energy,
                        cost_thb,
                        cep_status,
                        int(msg.partition()),
                        int(msg.offset()),
                    ]
                )

            if batch_rows and (len(batch_rows) >= 200 or (time.time() - last_flush >= 0.12)):
                ch.insert(
                    f"{CLICKHOUSE_DB}.sensor_events_kappa",
                    batch_rows,
                    column_names=[
                        "event_id",
                        "seq_no",
                        "event_time",
                        "zone",
                        "device_id",
                        "device_model",
                        "temperature",
                        "energy_kwh",
                        "cost_thb",
                        "cep_status",
                        "kafka_partition",
                        "kafka_offset",
                    ],
                )
                consumer.commit(asynchronous=False)
                print(
                    f"✅ [STREAM WORKER] Enriched & Ingested {len(batch_rows):4d} rows -> ClickHouse"
                )
                batch_rows.clear()
                last_flush = time.time()

    except KeyboardInterrupt:
        print("\n🛑 [STREAM WORKER] Flushing final state & tumbling window...")
    finally:
        try:
            flush_tumbling_window(window_start_epoch)
            if batch_rows:
                ch.insert(
                    f"{CLICKHOUSE_DB}.sensor_events_kappa",
                    batch_rows,
                    column_names=[
                        "event_id",
                        "seq_no",
                        "event_time",
                        "zone",
                        "device_id",
                        "device_model",
                        "temperature",
                        "energy_kwh",
                        "cost_thb",
                        "cep_status",
                        "kafka_partition",
                        "kafka_offset",
                    ],
                )
                consumer.commit(asynchronous=False)
        except Exception:
            pass
        consumer.close()
        print("✅ [STREAM WORKER] Disconnected cleanly.")


if __name__ == "__main__":
    main()
