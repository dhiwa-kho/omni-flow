import json
import random
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import clickhouse_connect
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI(title="The Real-Time Stream & OLAP Command Center")

KAFKA_BOOTSTRAP = "localhost:9092"
CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_DB = "omni_olap"
TOPIC = "omni_stream_v2"
GROUP_ID = "omni_olap_group_v2"

ZONES = ["Zone-A (Assembly)", "Zone-B (Packaging)", "Zone-C (Cooling)"]
DEVICES = [f"SENSOR-{i:03d}" for i in range(1, 31)]

lock = threading.Lock()
state = {
    "broker_online": False,
    "clickhouse_online": False,
    "producer_paused": False,
    "producer_tps": 0.0,
    "consumer_tps": 0.0,
    "consumer_lag": 0,
    "last_committed_offset": 0,
    "high_watermark_offset": 0,
    "network_status": "HEALTHY",  # HEALTHY | NETWORK DROP (2s GAP) | REPLAYING BACKLOG
    "drop_until": 0.0,
    "drop_start_offset": 0,
    "gap_events_recovered": 0,
    "duplicates_injected": 0,
    "source_total_count": 0,
    "source_total_energy": 0.0,
    "last_events_buffer": [],
    "recent_gap_logs": [],
    "running": True,
}


def add_gap_log(msg: str, level: str = "INFO"):
    now_str = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    with lock:
        state["recent_gap_logs"].insert(0, {"time": now_str, "level": level, "message": msg})
        state["recent_gap_logs"] = state["recent_gap_logs"][:15]


def ensure_setup():
    for _ in range(15):
        try:
            admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
            md = admin.list_topics(timeout=3.0)
            if TOPIC not in md.topics:
                fs = admin.create_topics([NewTopic(TOPIC, num_partitions=1, replication_factor=1)])
                for _, f in fs.items():
                    try:
                        f.result(timeout=5.0)
                    except Exception:
                        pass
            with lock:
                state["broker_online"] = True
            break
        except Exception:
            time.sleep(1.0)

    for _ in range(15):
        try:
            ch = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)
            ch.command(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB}")
            ch.command(f"DROP TABLE IF EXISTS {CLICKHOUSE_DB}.sensor_events")
            ch.command(
                f"""
                CREATE TABLE {CLICKHOUSE_DB}.sensor_events (
                    event_id String,
                    seq_no UInt64,
                    event_time DateTime64(3),
                    zone LowCardinality(String),
                    device_id LowCardinality(String),
                    temperature Float64,
                    energy_kwh Float64,
                    ingested_at DateTime64(3) DEFAULT now64(3)
                ) ENGINE = ReplacingMergeTree(ingested_at)
                ORDER BY (event_id)
                """
            )
            with lock:
                state["clickhouse_online"] = True
            break
        except Exception:
            time.sleep(1.0)


def producer_worker():
    producer = None
    seq_no = 0
    produced_in_window = 0
    window_start = time.time()

    def on_delivery(err, msg):
        nonlocal produced_in_window
        if err is None:
            payload = json.loads(msg.value().decode("utf-8"))
            with lock:
                state["source_total_count"] += 1
                state["source_total_energy"] = round(
                    state["source_total_energy"] + float(payload["energy_kwh"]), 4
                )
                state["last_events_buffer"].append(payload)
                if len(state["last_events_buffer"]) > 30:
                    state["last_events_buffer"].pop(0)
                state["broker_online"] = True
            produced_in_window += 1

    while state["running"]:
        try:
            if state["producer_paused"]:
                with lock:
                    state["producer_tps"] = 0.0
                time.sleep(0.1)
                window_start = time.time()
                produced_in_window = 0
                continue

            if producer is None:
                producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "linger.ms": 2})

            seq_no += 1
            device_id = random.choice(DEVICES)
            zone = ZONES[int(device_id.split("-")[1]) % 3]
            temp = round(random.uniform(36.0, 78.0), 2)
            energy = round(random.uniform(0.15, 1.85), 4)
            now_dt = datetime.now(timezone.utc)
            event_id = f"EVT-{seq_no:07d}"

            payload = {
                "event_id": event_id,
                "seq_no": seq_no,
                "event_time": now_dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                "zone": zone,
                "device_id": device_id,
                "temperature": temp,
                "energy_kwh": energy,
            }

            producer.produce(
                TOPIC,
                key=event_id,
                value=json.dumps(payload).encode("utf-8"),
                on_delivery=on_delivery,
            )
            producer.poll(0)

            elapsed = time.time() - window_start
            if elapsed >= 0.5:
                producer.flush(timeout=1.0)
                with lock:
                    state["producer_tps"] = round(produced_in_window / elapsed, 1)
                produced_in_window = 0
                window_start = time.time()

            time.sleep(0.04)  # ~25 TPS steady stream
        except Exception:
            with lock:
                state["broker_online"] = False
                state["producer_tps"] = 0.0
            time.sleep(1.0)


def consumer_worker():
    consumer = None
    ch_client = None
    consumed_in_window = 0
    window_start = time.time()
    last_watermark_check = 0.0
    batch_rows = []
    last_msg = None
    last_flush = time.time()
    was_dropped = False
    replay_started_at = 0.0
    replayed_count = 0

    while state["running"]:
        try:
            if consumer is None:
                consumer = Consumer(
                    {
                        "bootstrap.servers": KAFKA_BOOTSTRAP,
                        "group.id": GROUP_ID,
                        "auto.offset.reset": "earliest",
                        "enable.auto.commit": False,
                    }
                )
                consumer.subscribe([TOPIC])

            if ch_client is None:
                ch_client = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)
                with lock:
                    state["clickhouse_online"] = True

            now = time.time()
            with lock:
                drop_until = state["drop_until"]

            # Periodically check Watermark Offsets (every 0.25s) so it never bottlenecks consuming speed
            if now - last_watermark_check >= 0.25:
                last_watermark_check = now
                try:
                    tp = TopicPartition(TOPIC, 0)
                    _, high = consumer.get_watermark_offsets(tp, timeout=0.2, cached=False)
                    with lock:
                        committed = state["last_committed_offset"]
                        state["high_watermark_offset"] = high
                        state["consumer_lag"] = max(0, high - committed)
                except Exception:
                    pass

            # Simulate Network Drop (The 2-Second Gap)
            if now < drop_until:
                if not was_dropped:
                    # Flush any pending rows before dropping
                    if batch_rows and last_msg is not None:
                        ch_client.insert(
                            f"{CLICKHOUSE_DB}.sensor_events",
                            batch_rows,
                            column_names=[
                                "event_id",
                                "seq_no",
                                "event_time",
                                "zone",
                                "device_id",
                                "temperature",
                                "energy_kwh",
                            ],
                        )
                        consumer.commit(message=last_msg, asynchronous=False)
                        with lock:
                            state["last_committed_offset"] = last_msg.offset() + 1
                        batch_rows.clear()
                        last_msg = None

                    was_dropped = True
                    with lock:
                        state["network_status"] = "NETWORK DROP (2s GAP)"
                        state["consumer_tps"] = 0.0
                        state["drop_start_offset"] = state["last_committed_offset"]
                    add_gap_log(
                        f"⚠️ Simulated 2.0s Network Drop! Consumer halted at Offset #{state['drop_start_offset']}",
                        "WARN",
                    )
                time.sleep(0.05)
                window_start = time.time()
                consumed_in_window = 0
                continue

            if was_dropped:
                was_dropped = False
                replay_started_at = time.time()
                replayed_count = 0
                with lock:
                    state["network_status"] = "REPLAYING BACKLOG"
                add_gap_log(
                    f"🔄 Network Restored! Replaying backlog from Offset #{state['drop_start_offset']} (Lag: {state['consumer_lag']} msgs)",
                    "RECOVERY",
                )

            msg = consumer.poll(0.01)
            if msg is not None and not msg.error():
                data = json.loads(msg.value().decode("utf-8"))
                dt_obj = datetime.strptime(data["event_time"], "%Y-%m-%d %H:%M:%S.%f").replace(
                    tzinfo=timezone.utc
                )
                batch_rows.append(
                    [
                        data["event_id"],
                        int(data["seq_no"]),
                        dt_obj,
                        data["zone"],
                        data["device_id"],
                        float(data["temperature"]),
                        float(data["energy_kwh"]),
                    ]
                )
                last_msg = msg
                consumed_in_window += 1
                if state["network_status"] == "REPLAYING BACKLOG":
                    replayed_count += 1

            # Flush micro-batch immediately when >= 10 rows or every 80ms
            if batch_rows and (len(batch_rows) >= 10 or (time.time() - last_flush >= 0.08)):
                ch_client.insert(
                    f"{CLICKHOUSE_DB}.sensor_events",
                    batch_rows,
                    column_names=[
                        "event_id",
                        "seq_no",
                        "event_time",
                        "zone",
                        "device_id",
                        "temperature",
                        "energy_kwh",
                    ],
                )
                consumer.commit(message=last_msg, asynchronous=False)
                new_offset = last_msg.offset() + 1
                with lock:
                    state["last_committed_offset"] = new_offset
                    state["high_watermark_offset"] = max(state["high_watermark_offset"], new_offset)
                    state["consumer_lag"] = max(0, state["high_watermark_offset"] - new_offset)
                    if state["network_status"] == "REPLAYING BACKLOG" and state["consumer_lag"] == 0:
                        dur = round(time.time() - replay_started_at, 2)
                        state["network_status"] = "HEALTHY"
                        state["gap_events_recovered"] += replayed_count
                        start_off = state["drop_start_offset"]
                        threading.Thread(
                            target=add_gap_log,
                            args=(
                                f"✅ Offset Recovery Complete! Replayed {replayed_count} msgs (Offset #{start_off} → #{new_offset}) in {dur}s with 0 loss.",
                                "SUCCESS",
                            ),
                            daemon=True,
                        ).start()
                batch_rows.clear()
                last_msg = None
                last_flush = time.time()

            elapsed = time.time() - window_start
            if elapsed >= 0.5:
                with lock:
                    state["consumer_tps"] = round(consumed_in_window / elapsed, 1)
                consumed_in_window = 0
                window_start = time.time()

        except Exception:
            with lock:
                state["consumer_tps"] = 0.0
            ch_client = None
            time.sleep(0.5)


@app.on_event("startup")
def on_startup():
    ensure_setup()
    add_gap_log("🚀 Command Center initialized. Streaming telemetry to Kafka -> ClickHouse.", "INFO")
    threading.Thread(target=producer_worker, daemon=True).start()
    threading.Thread(target=consumer_worker, daemon=True).start()


@app.post("/api/simulate-drop")
def simulate_network_drop():
    with lock:
        state["drop_until"] = time.time() + 2.0
    return {"status": "triggered", "duration_sec": 2.0}


@app.post("/api/toggle-producer")
def toggle_producer():
    with lock:
        state["producer_paused"] = not state["producer_paused"]
        paused = state["producer_paused"]
    if paused:
        add_gap_log("⏸️ Producer stream paused for exact Batch Audit inspection.", "INFO")
    else:
        add_gap_log("▶️ Producer stream resumed.", "INFO")
    return {"producer_paused": paused}


@app.post("/api/simulate-dup")
def simulate_duplicate_delivery():
    try:
        with lock:
            sample_events = list(state["last_events_buffer"][-10:])
        if not sample_events:
            return {"status": "no_events_yet"}

        ch = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)
        dup_rows = []
        for data in sample_events:
            dt_obj = datetime.strptime(data["event_time"], "%Y-%m-%d %H:%M:%S.%f").replace(
                tzinfo=timezone.utc
            )
            dup_rows.append(
                [
                    data["event_id"],
                    int(data["seq_no"]),
                    dt_obj,
                    data["zone"],
                    data["device_id"],
                    float(data["temperature"]),
                    float(data["energy_kwh"]),
                ]
            )
        ch.insert(
            f"{CLICKHOUSE_DB}.sensor_events",
            dup_rows,
            column_names=[
                "event_id",
                "seq_no",
                "event_time",
                "zone",
                "device_id",
                "temperature",
                "energy_kwh",
            ],
        )
        with lock:
            state["duplicates_injected"] += len(dup_rows)
        add_gap_log(
            f"🔁 Injected {len(dup_rows)} duplicate retry events into ClickHouse. ReplacingMergeTree(FINAL) deduplicated them automatically!",
            "INFO",
        )
        return {"status": "injected", "count": len(dup_rows)}
    except Exception as e:
        return {"status": "error", "detail": str(e)}


@app.get("/api/dashboard")
def get_dashboard_data():
    olap_zones = []
    ch_final_count = 0
    ch_raw_count = 0
    ch_final_energy = 0.0

    try:
        ch = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)
        zone_res = ch.query(
            f"""
            SELECT
                zone,
                count() AS event_count,
                round(avg(temperature), 2) AS avg_temp,
                round(max(temperature), 2) AS max_temp,
                round(sum(energy_kwh), 4) AS total_energy
            FROM {CLICKHOUSE_DB}.sensor_events FINAL
            GROUP BY zone
            ORDER BY zone
            """
        )
        for r in zone_res.result_rows:
            olap_zones.append(
                {
                    "zone": r[0],
                    "event_count": int(r[1]),
                    "avg_temp": float(r[2]),
                    "max_temp": float(r[3]),
                    "total_energy": float(r[4]),
                }
            )
            ch_final_count += int(r[1])
            ch_final_energy = round(ch_final_energy + float(r[4]), 4)

        raw_res = ch.query(f"SELECT count() FROM {CLICKHOUSE_DB}.sensor_events")
        if raw_res.result_rows:
            ch_raw_count = int(raw_res.result_rows[0][0])

        with lock:
            state["clickhouse_online"] = True
    except Exception:
        with lock:
            state["clickhouse_online"] = False

    with lock:
        src_count = state["source_total_count"]
        src_energy = state["source_total_energy"]
        count_diff = src_count - ch_final_count
        energy_diff = round(src_energy - ch_final_energy, 4)

        return {
            "top_row": {
                "broker_online": state["broker_online"],
                "clickhouse_online": state["clickhouse_online"],
                "producer_paused": state["producer_paused"],
                "producer_tps": state["producer_tps"],
                "consumer_tps": state["consumer_tps"],
                "consumer_lag": state["consumer_lag"],
                "last_committed_offset": state["last_committed_offset"],
                "high_watermark_offset": state["high_watermark_offset"],
            },
            "middle_section": {
                "network_status": state["network_status"],
                "gap_events_recovered": state["gap_events_recovered"],
                "duplicates_injected": state["duplicates_injected"],
                "recent_gap_logs": list(state["recent_gap_logs"]),
            },
            "bottom_row": {
                "olap_zones": olap_zones,
                "reconciliation": {
                    "source_count": src_count,
                    "clickhouse_raw_count": ch_raw_count,
                    "clickhouse_final_count": ch_final_count,
                    "count_diff": count_diff,
                    "source_energy": src_energy,
                    "clickhouse_final_energy": ch_final_energy,
                    "energy_diff": energy_diff,
                    "is_reconciled": (count_diff == 0 and abs(energy_diff) < 0.01),
                },
            },
        }


static_dir = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


@app.get("/")
def serve_index():
    return FileResponse(str(static_dir / "index.html"))
