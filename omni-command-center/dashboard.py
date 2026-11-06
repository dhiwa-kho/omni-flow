import ctypes
import threading
import time
from datetime import datetime
from pathlib import Path

import clickhouse_connect
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI(title="The Real-Time Stream & OLAP Command Center (Kappa + DDIA Edition)")

KAFKA_BOOTSTRAP = "localhost:9092"
CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_DB = "omni_olap"
TOPIC = "omni_stream_kappa"
GROUP_ID = "omni_kappa_group_v1"
NUM_PARTITIONS = 3


class MEMORYSTATUSEX(ctypes.Structure):
    _fields_ = [
        ("dwLength", ctypes.c_ulong),
        ("dwMemoryLoad", ctypes.c_ulong),
        ("ullTotalPhys", ctypes.c_ulonglong),
        ("ullAvailPhys", ctypes.c_ulonglong),
        ("ullTotalPageFile", ctypes.c_ulonglong),
        ("ullAvailPageFile", ctypes.c_ulonglong),
        ("ullTotalVirtual", ctypes.c_ulonglong),
        ("ullAvailVirtual", ctypes.c_ulonglong),
        ("sullAvailExtendedVirtual", ctypes.c_ulonglong),
    ]


class FILETIME(ctypes.Structure):
    _fields_ = [("dwLowDateTime", ctypes.c_ulong), ("dwHighDateTime", ctypes.c_ulong)]


_prev_idle = 0
_prev_kernel = 0
_prev_user = 0


def get_system_hardware_stats():
    global _prev_idle, _prev_kernel, _prev_user
    ram_percent = 0.0
    ram_used_gb = 0.0
    ram_total_gb = 0.0
    cpu_percent = 0.0
    try:
        mem = MEMORYSTATUSEX()
        mem.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
        ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(mem))
        ram_percent = float(mem.dwMemoryLoad)
        ram_total_gb = round(mem.ullTotalPhys / (1024**3), 1)
        ram_used_gb = round((mem.ullTotalPhys - mem.ullAvailPhys) / (1024**3), 1)

        idle, kernel, user = FILETIME(), FILETIME(), FILETIME()
        ctypes.windll.kernel32.GetSystemTimes(
            ctypes.byref(idle), ctypes.byref(kernel), ctypes.byref(user)
        )
        idle_val = (idle.dwHighDateTime << 32) | idle.dwLowDateTime
        kernel_val = (kernel.dwHighDateTime << 32) | kernel.dwLowDateTime
        user_val = (user.dwHighDateTime << 32) | user.dwLowDateTime

        if _prev_idle > 0:
            d_idle = idle_val - _prev_idle
            d_kernel = kernel_val - _prev_kernel
            d_user = user_val - _prev_user
            total_sys = d_kernel + d_user
            if total_sys > 0:
                cpu_percent = round(max(0.0, min(100.0, (total_sys - d_idle) * 100.0 / total_sys)), 1)

        _prev_idle, _prev_kernel, _prev_user = idle_val, kernel_val, user_val
    except Exception:
        pass
    return {
        "cpu_percent": cpu_percent,
        "ram_percent": ram_percent,
        "ram_used_gb": ram_used_gb,
        "ram_total_gb": ram_total_gb,
    }


lock = threading.Lock()
monitor_state = {
    "broker_online": False,
    "clickhouse_online": False,
    "active_scenario": "HAPPY_PATH",
    "producer_tps": 0.0,
    "consumer_tps": 0.0,
    "consumer_lag": 0,
    "last_committed_offset": 0,
    "high_watermark_offset": 0,
    "partitions": [
        {"id": 0, "leader": 1, "isr": 1, "committed": 0, "high": 0, "lag": 0, "pct": 33.3},
        {"id": 1, "leader": 1, "isr": 1, "committed": 0, "high": 0, "lag": 0, "pct": 33.3},
        {"id": 2, "leader": 1, "isr": 1, "committed": 0, "high": 0, "lag": 0, "pct": 33.4},
    ],
    "network_status": "HEALTHY",
    "gap_events_recovered": 0,
    "duplicates_filtered": 0,
    "gap_start_offset": 0,
    "in_gap": False,
    "hardware": {"cpu_percent": 0.0, "ram_percent": 0.0, "ram_used_gb": 0.0, "ram_total_gb": 0.0},
    "recent_gap_logs": [],
}


def add_gap_log(msg: str, level: str = "INFO"):
    now_str = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    with lock:
        monitor_state["recent_gap_logs"].insert(0, {"time": now_str, "level": level, "message": msg})
        monitor_state["recent_gap_logs"] = monitor_state["recent_gap_logs"][:20]


def ensure_tables():
    try:
        admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
        md = admin.list_topics(timeout=3.0)
        if TOPIC not in md.topics:
            fs = admin.create_topics(
                [NewTopic(TOPIC, num_partitions=NUM_PARTITIONS, replication_factor=1)]
            )
            for _, f in fs.items():
                try:
                    f.result(timeout=5.0)
                except Exception:
                    pass
    except Exception:
        pass

    try:
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
    except Exception:
        pass


def kafka_offset_inspector_worker():
    inspector = None
    admin = None
    prev_high = None
    prev_committed = None
    prev_time = time.time()

    while True:
        try:
            hw = get_system_hardware_stats()
            if inspector is None:
                inspector = Consumer(
                    {
                        "bootstrap.servers": KAFKA_BOOTSTRAP,
                        "group.id": GROUP_ID,
                        "enable.auto.commit": False,
                    }
                )
            if admin is None:
                admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})

            md = admin.list_topics(topic=TOPIC, timeout=0.4)
            topic_md = md.topics.get(TOPIC)

            tps = [TopicPartition(TOPIC, p) for p in range(NUM_PARTITIONS)]
            committed_list = inspector.committed(tps, timeout=0.4)

            total_high = 0
            total_committed = 0
            part_stats = []

            for p in range(NUM_PARTITIONS):
                _, high = inspector.get_watermark_offsets(tps[p], timeout=0.3, cached=False)
                comm = 0
                if committed_list and committed_list[p] and committed_list[p].offset > 0:
                    comm = committed_list[p].offset

                leader_id = 1
                isr_cnt = 1
                if topic_md and p in topic_md.partitions:
                    p_meta = topic_md.partitions[p]
                    leader_id = p_meta.leader
                    isr_cnt = len(p_meta.isrs)

                total_high += high
                total_committed += comm
                part_stats.append(
                    {
                        "id": p,
                        "leader": leader_id,
                        "isr": isr_cnt,
                        "committed": comm,
                        "high": high,
                        "lag": max(0, high - comm),
                        "pct": 0.0,
                    }
                )

            for ps in part_stats:
                ps["pct"] = round((ps["high"] * 100.0 / total_high), 1) if total_high > 0 else 33.3

            now = time.time()
            dt = max(0.001, now - prev_time)

            if prev_high is None:
                prev_high = total_high
                prev_committed = total_committed

            prod_tps = round(max(0, total_high - prev_high) / dt, 1)
            cons_tps = round(max(0, total_committed - prev_committed) / dt, 1)
            lag = max(0, total_high - total_committed)

            prev_high = total_high
            prev_committed = total_committed
            prev_time = now

            with lock:
                monitor_state["broker_online"] = True
                monitor_state["hardware"] = hw
                monitor_state["producer_tps"] = prod_tps
                monitor_state["consumer_tps"] = cons_tps
                monitor_state["high_watermark_offset"] = total_high
                monitor_state["last_committed_offset"] = total_committed
                monitor_state["consumer_lag"] = lag
                monitor_state["partitions"] = part_stats

                if lag >= 20 and not monitor_state["in_gap"]:
                    monitor_state["in_gap"] = True
                    monitor_state["gap_start_offset"] = total_committed
                    monitor_state["network_status"] = "NETWORK GAP / BACKLOG BUFFERING"
                    threading.Thread(
                        target=add_gap_log,
                        args=(
                            f"⚠️ [GAP MONITOR] Consumer Lag climbed to {lag} msgs across P0/P1/P2 (Committed #{total_committed})!",
                            "WARN",
                        ),
                        daemon=True,
                    ).start()
                elif monitor_state["in_gap"]:
                    if cons_tps > 0 and lag > 0:
                        monitor_state["network_status"] = "REPLAYING BACKLOG (OFFSET RECOVERY)"
                    if lag <= 5 and total_committed > monitor_state["gap_start_offset"]:
                        replayed = total_committed - monitor_state["gap_start_offset"]
                        monitor_state["in_gap"] = False
                        monitor_state["network_status"] = "HEALTHY"
                        monitor_state["gap_events_recovered"] += replayed
                        start_off = monitor_state["gap_start_offset"]
                        threading.Thread(
                            target=add_gap_log,
                            args=(
                                f"✅ [OFFSET RECOVERY] Replayed {replayed} events across P0/P1/P2 (#{start_off} → #{total_committed}) with 0 loss!",
                                "SUCCESS",
                            ),
                            daemon=True,
                        ).start()

        except Exception:
            with lock:
                monitor_state["broker_online"] = False
                monitor_state["producer_tps"] = 0.0
                monitor_state["consumer_tps"] = 0.0
            inspector = None
            admin = None

        time.sleep(0.5)


@app.on_event("startup")
def on_startup():
    ensure_tables()
    add_gap_log(
        "🧠 Kappa Stateful Stream Processor + DDIA Command Center Ready!",
        "INFO",
    )
    threading.Thread(target=kafka_offset_inspector_worker, daemon=True).start()


@app.post("/api/scenario/{mode}")
def set_scenario(mode: str):
    valid_modes = {
        "HAPPY_PATH",
        "IDEMPOTENCY",
        "STRESS_TEST",
        "PARTITION_SKEW",
        "KEY_SALTING",
        "PAUSED",
    }
    if mode not in valid_modes:
        return {"status": "invalid_mode"}
    try:
        ch = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)
        res = ch.query(f"SELECT drop_until FROM {CLICKHOUSE_DB}.scenario_control FINAL WHERE id = 1")
        drop_until = float(res.result_rows[0][0]) if res.result_rows else 0.0
        ch.insert(
            f"{CLICKHOUSE_DB}.scenario_control",
            [[1, mode, drop_until]],
            column_names=["id", "active_scenario", "drop_until"],
        )
        with lock:
            monitor_state["active_scenario"] = mode

        labels = {
            "HAPPY_PATH": "🟢 Scenario 1: Happy Path (Entity Key = device_id, Stateful CEP & 5s Windows)",
            "IDEMPOTENCY": "🔁 Scenario 3: Idempotency Testing (Duplicate Retries + ReplacingMergeTree FINAL)",
            "STRESS_TEST": "🔥 Scenario 4: High Load Stress Test (1,500+ TPS Burst across P0/P1/P2)",
            "PARTITION_SKEW": "⚖️ Scenario 5A [DDIA Ch.6]: Partition Skew (90% Hot-Spot routed to Partition 0!)",
            "KEY_SALTING": "🧂 Scenario 5B [DDIA Ch.6]: Key Salting Fix (device_id#salt-0..2 evenly balanced)",
            "PAUSED": "⏸️ Stream Paused for exact Batch Reconciliation check",
        }
        add_gap_log(f"🎛️ {labels.get(mode, mode)}", "INFO")
        return {"status": "ok", "active_scenario": mode}
    except Exception as e:
        return {"status": "error", "detail": str(e)}


@app.post("/api/simulate-drop")
def simulate_network_drop():
    try:
        ch = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)
        res = ch.query(
            f"SELECT active_scenario FROM {CLICKHOUSE_DB}.scenario_control FINAL WHERE id = 1"
        )
        mode = str(res.result_rows[0][0]) if res.result_rows else "HAPPY_PATH"
        drop_until = time.time() + 3.0
        ch.insert(
            f"{CLICKHOUSE_DB}.scenario_control",
            [[1, mode, drop_until]],
            column_names=["id", "active_scenario", "drop_until"],
        )
        add_gap_log(
            "⚡ [Scenario 2: Network Gap] Triggered 3.0s Stream Worker drop across P0/P1/P2!",
            "WARN",
        )
        return {"status": "triggered", "duration_sec": 3.0}
    except Exception as e:
        return {"status": "error", "detail": str(e)}


@app.post("/api/simulate-dup")
def simulate_duplicate_delivery():
    try:
        ch = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)
        res = ch.query(
            f"""
            SELECT event_id, seq_no, event_time, zone, device_id, device_model, temperature, energy_kwh, cost_thb, cep_status, kafka_partition, kafka_offset
            FROM {CLICKHOUSE_DB}.sensor_events_kappa FINAL
            ORDER BY seq_no DESC
            LIMIT 10
            """
        )
        if not res.result_rows:
            return {"status": "no_events_yet"}

        ch.insert(
            f"{CLICKHOUSE_DB}.sensor_events_kappa",
            res.result_rows,
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
        add_gap_log(
            f"🔁 [Idempotency] Injected {len(res.result_rows)} duplicate rows into ClickHouse. Deduplicated via FINAL!",
            "INFO",
        )
        return {"status": "injected", "count": len(res.result_rows)}
    except Exception as e:
        return {"status": "error", "detail": str(e)}


@app.get("/api/dashboard")
def get_dashboard_data():
    devices_olap = []
    ch_final_count = 0
    ch_raw_count = 0
    ch_final_energy = 0.0
    ch_final_cost_thb = 0.0
    total_cep_streaks = 0
    windows_flushed = 0
    src_count = 0
    src_energy = 0.0
    active_scenario = "HAPPY_PATH"
    latency_ms = {"p50": 0.0, "p95": 0.0, "p99": 0.0}

    try:
        ch = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)

        lat_res = ch.query(
            f"""
            SELECT
                round(quantile(0.50)(lat_ms), 1) AS p50,
                round(quantile(0.95)(lat_ms), 1) AS p95,
                round(quantile(0.99)(lat_ms), 1) AS p99
            FROM (
                SELECT greatest(1.0, toFloat64(dateDiff('millisecond', event_time, ingested_at))) AS lat_ms
                FROM {CLICKHOUSE_DB}.sensor_events_kappa
                ORDER BY seq_no DESC
                LIMIT 1000
            )
            """
        )
        if lat_res.result_rows and lat_res.result_rows[0][0] is not None:
            r0 = lat_res.result_rows[0]
            import math
            if not math.isnan(float(r0[0])):
                latency_ms = {
                    "p50": float(r0[0] or 0.0),
                    "p95": float(r0[1] or 0.0),
                    "p99": float(r0[2] or 0.0),
                }

        sc_res = ch.query(
            f"SELECT active_scenario FROM {CLICKHOUSE_DB}.scenario_control FINAL WHERE id = 1"
        )
        if sc_res.result_rows:
            active_scenario = str(sc_res.result_rows[0][0])

        src_res = ch.query(
            f"""
            SELECT count(), round(sum(energy_kwh), 4)
            FROM {CLICKHOUSE_DB}.source_audit_kappa FINAL
            """
        )
        if src_res.result_rows:
            src_count = int(src_res.result_rows[0][0] or 0)
            src_energy = float(src_res.result_rows[0][1] or 0.0)

        win_res = ch.query(f"SELECT count() FROM {CLICKHOUSE_DB}.windowed_5s_olap FINAL")
        if win_res.result_rows:
            windows_flushed = int(win_res.result_rows[0][0] or 0)

        dev_res = ch.query(
            f"""
            SELECT
                f.device_id,
                f.device_model,
                r.raw_cnt,
                f.final_cnt,
                (r.raw_cnt - f.final_cnt) AS dup_cnt,
                f.cep_streaks,
                f.total_energy,
                f.total_cost_thb
            FROM (
                SELECT
                    device_id,
                    any(device_model) AS device_model,
                    count() AS final_cnt,
                    countIf(cep_status = 'CRITICAL_STREAK') AS cep_streaks,
                    round(sum(energy_kwh), 4) AS total_energy,
                    round(sum(cost_thb), 2) AS total_cost_thb
                FROM {CLICKHOUSE_DB}.sensor_events_kappa FINAL
                GROUP BY device_id
            ) AS f
            LEFT JOIN (
                SELECT
                    device_id,
                    count() AS raw_cnt
                FROM {CLICKHOUSE_DB}.sensor_events_kappa
                GROUP BY device_id
            ) AS r USING (device_id)
            ORDER BY f.device_id
            """
        )
        for r in dev_res.result_rows:
            raw_c = int(r[2])
            fin_c = int(r[3])
            streaks = int(r[5])
            e_sum = float(r[6])
            c_thb = float(r[7])
            devices_olap.append(
                {
                    "device_id": r[0],
                    "zone": r[1],  # Displays enriched device_model
                    "raw_count": raw_c,
                    "final_count": fin_c,
                    "duplicates_removed": int(r[4]),
                    "cep_streaks": streaks,
                    "total_energy": e_sum,
                    "total_cost_thb": c_thb,
                }
            )
            ch_raw_count += raw_c
            ch_final_count += fin_c
            total_cep_streaks += streaks
            ch_final_energy = round(ch_final_energy + e_sum, 4)
            ch_final_cost_thb = round(ch_final_cost_thb + c_thb, 2)

        with lock:
            monitor_state["clickhouse_online"] = True
            monitor_state["active_scenario"] = active_scenario
            monitor_state["duplicates_filtered"] = max(0, ch_raw_count - ch_final_count)
    except Exception:
        with lock:
            monitor_state["clickhouse_online"] = False

    count_diff = src_count - ch_final_count
    energy_diff = round(src_energy - ch_final_energy, 4)

    with lock:
        return {
            "active_scenario": monitor_state["active_scenario"],
            "hardware": monitor_state["hardware"],
            "partitions": monitor_state["partitions"],
            "kappa_state": {
                "windows_5s_flushed": windows_flushed,
                "total_cep_streaks": total_cep_streaks,
                "total_cost_thb": ch_final_cost_thb,
            },
            "top_row": {
                "broker_online": monitor_state["broker_online"],
                "clickhouse_online": monitor_state["clickhouse_online"],
                "producer_tps": monitor_state["producer_tps"],
                "consumer_tps": monitor_state["consumer_tps"],
                "consumer_lag": monitor_state["consumer_lag"],
                "last_committed_offset": monitor_state["last_committed_offset"],
                "high_watermark_offset": monitor_state["high_watermark_offset"],
                "latency_ms": latency_ms,
            },
            "middle_section": {
                "network_status": monitor_state["network_status"],
                "gap_events_recovered": monitor_state["gap_events_recovered"],
                "duplicates_injected": monitor_state["duplicates_filtered"],
                "recent_gap_logs": list(monitor_state["recent_gap_logs"]),
            },
            "bottom_row": {
                "devices_olap": devices_olap,
                "reconciliation": {
                    "source_count": src_count,
                    "clickhouse_raw_count": ch_raw_count,
                    "clickhouse_final_count": ch_final_count,
                    "duplicates_removed": max(0, ch_raw_count - ch_final_count),
                    "count_diff": count_diff,
                    "source_energy": src_energy,
                    "clickhouse_final_energy": ch_final_energy,
                    "energy_diff": energy_diff,
                    "is_reconciled": (src_count > 0 and count_diff == 0 and abs(energy_diff) < 0.01),
                },
            },
        }


static_dir = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


@app.get("/")
def serve_index():
    return FileResponse(str(static_dir / "index.html"))
