import random
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import clickhouse_connect
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI(title="Lab 4: Medallion DAG Orchestrator & Data Quality Gate")

CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_DB = "audit_olap"

lock = threading.Lock()
dag_state = {
    "current_step": "IDLE",  # IDLE | STEP_1 | STEP_2 | STEP_3 | STEP_4
    "last_run_id": 0,
    "next_run_in_sec": 15,
    "logs": [],
}


def add_log(msg: str, level: str = "INFO"):
    now_str = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    with lock:
        dag_state["logs"].insert(0, {"time": now_str, "level": level, "message": msg})
        dag_state["logs"] = dag_state["logs"][:20]


def ensure_tables():
    ch = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)
    ch.command(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB}")

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

    ch.command(
        f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.silver_clean_events (
            dag_run_id UInt32,
            seq_no UInt64,
            event_id String,
            event_time DateTime64(3),
            plant LowCardinality(String),
            device_id LowCardinality(String),
            temperature Float64,
            revenue_thb Float64,
            validated_at DateTime64(3) DEFAULT now64(3)
        ) ENGINE = ReplacingMergeTree(validated_at)
        ORDER BY (seq_no, event_id)
        """
    )

    ch.command(
        f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.quarantine_bad_rows (
            dag_run_id UInt32,
            seq_no UInt64,
            event_id String,
            plant LowCardinality(String),
            device_id LowCardinality(String),
            temperature Float64,
            revenue_thb Float64,
            dq_failure_reason String,
            quarantined_at DateTime64(3) DEFAULT now64(3)
        ) ENGINE = MergeTree()
        ORDER BY (dag_run_id, seq_no)
        """
    )

    ch.command(
        f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.gold_executive_mart (
            plant LowCardinality(String),
            verified_events UInt64,
            avg_temperature Float64,
            total_clean_revenue_thb Float64,
            last_dag_run_id UInt32,
            updated_at DateTime64(3) DEFAULT now64(3)
        ) ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY (plant)
        """
    )

    ch.command(
        f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.dag_run_history (
            run_id UInt32,
            trigger_type LowCardinality(String),
            bronze_rows_read UInt32,
            silver_clean_rows UInt32,
            quarantined_rows UInt32,
            reconciled_status LowCardinality(String),
            duration_ms UInt32,
            finished_at DateTime64(3) DEFAULT now64(3)
        ) ENGINE = MergeTree()
        ORDER BY (run_id)
        """
    )
    return ch


def execute_dag_pipeline(trigger_type: str = "SCHEDULED"):
    with lock:
        if dag_state["current_step"] != "IDLE":
            return {"status": "already_running"}
        dag_state["current_step"] = "STEP_1"

    t0 = time.time()
    try:
        ch = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)
        res = ch.query(f"SELECT max(run_id) FROM {CLICKHOUSE_DB}.dag_run_history")
        run_id = int(res.result_rows[0][0] or 0) + 1
        with lock:
            dag_state["last_run_id"] = run_id

        # =====================================================================
        # STEP 1: EXTRACT UNPROCESSED BRONZE BATCH
        # =====================================================================
        add_log(f"▶️ [DAG Run #{run_id} - STEP 1] Extracting unprocessed rows from Bronze table...", "INFO")
        time.sleep(0.4)

        max_silver_res = ch.query(
            f"""
            SELECT greatest(
                (SELECT max(seq_no) FROM {CLICKHOUSE_DB}.silver_clean_events),
                (SELECT max(seq_no) FROM {CLICKHOUSE_DB}.quarantine_bad_rows)
            )
            """
        )
        last_seq = int(max_silver_res.result_rows[0][0] or 0)

        bronze_res = ch.query(
            f"""
            SELECT seq_no, event_id, event_time, plant, device_id, temperature, revenue_thb
            FROM {CLICKHOUSE_DB}.bronze_raw_events FINAL
            WHERE seq_no > {last_seq}
            ORDER BY seq_no ASC
            """
        )
        rows = bronze_res.result_rows
        bronze_count = len(rows)

        # =====================================================================
        # STEP 2: DATA QUALITY GATE (FILTER & QUARANTINE CORRUPT DATA)
        # =====================================================================
        with lock:
            dag_state["current_step"] = "STEP_2"
        add_log(
            f"🔍 [DAG Run #{run_id} - STEP 2] Running Data Quality Gate on {bronze_count} Bronze rows...",
            "INFO",
        )
        time.sleep(0.4)

        silver_rows = []
        quarantine_rows = []

        for r in rows:
            seq_no, ev_id, ev_time, plant, dev_id, temp, rev = r
            reasons = []
            if float(temp) < 0.0 or float(temp) > 200.0:
                reasons.append(f"TEMP_OUT_OF_RANGE ({temp}C)")
            if float(rev) < 0.0:
                reasons.append(f"NEGATIVE_REVENUE ({rev} THB)")

            if reasons:
                quarantine_rows.append(
                    [
                        run_id,
                        int(seq_no),
                        str(ev_id),
                        str(plant),
                        str(dev_id),
                        float(temp),
                        float(rev),
                        " | ".join(reasons),
                    ]
                )
            else:
                silver_rows.append(
                    [
                        run_id,
                        int(seq_no),
                        str(ev_id),
                        ev_time,
                        str(plant),
                        str(dev_id),
                        float(temp),
                        float(rev),
                    ]
                )

        if silver_rows:
            ch.insert(
                f"{CLICKHOUSE_DB}.silver_clean_events",
                silver_rows,
                column_names=[
                    "dag_run_id",
                    "seq_no",
                    "event_id",
                    "event_time",
                    "plant",
                    "device_id",
                    "temperature",
                    "revenue_thb",
                ],
            )
        if quarantine_rows:
            ch.insert(
                f"{CLICKHOUSE_DB}.quarantine_bad_rows",
                quarantine_rows,
                column_names=[
                    "dag_run_id",
                    "seq_no",
                    "event_id",
                    "plant",
                    "device_id",
                    "temperature",
                    "revenue_thb",
                    "dq_failure_reason",
                ],
            )
            add_log(
                f"🚨 [DQ GATE ALERT] Quarantined {len(quarantine_rows)} corrupt/poison rows into quarantine_bad_rows!",
                "WARN",
            )

        # =====================================================================
        # STEP 3: BATCH RECONCILIATION AUDIT (COUNT + KEY-LEVEL LEFT JOIN CHECK)
        # =====================================================================
        with lock:
            dag_state["current_step"] = "STEP_3"
        time.sleep(0.35)

        # DDIA Chapter 12: Key-level LEFT JOIN Reconciliation (Missing / Extra / Mismatched)
        key_rec_res = ch.query(
            f"""
            SELECT
                countIf(s.event_id = '' AND q.event_id = '') AS missing_in_downstream,
                countIf(s.event_id != '' AND abs(b.revenue_thb - s.revenue_thb) > 0.01) AS mismatched_amount
            FROM (
                SELECT event_id, revenue_thb
                FROM {CLICKHOUSE_DB}.bronze_raw_events FINAL
                WHERE seq_no > {last_seq}
            ) AS b
            LEFT JOIN (
                SELECT event_id, revenue_thb
                FROM {CLICKHOUSE_DB}.silver_clean_events FINAL
                WHERE dag_run_id = {run_id}
            ) AS s ON b.event_id = s.event_id
            LEFT JOIN (
                SELECT event_id
                FROM {CLICKHOUSE_DB}.quarantine_bad_rows
                WHERE dag_run_id = {run_id}
            ) AS q ON b.event_id = q.event_id
            """
        )
        missing_keys = int(key_rec_res.result_rows[0][0] or 0) if key_rec_res.result_rows else 0
        mismatched_keys = int(key_rec_res.result_rows[0][1] or 0) if key_rec_res.result_rows else 0

        is_reconciled = (
            bronze_count == (len(silver_rows) + len(quarantine_rows))
            and missing_keys == 0
            and mismatched_keys == 0
        )
        rec_status = "RECONCILED_OK" if is_reconciled else f"DISCREPANCY(miss={missing_keys},diff={mismatched_keys})"
        add_log(
            f"⚖️ [DAG Run #{run_id} - STEP 3] Key-Level LEFT JOIN Audit: Bronze({bronze_count}) == Silver({len(silver_rows)}) + Quarantine({len(quarantine_rows)}) | Missing={missing_keys}, Mismatch={mismatched_keys} [{rec_status}]",
            "INFO",
        )

        # =====================================================================
        # STEP 4: PUBLISH GOLD EXECUTIVE MART TABLE
        # =====================================================================
        with lock:
            dag_state["current_step"] = "STEP_4"
        time.sleep(0.35)

        ch.command(
            f"""
            INSERT INTO {CLICKHOUSE_DB}.gold_executive_mart
            SELECT
                plant,
                count() AS verified_events,
                round(avg(temperature), 2) AS avg_temperature,
                round(sum(revenue_thb), 2) AS total_clean_revenue_thb,
                {run_id} AS last_dag_run_id,
                now64(3) AS updated_at
            FROM {CLICKHOUSE_DB}.silver_clean_events FINAL
            GROUP BY plant
            """
        )

        dur_ms = int((time.time() - t0) * 1000)
        ch.insert(
            f"{CLICKHOUSE_DB}.dag_run_history",
            [
                [
                    run_id,
                    trigger_type,
                    bronze_count,
                    len(silver_rows),
                    len(quarantine_rows),
                    rec_status,
                    dur_ms,
                ]
            ],
            column_names=[
                "run_id",
                "trigger_type",
                "bronze_rows_read",
                "silver_clean_rows",
                "quarantined_rows",
                "reconciled_status",
                "duration_ms",
            ],
        )

        add_log(
            f"✅ [DAG Run #{run_id} COMPLETE] Published Gold Executive Mart in {dur_ms}ms!",
            "SUCCESS",
        )
        return {"status": "completed", "run_id": run_id}
    except Exception as e:
        add_log(f"❌ [DAG ERROR] {e}", "WARN")
        return {"status": "error", "detail": str(e)}
    finally:
        with lock:
            dag_state["current_step"] = "IDLE"


def scheduler_loop():
    countdown = 12
    while True:
        time.sleep(1.0)
        with lock:
            if dag_state["current_step"] != "IDLE":
                continue
            countdown -= 1
            dag_state["next_run_in_sec"] = max(0, countdown)
        if countdown <= 0:
            execute_dag_pipeline("SCHEDULED_15S")
            countdown = 15


@app.on_event("startup")
def on_startup():
    ensure_tables()
    add_log("🎛️ Medallion DAG Orchestrator & Data Quality Gate initialized.", "INFO")
    threading.Thread(target=scheduler_loop, daemon=True).start()


@app.post("/api/trigger-dag")
def manual_trigger_dag():
    threading.Thread(target=execute_dag_pipeline, args=("MANUAL_TRIGGER",), daemon=True).start()
    return {"status": "triggered"}


@app.post("/api/inject-poison")
def inject_poison_data():
    try:
        ch = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)
        res = ch.query(f"SELECT max(seq_no) FROM {CLICKHOUSE_DB}.bronze_raw_events")
        base_seq = int(res.result_rows[0][0] or 0) + 100000
        now_dt = datetime.now(timezone.utc)
        poison_rows = [
            [
                base_seq + 1,
                f"POISON-{base_seq + 1}",
                now_dt,
                "Plant-Rayong",
                "BOILER-01",
                999.9,
                -8500.00,
                0,
            ],
            [
                base_seq + 2,
                f"POISON-{base_seq + 2}",
                now_dt,
                "Plant-Chonburi",
                "BOILER-02",
                -99.0,
                -4200.00,
            0,
            ],
            [
                base_seq + 3,
                f"POISON-{base_seq + 3}",
                now_dt,
                "Plant-Saraburi",
                "BOILER-03",
                888.8,
                -12000.00,
                0,
            ],
        ]
        ch.insert(
            f"{CLICKHOUSE_DB}.bronze_raw_events",
            poison_rows,
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
        add_log(
            "🧪 Injected 3 POISON / CORRUPT rows (Temp=999C, Revenue=-12,000 THB) into Bronze table! Trigger DAG to see Step 2 quarantine them.",
            "WARN",
        )
        return {"status": "injected", "count": 3}
    except Exception as e:
        return {"status": "error", "detail": str(e)}


@app.get("/api/state")
def get_orchestrator_state():
    gold_mart = []
    quarantine_list = []
    dag_runs = []
    counts = {"bronze": 0, "silver": 0, "quarantine": 0}

    try:
        ch = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)
        counts["bronze"] = int(
            ch.query(f"SELECT count() FROM {CLICKHOUSE_DB}.bronze_raw_events FINAL").result_rows[0][0]
        )
        counts["silver"] = int(
            ch.query(f"SELECT count() FROM {CLICKHOUSE_DB}.silver_clean_events FINAL").result_rows[0][0]
        )
        counts["quarantine"] = int(
            ch.query(f"SELECT count() FROM {CLICKHOUSE_DB}.quarantine_bad_rows").result_rows[0][0]
        )

        g_res = ch.query(
            f"""
            SELECT plant, verified_events, avg_temperature, total_clean_revenue_thb, last_dag_run_id
            FROM {CLICKHOUSE_DB}.gold_executive_mart FINAL
            ORDER BY plant
            """
        )
        for r in g_res.result_rows:
            gold_mart.append(
                {
                    "plant": r[0],
                    "verified_events": int(r[1]),
                    "avg_temp": float(r[2]),
                    "total_revenue": float(r[3]),
                    "last_run_id": int(r[4]),
                }
            )

        q_res = ch.query(
            f"""
            SELECT dag_run_id, event_id, plant, device_id, temperature, revenue_thb, dq_failure_reason
            FROM {CLICKHOUSE_DB}.quarantine_bad_rows
            ORDER BY quarantined_at DESC
            LIMIT 8
            """
        )
        for r in q_res.result_rows:
            quarantine_list.append(
                {
                    "run_id": int(r[0]),
                    "event_id": r[1],
                    "plant": r[2],
                    "device_id": r[3],
                    "temp": float(r[4]),
                    "rev": float(r[5]),
                    "reason": r[6],
                }
            )

        r_res = ch.query(
            f"""
            SELECT run_id, trigger_type, bronze_rows_read, silver_clean_rows, quarantined_rows, reconciled_status, duration_ms
            FROM {CLICKHOUSE_DB}.dag_run_history
            ORDER BY run_id DESC
            LIMIT 6
            """
        )
        for r in r_res.result_rows:
            dag_runs.append(
                {
                    "run_id": int(r[0]),
                    "trigger": r[1],
                    "bronze": int(r[2]),
                    "silver": int(r[3]),
                    "quarantine": int(r[4]),
                    "status": r[5],
                    "duration_ms": int(r[6]),
                }
            )
    except Exception:
        pass

    with lock:
        return {
            "current_step": dag_state["current_step"],
            "last_run_id": dag_state["last_run_id"],
            "next_run_in_sec": dag_state["next_run_in_sec"],
            "counts": counts,
            "gold_mart": gold_mart,
            "quarantine_list": quarantine_list,
            "dag_runs": dag_runs,
            "logs": list(dag_state["logs"]),
        }


static_dir = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


@app.get("/")
def serve_index():
    return FileResponse(str(static_dir / "index.html"))

