import json
import time
from datetime import datetime, timezone

import clickhouse_connect
from confluent_kafka import Consumer

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "lakehouse_raw_stream"
GROUP_ID = "lakehouse_tiering_worker_v1"

CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_DB = "lakehouse_db"

MINIO_S3_BASE = "http://minio:9000/omni-lakehouse/snapshots"
MINIO_USER = "minioadmin"
MINIO_PASS = "minioadmin123"


def init_lakehouse_tables():
    ch = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)
    ch.command(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB}")

    # 1. [HOT TIER] ตาราง ClickHouse เก็บข้อมูลสดสำหรับ Real-Time Operational Dashboard
    ch.command(
        f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.hot_events (
            snapshot_id UInt32,
            event_id String,
            seq_no UInt64,
            event_time DateTime64(3),
            region LowCardinality(String),
            device_id LowCardinality(String),
            category LowCardinality(String),
            voltage Float64,
            output_kw Float64,
            revenue_thb Float64,
            ingested_at DateTime64(3) DEFAULT now64(3)
        ) ENGINE = MergeTree()
        ORDER BY (snapshot_id, region, device_id, event_time)
        """
    )

    # 2. [LAKEHOUSE CATALOG / METADATA] ตารางเก็บประวัติ Snapshot ไฟล์ Parquet บน MinIO S3 (ทำ Time-Travel)
    ch.command(
        f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.lakehouse_snapshots (
            snapshot_id UInt32,
            s3_parquet_path String,
            row_count UInt32,
            min_seq_no UInt64,
            max_seq_no UInt64,
            total_output_kw Float64,
            total_revenue_thb Float64,
            committed_at DateTime64(3) DEFAULT now64(3)
        ) ENGINE = MergeTree()
        ORDER BY (snapshot_id)
        """
    )
    return ch


def main():
    ch = init_lakehouse_tables()
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])

    res = ch.query(f"SELECT max(snapshot_id) FROM {CLICKHOUSE_DB}.lakehouse_snapshots")
    snapshot_id = int(res.result_rows[0][0] or 0)

    print("=" * 78)
    print("🏛️  [LAKEHOUSE TIERING WORKER] Hot (ClickHouse) + Cold (MinIO S3 Parquet)")
    print(f"🌐 MinIO S3 Console : http://localhost:9001 (User: {MINIO_USER} / Pass: {MINIO_PASS})")
    print(f"📦 S3 Bucket Path   : s3://omni-lakehouse/snapshots/snap_XXXX.parquet")
    print("=" * 78)

    batch_rows = []
    last_msg = None

    try:
        while True:
            msg = consumer.poll(0.1)
            if msg is not None and not msg.error():
                data = json.loads(msg.value().decode("utf-8"))
                dt_obj = datetime.strptime(data["event_time"], "%Y-%m-%d %H:%M:%S.%f").replace(
                    tzinfo=timezone.utc
                )
                batch_rows.append(
                    {
                        "event_id": data["event_id"],
                        "seq_no": int(data["seq_no"]),
                        "event_time": dt_obj,
                        "region": data["region"],
                        "device_id": data["device_id"],
                        "category": data["category"],
                        "voltage": float(data["voltage"]),
                        "output_kw": float(data["output_kw"]),
                        "revenue_thb": float(data["revenue_thb"]),
                    }
                )
                last_msg = msg

            # ทุกๆ 20 รายการ: เขียนลง Hot ClickHouse + แพ็กเป็นไฟล์ .parquet ขึ้น MinIO S3 พร้อมเลข Snapshot!
            if len(batch_rows) >= 20:
                snapshot_id += 1
                parquet_filename = f"snap_{snapshot_id:04d}.parquet"
                s3_url = f"{MINIO_S3_BASE}/{parquet_filename}"

                hot_insert_rows = [
                    [
                        snapshot_id,
                        r["event_id"],
                        r["seq_no"],
                        r["event_time"],
                        r["region"],
                        r["device_id"],
                        r["category"],
                        r["voltage"],
                        r["output_kw"],
                        r["revenue_thb"],
                    ]
                    for r in batch_rows
                ]

                # 1. บันทึกลง Hot Tier (ClickHouse MergeTree)
                ch.insert(
                    f"{CLICKHOUSE_DB}.hot_events",
                    hot_insert_rows,
                    column_names=[
                        "snapshot_id",
                        "event_id",
                        "seq_no",
                        "event_time",
                        "region",
                        "device_id",
                        "category",
                        "voltage",
                        "output_kw",
                        "revenue_thb",
                    ],
                )

                # 2. ส่งออกเป็นไฟล์ Apache Parquet แท้ๆ ขึ้น MinIO S3 (Cold Data Lakehouse Tier)
                ch.command(
                    f"""
                    INSERT INTO FUNCTION s3(
                        '{s3_url}',
                        '{MINIO_USER}',
                        '{MINIO_PASS}',
                        'Parquet'
                    )
                    SELECT
                        snapshot_id,
                        event_id,
                        seq_no,
                        event_time,
                        region,
                        device_id,
                        category,
                        voltage,
                        output_kw,
                        revenue_thb
                    FROM {CLICKHOUSE_DB}.hot_events
                    WHERE snapshot_id = {snapshot_id}
                    SETTINGS s3_truncate_on_insert = 1
                    """
                )

                # 3. บันทึก Snapshot Catalog สำหรับทำ Time-Travel Query
                min_seq = min(r["seq_no"] for r in batch_rows)
                max_seq = max(r["seq_no"] for r in batch_rows)
                tot_kw = round(sum(r["output_kw"] for r in batch_rows), 3)
                tot_rev = round(sum(r["revenue_thb"] for r in batch_rows), 2)

                ch.insert(
                    f"{CLICKHOUSE_DB}.lakehouse_snapshots",
                    [[snapshot_id, s3_url, len(batch_rows), min_seq, max_seq, tot_kw, tot_rev]],
                    column_names=[
                        "snapshot_id",
                        "s3_parquet_path",
                        "row_count",
                        "min_seq_no",
                        "max_seq_no",
                        "total_output_kw",
                        "total_revenue_thb",
                    ],
                )

                consumer.commit(message=last_msg, asynchronous=False)
                print(
                    f"🧊 [SNAPSHOT #{snapshot_id:04d} -> S3 PARQUET] Saved '{parquet_filename}' ({len(batch_rows)} rows, Seq #{min_seq}..#{max_seq}, ฿{tot_rev:,.2f})"
                )
                batch_rows.clear()
                last_msg = None

    except KeyboardInterrupt:
        print("\n🛑 [LAKEHOUSE WORKER] หยุดการทำงานแล้ว")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()

