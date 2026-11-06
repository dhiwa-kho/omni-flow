import json
import time

import clickhouse_connect
from confluent_kafka import Consumer

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "pgserver.public.orders"
GROUP_ID = "clickhouse_cdc_sink_v1"

CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_DB = "cdc_olap"


def init_clickhouse():
    ch = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)
    ch.command(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB}")

    # 1. ตารางจำลองสถานะล่าสุดของ PostgreSQL (รองรับทั้ง Insert, Update และ Delete ด้วย is_deleted)
    ch.command(
        f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.orders_replica (
            order_id UInt32,
            customer_name String,
            product_name String,
            category LowCardinality(String),
            amount Float64,
            order_status LowCardinality(String),
            cdc_op LowCardinality(String),
            version_ts UInt64,
            is_deleted UInt8,
            synced_at DateTime64(3) DEFAULT now64(3)
        ) ENGINE = ReplacingMergeTree(version_ts, is_deleted)
        ORDER BY (order_id)
        """
    )

    # 2. ตารางเก็บประวัติการเปลี่ยนแปลงทั้งหมด (CDC Audit Trail: ก่อนแก้ vs หลังแก้)
    ch.command(
        f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.orders_cdc_changelog (
            order_id UInt32,
            cdc_op LowCardinality(String),
            before_status LowCardinality(String),
            after_status LowCardinality(String),
            before_amount Float64,
            after_amount Float64,
            wal_ts_ms UInt64,
            logged_at DateTime64(3) DEFAULT now64(3)
        ) ENGINE = MergeTree()
        ORDER BY (wal_ts_ms, order_id)
        """
    )
    return ch


def main():
    ch = init_clickhouse()
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])

    print("=" * 76)
    print(f"🔄 [CDC SINK WORKER] กำลังดักฟังการเปลี่ยนแปลงจาก PostgreSQL WAL ผ่าน Kafka")
    print(f"📡 Source Topic  : {TOPIC}")
    print(f"🗄️  Target Tables : {CLICKHOUSE_DB}.orders_replica & {CLICKHOUSE_DB}.orders_cdc_changelog")
    print("💡 ลองเปิด DBeaver แก้ไขข้อมูลใน PostgreSQL (INSERT / UPDATE / DELETE) ได้เลย!")
    print("=" * 76)

    try:
        while True:
            msg = consumer.poll(0.2)
            if msg is None:
                continue
            if msg.error():
                continue
            if msg.value() is None:
                continue

            envelope = json.loads(msg.value().decode("utf-8"))
            payload = envelope.get("payload", envelope)
            if not payload or "op" not in payload:
                continue

            op = payload["op"]  # 'r' = snapshot, 'c' = insert, 'u' = update, 'd' = delete
            ts_ms = int(payload.get("ts_ms") or int(time.time() * 1000))
            before = payload.get("before") or {}
            after = payload.get("after") or {}

            if op in ("r", "c", "u"):
                order_id = int(after["order_id"])
                customer_name = str(after["customer_name"])
                product_name = str(after["product_name"])
                category = str(after["category"])
                amount = float(after["amount"])
                order_status = str(after["order_status"])
                is_deleted = 0
            elif op == "d":
                order_id = int(before["order_id"])
                customer_name = str(before.get("customer_name", ""))
                product_name = str(before.get("product_name", ""))
                category = str(before.get("category", ""))
                amount = float(before.get("amount", 0.0))
                order_status = "DELETED"
                is_deleted = 1
            else:
                continue

            # 1. Upsert/Delete into ClickHouse Replica Table
            ch.insert(
                f"{CLICKHOUSE_DB}.orders_replica",
                [
                    [
                        order_id,
                        customer_name,
                        product_name,
                        category,
                        amount,
                        order_status,
                        op,
                        ts_ms,
                        is_deleted,
                    ]
                ],
                column_names=[
                    "order_id",
                    "customer_name",
                    "product_name",
                    "category",
                    "amount",
                    "order_status",
                    "cdc_op",
                    "version_ts",
                    "is_deleted",
                ],
            )

            # 2. Append to CDC Audit Changelog Table
            ch.insert(
                f"{CLICKHOUSE_DB}.orders_cdc_changelog",
                [
                    [
                        order_id,
                        op,
                        str(before.get("order_status") or ""),
                        str(after.get("order_status") or ("DELETED" if op == "d" else "")),
                        float(before.get("amount") or 0.0),
                        float(after.get("amount") or 0.0),
                        ts_ms,
                    ]
                ],
                column_names=[
                    "order_id",
                    "cdc_op",
                    "before_status",
                    "after_status",
                    "before_amount",
                    "after_amount",
                    "wal_ts_ms",
                ],
            )

            consumer.commit(message=msg, asynchronous=False)

            op_label = {
                "r": "📸 SNAPSHOT",
                "c": "🟢 INSERT  ",
                "u": "🟡 UPDATE  ",
                "d": "🔴 DELETE  ",
            }.get(op, op)
            print(
                f"{op_label} | Order #{order_id} ({customer_name}) | Status: {before.get('order_status', '-')} -> {order_status} | Amount: {amount:,.2f} -> Synced to ClickHouse!"
            )

    except KeyboardInterrupt:
        print("\n🛑 [CDC SINK WORKER] หยุดการทำงานแล้ว")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()

