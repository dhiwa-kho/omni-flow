import json
from datetime import datetime, timezone

import clickhouse_connect
from confluent_kafka import Consumer

KAFKA_BOOTSTRAP = "localhost:9092"
OUT_TOPIC = "flink_windowed_output"
GROUP_ID = "clickhouse_flink_sink_v1"

CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_DB = "flink_olap"


def init_clickhouse():
    ch = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)
    ch.command(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB}")
    ch.command(
        f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.flink_window_summary (
            window_start DateTime64(3),
            window_end DateTime64(3),
            category LowCardinality(String),
            tx_count UInt64,
            total_amount Float64,
            max_single_tx Float64,
            high_risk_count UInt64,
            ingested_at DateTime64(3) DEFAULT now64(3)
        ) ENGINE = ReplacingMergeTree(ingested_at)
        ORDER BY (window_start, category)
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
    consumer.subscribe([OUT_TOPIC])

    print("=" * 76)
    print(f"📥 [CLICKHOUSE FLINK SINK] กำลังรอรับผลลัพธ์ 10s Tumbling Window จาก Apache Flink")
    print(f"📡 Source Topic  : {OUT_TOPIC}")
    print(f"🗄️  Target Table  : {CLICKHOUSE_DB}.flink_window_summary")
    print("=" * 76)

    try:
        while True:
            msg = consumer.poll(0.2)
            if msg is None or msg.error() or msg.value() is None:
                continue

            data = json.loads(msg.value().decode("utf-8"))
            w_start = datetime.fromisoformat(data["window_start"]).replace(tzinfo=timezone.utc)
            w_end = datetime.fromisoformat(data["window_end"]).replace(tzinfo=timezone.utc)
            cat = str(data["category"])
            tx_cnt = int(data["tx_count"])
            tot_amt = float(data["total_amount"])
            max_tx = float(data["max_single_tx"])
            risk_cnt = int(data["high_risk_count"])

            ch.insert(
                f"{CLICKHOUSE_DB}.flink_window_summary",
                [[w_start, w_end, cat, tx_cnt, tot_amt, max_tx, risk_cnt]],
                column_names=[
                    "window_start",
                    "window_end",
                    "category",
                    "tx_count",
                    "total_amount",
                    "max_single_tx",
                    "high_risk_count",
                ],
            )
            consumer.commit(message=msg, asynchronous=False)
            print(
                f"⚡ [FLINK 10s WINDOW -> CLICKHOUSE] {data['window_start']}..{data['window_end'][-12:]} | {cat:16s} | Tx: {tx_cnt:3d} | Sum: ฿{tot_amt:10,.2f} | High-Risk: {risk_cnt}"
            )
    except KeyboardInterrupt:
        print("\n🛑 [CLICKHOUSE SINK] หยุดการทำงานแล้ว")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()

