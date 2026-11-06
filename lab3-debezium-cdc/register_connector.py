import json
import time
import urllib.error
import urllib.request

DEBEZIUM_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "omni-postgres-orders-cdc"

connector_payload = {
    "name": CONNECTOR_NAME,
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "plugin.name": "pgoutput",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": "postgres",
        "database.password": "password123",
        "database.dbname": "shop_oltp",
        "topic.prefix": "pgserver",
        "table.include.list": "public.orders",
        "decimal.handling.mode": "double",
        "tombstones.on.delete": "false",
    },
}


def main():
    print("⏳ กำลังเช็กสถานะ Debezium Kafka Connect ที่ http://localhost:8083 ...")
    for attempt in range(1, 31):
        try:
            with urllib.request.urlopen(DEBEZIUM_URL, timeout=3) as resp:
                existing = json.loads(resp.read().decode("utf-8"))
                if CONNECTOR_NAME in existing:
                    print(f"✅ Connector '{CONNECTOR_NAME}' ถูกลงทะเบียนไว้เรียบร้อยแล้ว!")
                    return
                break
        except Exception:
            print(f"   รอ Debezium พร้อมใช้งาน... ({attempt}/30)")
            time.sleep(2.0)
    else:
        print("❌ ไม่สามารถเชื่อมต่อ Debezium ได้ กรุณาตรวจสอบว่า docker compose up -d รันเสร็จแล้ว")
        return

    req = urllib.request.Request(
        DEBEZIUM_URL,
        data=json.dumps(connector_payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read().decode("utf-8"))
            print("=" * 70)
            print(f"🚀 ลงทะเบียน Debezium CDC Connector สำเร็จ: {result['name']}")
            print("📡 ตอนนี้ทุกการ INSERT / UPDATE / DELETE ใน PostgreSQL (public.orders)")
            print("   จะถูกส่งเข้า Kafka Topic: 'pgserver.public.orders' อัตโนมัติ!")
            print("=" * 70)
    except urllib.error.HTTPError as e:
        print(f"❌ Error ({e.code}): {e.read().decode('utf-8')}")


if __name__ == "__main__":
    main()

