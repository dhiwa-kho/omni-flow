import random
import subprocess
import time

CUSTOMERS = [
    "Somchai K.",
    "Suda W.",
    "Ananda P.",
    "Nattapong T.",
    "Kanya S.",
    "Wichai R.",
    "Ploy M.",
    "Chaiwat B.",
]
PRODUCTS = [
    ("MacBook Pro M3", "Electronics", 59900.00),
    ("iPhone 16 Pro", "Electronics", 41900.00),
    ("Mechanical Keyboard", "Accessories", 3490.00),
    ("4K Monitor 27-inch", "Electronics", 12500.00),
    ("Ergonomic Chair", "Furniture", 8900.00),
    ("Standing Desk", "Furniture", 14500.00),
    ("Wireless Mouse Pro", "Accessories", 2590.00),
]


def run_pg_sql(sql: str) -> str:
    cmd = [
        "docker",
        "exec",
        "cdc-postgres",
        "psql",
        "-U",
        "postgres",
        "-d",
        "shop_oltp",
        "-t",
        "-A",
        "-c",
        sql,
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    return res.stdout.strip()


def main():
    print("=" * 74)
    print("🛒 [OLTP TRAFFIC SIMULATOR] จำลองธุรกรรมจริงลง PostgreSQL (shop_oltp)")
    print("   - 55% INSERT ออเดอร์ใหม่")
    print("   - 33% UPDATE สถานะ (PENDING -> PAID -> SHIPPED) หรือปรับยอดเงิน")
    print("   - 12% DELETE ยกเลิกออเดอร์")
    print("💡 ทุกคำสั่งที่รันใน PostgreSQL จะไหลผ่าน Debezium WAL -> Kafka -> ClickHouse สดๆ!")
    print("=" * 74)

    try:
        while True:
            roll = random.random()
            if roll < 0.55:
                cust = random.choice(CUSTOMERS)
                prod, cat, price = random.choice(PRODUCTS)
                sql = (
                    f"WITH ins AS ("
                    f"  INSERT INTO public.orders (customer_name, product_name, category, amount, order_status) "
                    f"  VALUES ('{cust}', '{prod}', '{cat}', {price}, 'PENDING') RETURNING order_id"
                    f") "
                    f"INSERT INTO public.outbox_events (aggregate_type, aggregate_id, event_type, payload) "
                    f"SELECT 'ORDER', order_id, 'ORDER_CREATED', "
                    f"jsonb_build_object('order_id', order_id, 'customer', '{cust}', 'amount', {price}, 'status', 'PENDING') "
                    f"FROM ins RETURNING aggregate_id;"
                )
                out = run_pg_sql(sql)
                new_id = out.splitlines()[0] if out else "?"
                print(
                    f"🟢 [PG INSERT + OUTBOX TX] Order #{new_id} | {cust} สั่งซื้อ {prod} (฿{price:,.2f}) [PENDING]"
                )

            elif roll < 0.88:
                new_status = random.choice(["PAID", "SHIPPED", "DELIVERED"])
                sql = (
                    f"UPDATE public.orders SET order_status = '{new_status}', updated_at = CURRENT_TIMESTAMP "
                    f"WHERE order_id = (SELECT order_id FROM public.orders ORDER BY random() LIMIT 1) "
                    f"RETURNING order_id;"
                )
                out = run_pg_sql(sql)
                upd_id = out.splitlines()[0] if out else ""
                if upd_id:
                    print(f"🟡 [PG UPDATE] Order #{upd_id} เปลี่ยนสถานะเป็น -> {new_status}")

            else:
                sql = (
                    "DELETE FROM public.orders "
                    "WHERE order_id = (SELECT order_id FROM public.orders WHERE order_status = 'PENDING' ORDER BY random() LIMIT 1) "
                    "RETURNING order_id;"
                )
                out = run_pg_sql(sql)
                del_id = out.splitlines()[0] if out else ""
                if del_id:
                    print(f"🔴 [PG DELETE] ยกเลิกและลบ Order #{del_id} ออกจาก PostgreSQL")

            time.sleep(0.8)
    except KeyboardInterrupt:
        print("\n🛑 [OLTP SIMULATOR] หยุดจำลองธุรกรรมแล้ว")


if __name__ == "__main__":
    main()

