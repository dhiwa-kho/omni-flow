# 📘 คู่มือ Lab 3: Real-Time Change Data Capture (PostgreSQL + Debezium + Kafka + ClickHouse)

ชุดทดลองสายทำมาหากินจริง (**Production CDC Pipeline**): ดักจับการเปลี่ยนแปลง (`INSERT`, `UPDATE`, `DELETE`) จาก **PostgreSQL Write-Ahead Log (WAL)** ด้วย **Debezium** ส่งเข้า **Kafka** แล้วซิงก์ลงตาราง **ClickHouse (`ReplacingMergeTree`)** แบบ Real-time โดยไม่สร้างภาระให้ฐานข้อมูลหลัก

---

## 1. โครงสร้างไฟล์ใน `lab3-debezium-cdc`

```text
lab3-debezium-cdc/
├── docker-compose.yml       # รัน PostgreSQL 16 (WAL logical) + Kafka + Debezium Connect + ClickHouse
├── init-postgres.sql        # สร้างตาราง public.orders พร้อมข้อมูลตั้งต้น 5 รายการ (Order #101..#105)
├── register_connector.py    # สคริปต์สั่งผูก Debezium เข้ากับ PostgreSQL ผ่าน REST API (:8083)
├── cdc_sink_worker.py       # ตัวดึง CDC Event จาก Kafka ไปอัปเดตลง ClickHouse (รองรับ Insert/Update/Delete)
└── CDC_TESTING_GUIDE.md     # คู่มือการรันและโจทย์ทดสอบใน DBeaver
```

---

## 2. วิธีเปิดรัน Lab 3 (ทีละขั้นตอน)

> **สำคัญ:** ก่อนรัน Lab 3 อย่าลืมปิด Docker ของ Lab อื่นก่อน (`docker compose down`) เพื่อไม่ให้พอร์ต `9092` และ `8123` ชนกันครับ

### ขั้นตอนที่ 2.1: สตาร์ท Docker Containers ทั้ง 4 ตัว
```powershell
cd c:\Omni-Flow\lab3-debezium-cdc
docker compose up -d
```
*(รอประมาณ 15–20 วินาทีให้ `cdc-debezium` บูตเสร็จ)*

### ขั้นตอนที่ 2.2: สั่งลงทะเบียน Debezium Connector
```powershell
python register_connector.py
```
เมื่อสำเร็จ จะขึ้นข้อความ `🚀 ลงทะเบียน Debezium CDC Connector สำเร็จ: omni-postgres-orders-cdc` และ Debezium จะทำ **Initial Snapshot** ดูดออเดอร์ `#101` ถึง `#105` เข้า Kafka ทันที!

### ขั้นตอนที่ 2.3: รันตัวซิงก์ข้อมูลเข้า ClickHouse
```powershell
python cdc_sink_worker.py
```
จะเห็นข้อความ `📸 SNAPSHOT | Order #101 .. #105 -> Synced to ClickHouse!` ขึ้นมาทันที! (เปิดหน้าต่างนี้ทิ้งไว้ได้เลย)

---

## 3. การตั้งค่า DBeaver (ต่อ 2 ฐานข้อมูลเทียบกันสดๆ)

ให้เพิ่ม Connection ใน **DBeaver** 2 ตัวเพื่อเปิดเทียบกันซ้าย-ขวา:

1. **ฝั่งต้นทาง (PostgreSQL - OLTP):**
   * **Host:** `localhost` | **Port:** `5432`
   * **Database:** `shop_oltp`
   * **Username:** `postgres` | **Password:** `password123`
2. **ฝั่งปลายทาง (ClickHouse - OLAP):**
   * **Host:** `localhost` | **Port:** `8123`
   * **Database:** `cdc_olap`
   * **Username:** `default` *(Password เว้นว่าง)*

---

## 4. โจทย์ทดสอบ Real-Time CDC (รันใน DBeaver ฝั่ง PostgreSQL แล้วดูผลใน ClickHouse)

### 🧪 การทดสอบที่ 1: ทดสอบ `INSERT` ออเดอร์ใหม่ใน PostgreSQL
รันคำสั่งนี้ใน SQL Editor ของ **PostgreSQL (`shop_oltp`)**:
```sql
INSERT INTO public.orders (customer_name, product_name, category, amount, order_status)
VALUES ('Dhiwa Architect', 'NVIDIA RTX 5090', 'Electronics', 75900.00, 'PENDING');
```
* **ดูที่ Terminal (`cdc_sink_worker.py`):** จะขึ้น `🟢 INSERT | Order #106 (Dhiwa Architect) -> Synced to ClickHouse!` ทันที

---

### 🧪 การทดสอบที่ 2: ทดสอบ `UPDATE` แก้ไขสถานะและราคาใน PostgreSQL
รันคำสั่งนี้ใน **PostgreSQL (`shop_oltp`)**:
```sql
UPDATE public.orders
SET order_status = 'PAID',
    amount = 72000.00,
    updated_at = CURRENT_TIMESTAMP
WHERE order_id = 102;
```
* **ดูที่ Terminal:** จะขึ้น `🟡 UPDATE | Order #102 (Suda W.) | Status: PENDING -> PAID`

---

### 🧪 การทดสอบที่ 3: ทดสอบ `DELETE` ลบออเดอร์ออกจาก PostgreSQL
รันคำสั่งนี้ใน **PostgreSQL (`shop_oltp`)**:
```sql
DELETE FROM public.orders WHERE order_id = 105;
```
* **ดูที่ Terminal:** จะขึ้น `🔴 DELETE | Order #105 (Kanya S.) | Status: PENDING -> DELETED`

---

## 5. คำสั่ง SQL ตรวจสอบผลลัพธ์ใน ClickHouse (`cdc_olap`)

สลับไปที่หน้าต่าง SQL Editor ของ **ClickHouse (`cdc_olap`)** ใน DBeaver แล้วรันคำสั่งเหล่านี้:

```sql
-- 1. ดูข้อมูลสถานะปัจจุบันที่ซิงก์มาจาก PostgreSQL
-- (หมายเหตุ: ClickHouse ReplacingMergeTree(version_ts, is_deleted) ร่วมกับคีย์เวิร์ด FINAL 
-- ทำหน้าที่ยุบรวมแถวตาม order_id และกรองแถวที่ is_deleted = 1 ออกตอนคิวรี 
-- ซึ่งเป็นการทำ Deduplication/Upsert ที่ฝั่ง Sink ตามลำดับ version_ts ไม่ใช่ Distributed Transaction ข้ามระบบ)
SELECT
    order_id,
    customer_name,
    product_name,
    category,
    amount,
    order_status,
    cdc_op AS last_operation,
    synced_at
FROM cdc_olap.orders_replica FINAL
ORDER BY order_id;

-- 2. ดูประวัติการแก้ไขย้อนหลังทั้งหมด (CDC Audit Trail: ก่อนแก้ vs หลังแก้)
SELECT
    logged_at,
    order_id,
    cdc_op,
    before_status,
    after_status,
    before_amount,
    after_amount
FROM cdc_olap.orders_cdc_changelog
ORDER BY wal_ts_ms DESC;

-- 3. วิเคราะห์ยอดขายรวมแบบ Real-time OLAP แยกตามหมวดหมู่สินค้า (เฉพาะรายการที่ยังไม่ถูกลบ)
SELECT
    category,
    count() AS active_orders,
    sum(amount) AS total_revenue_thb
FROM cdc_olap.orders_replica FINAL
GROUP BY category
ORDER BY total_revenue_thb DESC;
```

---

## 6. วิธีปิดระบบเมื่อเลิกเล่น
1. กด `Ctrl + C` ในหน้าต่าง `cdc_sink_worker.py`
2. สั่งปิด Docker Containers:
   ```powershell
   docker compose down
   ```

