# 📘 คู่มือ Lab 1: Apache Flink Stream Processing Engine (Kafka ──► Flink 10s Window ──► ClickHouse)

ชุดทดลองการประมวลผลข้อมูลแบบ Real-time กลางอากาศด้วย **Apache Flink Cluster (`JobManager` + `TaskManager`)** คั่นกลางระหว่าง **Kafka** และ **ClickHouse**

---

## 1. โครงสร้างไฟล์ใน `lab1-flink-stream`

```text
lab1-flink-stream/
├── docker-compose.yml       # รัน Flink JobManager (:8081) + TaskManager + Kafka (:9092) + ClickHouse (:8123)
├── jars/                    # เก็บปลั๊กอิน flink-sql-connector-kafka-3.2.0-1.19.jar (ใช้แบบ Offline ที่บ้านได้ทันที!)
├── flink_job.sql            # คำสั่ง Flink SQL สร้าง Streaming Job (10-Second Tumbling Window + Fraud Filter)
├── payment_producer.py      # ตัวส่งธุรกรรมบัตรเครดิตจำลองเข้า Kafka (raw_payments)
├── clickhouse_sink.py       # ตัวรับผลลัพธ์จาก Flink ไปบันทึกลง ClickHouse (flink_olap.flink_window_summary)
└── FLINK_TESTING_GUIDE.md   # คู่มือการเปิดดู Flink Web UI และคำสั่ง SQL ใน DBeaver
```

---

## 2. วิธีเปิดรันระบบ (เมื่อเอาไปเล่นที่บ้าน)

### ขั้นตอนที่ 2.1: สตาร์ท Docker Containers
```powershell
cd c:\Omni-Flow\lab1-flink-stream
docker compose up -d
```

### ขั้นตอนที่ 2.2: เปิดรัน Producer และ ClickHouse Sink (แยก 2 หน้าต่าง Terminal)
* **หน้าต่างที่ 1 (ยิงธุรกรรมเข้า Kafka):**
  ```powershell
  python payment_producer.py
  ```
* **หน้าต่างที่ 2 (รอรับผลลัพธ์จาก Flink ลง ClickHouse):**
  ```powershell
  python clickhouse_sink.py
  ```

### ขั้นตอนที่ 2.3: สั่งส่ง Streaming Job เข้าไปรันใน Apache Flink Cluster
รันคำสั่งนี้ใน PowerShell:
```powershell
docker exec flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/flink_job.sql
```

---

## 3. เปิดดูหน้าเว็บ Apache Flink Web Dashboard
* เปิดเบราว์เซอร์ไปที่: 👉 **http://localhost:8081**
* คลิกที่เมนู **Running Jobs** ด้านซ้ายมือ -> คลิกที่ชื่อ Job **`Omni-Flow Flink 10s Tumbling Window & Fraud Filter`**
* คุณจะเห็นกราฟ **Streaming DAG** ของ Flink ที่กำลังดูดข้อมูลจาก `raw_payments` มาทำ `GroupWindowAggregate (TUMBLE 10s)` แล้วส่งออกไปยัง `flink_windowed_output` แบบสดๆ พร้อมสถิติ `Records Received / Sent`
* **ข้อสังเกตสำหรับการสัมภาษณ์ (DDIA Chapter 11):**
  * ตัวอย่างใน `flink_job.sql` นี้ใช้ `proc_time AS PROCTIME()` (Processing Time) เพื่อให้รันทดสอบเห็นผลหน้าต่างเวลาทุก 10 วินาทีได้ทันที
  * ในระบบ Production ที่ต้องการความถูกต้องเมื่อเกิด **Late / Out-of-Order Events** จะต้องเปลี่ยนไปใช้ **Event Time** คู่กับการประกาศ **`WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND`** และเปิด **Checkpointing** เพื่อบันทึกสถานะลง Persistent Storage สำหรับใช้กู้คืนเมื่อเกิดการล้มเหลว

---

## 4. คำสั่ง SQL ตรวจสอบผลลัพธ์จาก Flink ใน DBeaver (`ClickHouse`)

เชื่อมต่อ **ClickHouse** ใน DBeaver (`Host: localhost`, `Port: 8123`, `Database: flink_olap`, `User: default`) แล้วรันคำสั่งนี้:

```sql
-- 1. ดูผลลัพธ์ 10-Second Tumbling Window ที่ถูกคำนวณมาจาก Apache Flink Cluster สดๆ
SELECT
    window_start,
    window_end,
    category,
    tx_count,
    total_amount,
    max_single_tx,
    high_risk_count,
    ingested_at
FROM flink_olap.flink_window_summary FINAL
ORDER BY window_start DESC, total_amount DESC
LIMIT 25;

-- 2. สรุปยอดรวมและจำนวนรายการเสี่ยงสูง (Fraud >= 25,000 บาท) แยกตามหมวดหมู่ร้านค้า
SELECT
    category,
    sum(tx_count) AS total_transactions,
    round(sum(total_amount), 2) AS grand_total_thb,
    max(max_single_tx) AS highest_tx_thb,
    sum(high_risk_count) AS total_high_risk_alerts
FROM flink_olap.flink_window_summary FINAL
GROUP BY category
ORDER BY grand_total_thb DESC;
```

