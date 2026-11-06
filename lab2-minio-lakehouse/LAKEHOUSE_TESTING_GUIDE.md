# 📘 คู่มือ Lab 2: Object Storage & Data Lakehouse (MinIO S3 + Parquet + Time-Travel Query)

ชุดทดลองสถาปัตยกรรม **Modern Data Lakehouse (Hot & Cold Storage Tiering)**:
* **Hot Tier:** เก็บข้อมูลสดไว้ในตาราง **ClickHouse (`MergeTree`)** เพื่อการคิวรีที่รวดเร็ว
* **Cold Lakehouse Tier:** แพ็กข้อมูลทุกๆ 20 รายการเป็นไฟล์บีบอัด **Apache Parquet (`.parquet`)** เก็บลง **MinIO S3 Object Storage** พร้อมตาราง Metadata Catalog (`lakehouse_snapshots`) สำหรับจำลองการทำ **Snapshot / Time-Travel Query**
* **ข้อสังเกตสำหรับการสัมภาษณ์ (DDIA Ch. 3, 4, 10):** MinIO และไฟล์ `.parquet` ล้วนๆ ไม่ได้มีกลไก Time-Travel อัตโนมัติในตัวเอง แต่ต้องอาศัยชั้นจัดการ Metadata/Snapshot Catalog (ในแล็บนี้ใช้ตาราง `lakehouse_snapshots` และฟิลด์ `snapshot_id` ส่วนในระบบจริงมักใช้ Open Table Format เช่น Apache Iceberg, Delta Lake หรือ Apache Hudi)

---

## 1. วิธีเปิดรันระบบ (เมื่อเอาไปเล่นที่บ้าน)

```powershell
cd c:\Omni-Flow\lab2-minio-lakehouse
docker compose up -d
```

จากนั้นเปิด Terminal 2 หน้าต่างรัน:
* **หน้าต่างที่ 1 (ตัวส่งข้อมูลเข้า Kafka):**
  ```powershell
  python lakehouse_producer.py
  ```
* **หน้าต่างที่ 2 (ตัวแบ่งข้อมูลลง Hot ClickHouse + แพ็กไฟล์ Parquet ขึ้น MinIO S3):**
  ```powershell
  python lakehouse_sink_worker.py
  ```

---

## 2. เปิดดูไฟล์ `.parquet` บนหน้าเว็บ MinIO S3 Console
1. เปิดเบราว์เซอร์ไปที่: 👉 **http://localhost:9001**
2. ล็อกอินด้วย:
   * **Username:** `minioadmin`
   * **Password:** `minioadmin123`
3. คลิกที่เมนู **Object Browser** -> คลิกถัง **`omni-lakehouse`** -> คลิกโฟลเดอร์ **`snapshots/`**
   * คุณจะเห็นไฟล์ `snap_0001.parquet`, `snap_0002.parquet`, `snap_0003.parquet` ถูกสร้างและอัปโหลดเข้ามาสดๆ ทุกๆ 3 วินาที!

---

## 3. ตั้งค่า DBeaver และคำสั่ง SQL ทดสอบ Data Lakehouse & Time-Travel

เชื่อมต่อ **ClickHouse** ใน DBeaver:
* **Host:** `localhost` | **Port:** `8123`
* **Database:** `lakehouse_db`
* **Username:** `default` *(Password เว้นว่าง)*

### คำสั่ง SQL สำหรับลองรันใน DBeaver:

```sql
-- 1. ดูตารางสารบัญ Snapshot (Lakehouse Metadata Catalog) ว่ามีไฟล์ Parquet กี่เวอร์ชันแล้วบน MinIO S3
SELECT
    snapshot_id,
    s3_parquet_path,
    row_count,
    min_seq_no,
    max_seq_no,
    total_output_kw,
    total_revenue_thb,
    committed_at
FROM lakehouse_db.lakehouse_snapshots
ORDER BY snapshot_id DESC;

-- 2. [ZERO-COPY S3 QUERY] คิวรีอ่านข้อมูลจากไฟล์ .parquet ทุกไฟล์ในถัง MinIO S3 โดยตรง!
SELECT
    region,
    category,
    count() AS total_events_in_s3_parquet,
    round(sum(output_kw), 2) AS s3_total_kw,
    round(sum(revenue_thb), 2) AS s3_total_revenue_thb
FROM s3(
    'http://minio:9000/omni-lakehouse/snapshots/*.parquet',
    'minioadmin',
    'minioadmin123',
    'Parquet'
)
GROUP BY region, category
ORDER BY s3_total_revenue_thb DESC;

-- 3. [TIME-TRAVEL QUERY] ย้อนเวลากลับไปดูข้อมูล "เฉพาะ ณ ตอนจบ Snapshot ที่ 1 ถึง 2" (ไม่เอาข้อมูลใหม่หลังจากนั้น)
SELECT
    snapshot_id,
    region,
    count() AS events_at_that_time,
    round(sum(revenue_thb), 2) AS revenue_at_that_time
FROM s3(
    'http://minio:9000/omni-lakehouse/snapshots/*.parquet',
    'minioadmin',
    'minioadmin123',
    'Parquet'
)
WHERE snapshot_id <= 2
GROUP BY snapshot_id, region
ORDER BY snapshot_id, region;

-- 4. [SIMULATE HOT TIER RETENTION] ลองลบข้อมูลเก่าออกจากตาราง Hot ClickHouse (เพื่อประหยัด SSD)
-- แล้วพิสูจน์ว่าข้อมูลใน Cold S3 Parquet ยังอยู่ครบ 100%!
ALTER TABLE lakehouse_db.hot_events DELETE WHERE snapshot_id = 1;

-- เช็กเทียบกันระหว่าง Hot Table (ที่ลบ Snapshot 1 ทิ้งแล้ว) vs Cold S3 Parquet (ที่ยังเก็บ Snapshot 1 ไว้ครบ!)
SELECT
    (SELECT count() FROM lakehouse_db.hot_events WHERE snapshot_id = 1) AS rows_in_hot_clickhouse,
    (SELECT count() FROM s3('http://minio:9000/omni-lakehouse/snapshots/snap_0001.parquet', 'minioadmin', 'minioadmin123', 'Parquet')) AS rows_preserved_in_cold_s3_parquet;
```

