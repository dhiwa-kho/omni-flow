# 🎯 Omni-Flow: Job Interview Preparation Script (Bilingual: Thai - English)
> **คู่มือและสคริปต์ตอบคำถามสัมภาษณ์งานสถาปัตยกรรมระบบข้อมูล (Data Architecture & Distributed Systems)**  
> โครงสร้างการตอบ: **Problem (ปัญหา) ➔ Solution & Impact (วิธีแก้และผลลัพธ์) ➔ Key Tech & Architecture (เทคโนโลยีและรายละเอียดทางเทคนิค)**

---

## 🎙️ 0. Elevator Pitch: ภาพรวมโปรเจกต์ (30–45 วินาที)

### 🇬🇧 English Script
> *"Omni-Flow is a production-grade data architecture sandbox I built, inspired by Martin Kleppmann's **Designing Data-Intensive Applications (DDIA)**. It addresses real-world enterprise challenges across 6 key areas—including high-throughput streaming, stateful stream processing, tiered storage, log-based CDC, automated data quality gates, and zero-downtime schema evolution."*

### 🇹🇭 สคริปต์ภาษาไทย
> *"Omni-Flow คือโปรเจกต์สถาปัตยกรรมข้อมูลระดับโปรดักชันที่ผมสร้างขึ้นจริง โดยอิงหลักการจากหนังสือ **DDIA** ครับ โฟกัสการแก้โจทย์จริง 6 ด้านหลัก ทั้งเรื่อง Streaming ความเร็วสูง, Stateful Stream Processing, Tiered Storage ลดต้นทุน, Log-based CDC, Data Quality Gate อัตโนมัติ และการทำ Schema Evolution แบบ Zero-Downtime ครับ"*

---

## ⚡ 1. Real-Time Streaming & Chaos Engineering (`omni-command-center`)

* **Key Tech & Architecture:** Apache Kafka KRaft (3 Partitions), ClickHouse (`ReplacingMergeTree`), Python Multiprocess (`producer.py`, `consumer.py`, `dashboard.py`)

### 🇬🇧 English Script
* **Problem:** *"In streaming architectures, relying on average latency creates a blind spot because it hides severe tail latency experienced by heavy users. Additionally, unhandled Kafka broker crashes often crash consumer groups or cause message duplication."*
* **Solution & Impact:** *"In the `omni-command-center`, I implemented a 3-partition Kafka KRaft topic (`omni_stream_kappa`) streaming into ClickHouse's `sensor_events_kappa` table powered by `ReplacingMergeTree` for automatic deduplication. Instead of simple averages, our FastAPI dashboard computes **p50, p95, and p99 percentiles** in real time by comparing `sent_at` and `ingested_at`. I also conducted **Chaos Engineering experiments** by killing a broker node in our 3-broker cluster, proving that Kafka rebalances partitions smoothly with zero data loss."*

### 🇹🇭 สคริปต์ภาษาไทย
* **ปัญหา:** *"ในระบบ Streaming การดูแค่ค่าเฉลี่ย (Average Latency) จะหลอกตาเรา เพราะมันบดบังปัญหา Tail Latency ของลูกค้ารายใหญ่ และเมื่อ Kafka Broker ล่มกลางคัน Consumer มักสะดุดหรือทำให้ข้อมูลบันทึกซ้ำซ้อน"*
* **วิธีแก้ & ผลลัพธ์:** *"ในโฟลเดอร์ `omni-command-center` ผมออกแบบ Kafka KRaft 3 Partitions บน Topic `omni_stream_kappa` ส่งเข้า ClickHouse ตาราง `sensor_events_kappa` ที่ใช้ `ENGINE = ReplacingMergeTree` เพื่อตัดข้อมูลซ้ำอัตโนมัติ ระบบคำนวณ **p50, p95 และ p99** แบบเรียลไทม์จาก `sent_at` และ `ingested_at` นอกจากนี้ผมทำ **Chaos Testing** ปิด Broker ตัวจริงเพื่อพิสูจน์ว่า Consumer Group ทำ Auto-Rebalance ได้อย่างสมบูรณ์โดยที่ข้อมูลไม่หลุดหายครับ"*

---

## 🌊 2. Stateful Stream Processing & Watermarks (`lab1-flink-stream`)

* **Key Tech & Architecture:** Apache Flink 1.19 Cluster (`JobManager` + `TaskManager`), Flink SQL, Bounded-out-of-orderness Watermark, Tumbling Window

### 🇬🇧 English Script
* **Problem:** *"Network jitter causes sensor and payment events to arrive late and out of order. If aggregations rely on Server Processing Time, windowed financial totals will be corrupted."*
* **Solution & Impact:** *"In `lab1-flink-stream`, I authored a Flink SQL pipeline (`flink_job.sql`) that ingests from `raw_payments`. I decoupled Event Time from Processing Time by defining `WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND` on a `TUMBLE(event_ts, INTERVAL '10' SECOND)` window. This gives delayed network packets a 5-second grace period before emitting aggregated metrics and flagging fraud transactions (`amount >= 25,000 THB`) into `flink_windowed_output` with 10-second checkpoint guarantees."*

### 🇹🇭 สคริปต์ภาษาไทย
* **ปัญหา:** *"เน็ตเวิร์กดีเลย์ทำให้ข้อมูลธุรกรรมส่งมาไม่เรียงตามเวลาจริงและมาช้า หากระบบคำนวณยอดด้วยเวลาของ Server (Processing Time) ผลสรุปยอดเงินและรายงานบัญชีจะผิดพลาดทันที"*
* **วิธีแก้ & ผลลัพธ์:** *"ใน `lab1-flink-stream` ผมเขียน Flink SQL ใน `flink_job.sql` รับข้อมูลจาก `raw_payments` โดยแยก Event Time ออกจากเวลาเครื่องด้วยการตั้ง **Watermark 5 วินาที** (`event_ts - INTERVAL '5' SECOND`) บน **10s Tumbling Window** เพื่อเปิดระยะผ่อนผันให้ข้อมูลที่หลงทางมา ก่อนจะรวมยอดสรุปและดักจับธุรกรรมต้องสงสัย (`amount >= 25,000 THB`) ส่งลง `flink_windowed_output` พร้อมระบบ Checkpointing ทุก 10 วินาทีครับ"*

---

## 🏛️ 3. Hot/Cold Tiered Storage Lakehouse (`lab2-minio-lakehouse`)

* **Key Tech & Architecture:** ClickHouse (Hot Tier), MinIO S3 (Cold Tier), Apache Parquet (Snappy), Metadata Snapshot Catalog

### 🇬🇧 English Script
* **Problem:** *"Storing terabytes of raw event history in fast, SSD-backed analytical databases like ClickHouse drives cloud infrastructure costs prohibitively high."*
* **Solution & Impact:** *"In `lab2-minio-lakehouse`, I architected a **Tiered Storage Lakehouse**. Fresh events land in ClickHouse's `hot_events` table for sub-second operational queries. Older historical data is compacted by a background worker into columnar **Apache Parquet (Snappy-compressed)** and offloaded to MinIO S3 (`lakehouse-cold`). I maintained a `lakehouse_snapshots` catalog in ClickHouse, enabling analytical users to run **Time-Travel SQL queries directly on S3 Parquet files** via ClickHouse's `s3()` table function without reloading data into disk."*

### 🇹🇭 สคริปต์ภาษาไทย
* **ปัญหา:** *"การเก็บข้อมูลย้อนหลังปริมาณมหาศาลไว้บน SSD ใน ClickHouse ตลอดเวลา ทำให้ค่าเช่าดิสก์และทรัพยากรพุ่งสูงเกินความจำเป็น"*
* **วิธีแก้ & ผลลัพธ์:** *"ใน `lab2-minio-lakehouse` ผมออกแบบ **Tiered Storage** โดยข้อมูลสดจะเก็บใน ClickHouse ตาราง `hot_events` สำหรับมอนิเตอร์ระดับเสี้ยววินาที ส่วนข้อมูลเก่าย้อนหลัง Worker จะบีบอัดเป็น **Apache Parquet (Snappy)** ย้ายไปเก็บที่ MinIO S3 bucket `lakehouse-cold` ช่วยลดค่า Storage ได้กว่า 70% และมีตารางแคตตาล็อก `lakehouse_snapshots` ทำให้เราใช้ฟังก์ชัน `s3()` บน ClickHouse ทำ **Time-Travel Query อ่านตรงจาก S3** ได้ทันทีโดยไม่ต้องโหลดข้อมูลกลับลงดิสก์ครับ"*

---

## 🔄 4. Log-Based CDC & Transactional Outbox Pattern (`lab3-debezium-cdc`)

* **Key Tech & Architecture:** PostgreSQL 16 (Logical Decoding / WAL), Debezium Kafka Connect, ClickHouse (`ReplacingMergeTree(version_ts, is_deleted)`)

### 🇬🇧 English Script
* **Problem:** *"Using application-level dual-writes to update PostgreSQL and ClickHouse creates race conditions, partial write failures, and dirty analytics. Also, frequent polling queries degrade transactional DB performance."*
* **Solution & Impact:** *"In `lab3-debezium-cdc`, I deployed Debezium to stream row-level changes directly from PostgreSQL's Write-Ahead Log (WAL) with `REPLICA IDENTITY FULL` on both the `orders` table and a `transactional outbox` table. In ClickHouse, I designed the `orders_replica` table using `ReplacingMergeTree(version_ts, is_deleted)` which natively handles updates and tombstones (soft-deletes) without locks, alongside an `orders_cdc_changelog` table that preserves a complete before-and-after audit trail."*

### 🇹🇭 สคริปต์ภาษาไทย
* **ปัญหา:** *"การเขียนโค้ดบันทึกข้อมูลลง 2 ที่พร้อมกัน (Dual-Write) เสี่ยงมากที่จะเกิดข้อผิดพลาดครึ่งๆ กลางๆ ทำให้ข้อมูลใน OLAP กับ OLTP ไม่ตรงกัน และการเขียน Query ไปดึงข้อมูลบ่อยๆ ก็ดึง CPU ของฐานข้อมูลหลัก"*  
* **วิธีแก้ & ผลลัพธ์:** *"ใน `lab3-debezium-cdc` ผมใช้ Debezium ดึงเหตุการณ์ตรงจาก Write-Ahead Log (WAL) ของ Postgres ด้วย `REPLICA IDENTITY FULL` ทั้งในตาราง `orders` และ `outbox_events` ส่วนฝั่ง ClickHouse ผมออกแบบตาราง `orders_replica` โดยใช้ `ReplacingMergeTree(version_ts, is_deleted)` ทำให้รองรับทั้งการ Update และ Delete (Tombstone) โดยไม่ติด Lock และมีตาราง `orders_cdc_changelog` เก็บประวัติ Before & After ไว้อย่างสมบูรณ์ครับ"*

---

## 🎛️ 5. Medallion Pipeline & Automated Data Quality Gate (`lab4-orchestration-audit`)

* **Key Tech & Architecture:** Medallion DAG (`bronze_raw_events` ➔ `silver_clean_events` ➔ `gold_executive_mart`), Quarantine Isolation, Key-Level Reconciliation Audit

### 🇬🇧 English Script
* **Problem:** *"Malformed payloads or null values silently slip into production executive dashboards, and data engineers often lack real-time visibility into exactly where records were dropped or corrupted."*
* **Solution & Impact:** *"In `lab4-orchestration-audit`, I built a Medallion DAG orchestrator in Python and ClickHouse. Incoming records in `bronze_raw_events` pass through an **Automated Data Quality Gate**. Clean rows advance to `silver_clean_events`, while invalid rows (such as null IDs or negative amounts) are quarantined into `quarantine_bad_rows` with explicit failure reasons—preventing pipeline halts. Finally, an **Audit Engine** executes a key-level LEFT JOIN reconciliation across layers, verifying `Bronze = Silver + Quarantine` (100% data accountancy) in `dag_run_history`."*

### 🇹🇭 สคริปต์ภาษาไทย
* **ปัญหา:** *"ข้อมูลผิดรูปแบบหรือมีค่า Null หลุดเข้าไปในรายงานสำหรับผู้บริหารโดยไม่มีใครรู้ตัว และทีม Data มักไม่มีหลักฐานตรวจสอบว่าข้อมูลหล่นหายไปในขั้นตอนไหน"*  
* **วิธีแก้ & ผลลัพธ์:** *"ใน `lab4-orchestration-audit` ผมสร้าง Medallion DAG บน ClickHouse โดยรับข้อมูลเข้า `bronze_raw_events` แล้วผ่าน **Automated Quality Gate** แถวที่สมบูรณ์จะไปต่อที่ `silver_clean_events` ส่วนแถวที่มีปัญหา (เช่น Null หรือค่าติดลบ) จะถูกกักกันไว้ใน `quarantine_bad_rows` พร้อมระบุสาเหตุ ทำให้ Pipeline ไม่พัง และมี **Audit Engine** ทำ Key-Level LEFT JOIN พิสูจน์ว่า `Bronze = Silver + Quarantine` ครบ 100% บันทึกผลลง `dag_run_history` ครับ"*

---

## 🔬 6. Binary Serialization & Schema Evolution (`lab5-encoding-evolution`)

* **Key Tech & Architecture:** Protobuf, Apache Avro, Apache Thrift, MessagePack, JSON, FastAPI Interactive Simulator

### 🇬🇧 English Script
* **Problem:** *"JSON payloads waste immense network bandwidth due to repetitive field name strings, and deploying microservices independently (Rolling Upgrades) frequently breaks compatibility when developers inadvertently add required fields or alter field IDs."*
* **Solution & Impact:** *"In `lab5-encoding-evolution`, I benchmarked real-world payloads (`Martin / 1337 / Interests`) across encodings, demonstrating that **Protobuf (33B) and Avro (32B) shrink payload size by over 60% compared to JSON (81B)** through field tags, varint encoding, and schema resolution. I also created interactive simulations of **Backward and Forward Compatibility**, proving how to avoid the 'Database Trap' where legacy clients silently drop unknown new fields during read-modify-write cycles."*

### 🇹🇭 สคริปต์ภาษาไทย
* **ปัญหา:** *"การส่งข้อมูลด้วย JSON เปลือง Bandwidth มากเพราะต้องแนบชื่อฟิลด์ซ้ำๆ ทุก Record และการทำ Rolling Upgrade บริการต่างๆ มักทำให้ระบบล่มถ้าเผลอไปเพิ่มฟิลด์แบบ Required หรือแก้หมายเลข Field Tag"*  
* **วิธีแก้ & ผลลัพธ์:** *"ใน `lab5-encoding-evolution` ผมทำ Simulation ผ่าดูไบต์จริงของชุดข้อมูลตัวอย่าง พิสูจน์ว่า **Protobuf (33B) และ Avro (32B) ลดขนาด Payload ได้มากกว่า 60% เมื่อเทียบกับ JSON (81B)** ด้วยการใช้ Field Tag, Varint และ Schema Resolution พร้อมจำลองกฎ **Backward & Forward Compatibility** และสาธิตวิธีป้องกัน 'กับดักฐานข้อมูล (Database Trap)' ที่โค้ดเก่าเผลอลบฟิลด์ใหม่ทิ้งเวลาบันทึกทับครับ"*

---

## 🎯 คำถามปิดท้าย: *"Why this tech stack?" (ทำไมถึงเลือกใช้ Stack เหล่านี้?)*

### 🇬🇧 English Script
> *"I adopted the philosophy of **'the right tool for the right job'**:  
> - **Kafka** provides fault-tolerant, high-throughput event ingestion.  
> - **Flink** delivers stateful, low-latency stream processing with event-time semantics.  
> - **ClickHouse** excels at sub-second analytical aggregations over billions of rows.  
> - **MinIO / Parquet** provides cost-effective long-term cold storage.  
> This separation of concerns guarantees both massive scalability and optimal cloud cost efficiency."*

### 🇹🇭 สคริปต์ภาษาไทย
> *"ผมยึดหลัก **Right Tool for the Right Job** ครับ:  
> - **Kafka:** รับกระแสข้อมูลปริมาณมหาศาลได้อย่างทนทาน  
> - **Flink:** คำนวณผลสตรีมมิ่งแบบ Stateful และการันตีความถูกต้องด้วย Event Time  
> - **ClickHouse:** ตอบโจทย์ Real-time Analytics แบบ Columnar ความเร็วสูง  
> - **MinIO / Parquet:** จัดเก็บข้อมูลย้อนหลังระยะยาวในต้นทุนที่ถูกที่สุด  
> การแบ่งหน้าที่ชัดเจนแบบนี้ทำให้ระบบทั้งรองรับการขยายตัว (Scale) และคุมต้นทุน (Cost) ได้ดีที่สุดครับ"*
