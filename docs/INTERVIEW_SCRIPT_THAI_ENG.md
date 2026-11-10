# 🎯 Omni-Flow: Job Interview Preparation Script (Bilingual: Thai - English)
> **คู่มือและสคริปต์ตอบคำถามสัมภาษณ์งานสถาปัตยกรรมระบบข้อมูล (Data Architecture & Distributed Systems)**  
> โครงสร้างการตอบ: **Problem (ปัญหา) ➔ Solution & Tech Stack (การแก้ปัญหา) ➔ Key Engineering Insight (ผลลัพธ์และคีย์เวิร์ดระดับ Senior)**

---

## 🎙️ 0. Elevator Pitch: ภาพรวมโปรเจกต์ (30–45 วินาที)

### 🇬🇧 English Script
> *"Omni-Flow is a production-grade data architecture sandbox I built, inspired by Martin Kleppmann's **Designing Data-Intensive Applications (DDIA)**. It addresses real-world enterprise challenges across 6 key areas—including high-throughput streaming, stateful stream processing, tiered storage, log-based CDC, automated data quality gates, and zero-downtime schema evolution."*

### 🇹🇭 สคริปต์ภาษาไทย
> *"Omni-Flow คือโปรเจกต์สถาปัตยกรรมข้อมูลระดับโปรดักชันที่ผมสร้างขึ้นจริง โดยอิงหลักการจากหนังสือ **DDIA** ครับ โฟกัสการแก้โจทย์จริง 6 ด้านหลัก ทั้งเรื่อง Streaming ความเร็วสูง, Stateful Stream Processing, Tiered Storage ลดต้นทุน, Log-based CDC, Data Quality Gate อัตโนมัติ และการทำ Schema Evolution แบบ Zero-Downtime ครับ"*

---

## ⚡ 1. Omni Command Center: Real-Time Streaming & Chaos Engineering (DDIA Ch. 1, 5, 6, 8, 9)

* **Key Tech:** Apache Kafka (KRaft, 3 Partitions), ClickHouse (`ReplacingMergeTree`), Python Multiprocess

### 🇬🇧 English Script
* **Problem:** *"A common mistake in streaming systems is monitoring only average latency, which hides extreme delays experienced by heavy users. Furthermore, unhandled broker crashes often cause producers to fail or drop messages."*
* **Solution & Impact:** *"I built an end-to-end streaming pipeline with Kafka KRaft and ClickHouse ReplacingMergeTree for deduplication. Rather than relying on simple averages, the system computes **p50, p95, and p99 percentiles** in real time. I also performed **chaos engineering experiments** by killing active brokers to prove that consumer groups rebalance automatically without message loss."*

### 🇹🇭 สคริปต์ภาษาไทย
* **ปัญหา:** *"ข้อผิดพลาดทั่วไปของระบบ Streaming คือการดูแค่ Average Latency ซึ่งบดบังความล่าช้าของลูกค้ารายใหญ่ และเมื่อ Broker ล่มก็มักทำให้ Producer สะดุดหรือข้อมูลสูญหาย"*
* **วิธีแก้ & ผลลัพธ์:** *"ผมออกแบบ Pipeline ด้วย Kafka KRaft 3 Partitions ส่งเข้า ClickHouse ReplacingMergeTree เพื่อทำ Deduplication โดยระบบมอนิเตอร์ **p50, p95 และ p99** แบบเรียลไทม์ และทำ **Chaos Testing** ปิด Broker เพื่อทดสอบว่า Consumer Group ทำ Auto-Rebalance ได้อย่างสมบูรณ์โดยที่ข้อมูลไม่สูญหายครับ"*

---

## 🌊 2. Lab 1: Apache Flink (Stateful Stream Processing & Watermarks) (DDIA Ch. 11)

* **Key Tech:** Apache Flink 1.19 Cluster, Flink SQL, Tumbling Windows, Watermarks

### 🇬🇧 English Script
* **Problem:** *"Network latency inevitably causes out-of-order and late-arriving events. Aggregating data using Server Processing Time yields fundamentally inaccurate metrics and reports."*
* **Solution & Impact:** *"I implemented Apache Flink SQL to decouple **Event Time** from Server Processing Time. By configuring a **5-second bounded out-of-orderness watermark** on a **10-second tumbling window**, Flink gives late records a grace period before finalizing aggregations, ensuring 100% calculation accuracy despite network delays."*

### 🇹🇭 สคริปต์ภาษาไทย
* **ปัญหา:** *"ปัญหาเน็ตเวิร์กดีเลย์ทำให้ข้อมูลส่งมาไม่เป็นระเบียบและมาช้ากว่าปกติ ถ้าเราคำนวณยอดด้วยเวลาของ Server (Processing Time) ผลสรุปจะเพี้ยนทันที"*
* **วิธีแก้ & ผลลัพธ์:** *"ผมใช้ Apache Flink SQL แยก **Event Time** ออกจาก Processing Time โดยตั้ง **Watermark 5 วินาที** บน **10s Tumbling Window** เพื่อให้ Flink รอข้อมูลที่มาดีเลย์ก่อนตัดยอด ทำให้ได้ผลลัพธ์ที่ถูกต้อง 100% ตามเวลาที่เกิดเหตุการณ์จริงครับ"*

---

## 🏛️ 3. Lab 2: Hot/Cold Tiered Storage Lakehouse (DDIA Ch. 3, 4, 10, 12)

* **Key Tech:** ClickHouse (Hot Tier), MinIO S3 (Cold Tier), Apache Parquet, Snappy Compression, Snapshot Catalog

### 🇬🇧 English Script
* **Problem:** *"Retaining years of historical raw events in high-performance SSD-backed analytical databases like ClickHouse causes storage infrastructure bills to skyrocket."*
* **Solution & Impact:** *"I designed a **Hot/Cold Tiered Storage** architecture. Active data from the last 7 days resides in ClickHouse (Hot Tier) for sub-second query performance, while older historical records are compacted into columnar **Apache Parquet with Snappy compression** and offloaded to MinIO S3 (Cold Tier). This reduces storage costs by over 70% while supporting point-in-time time-travel queries via a metadata catalog."*

### 🇹🇭 สคริปต์ภาษาไทย
* **ปัญหา:** *"การเก็บข้อมูลย้อนหลังหลายปีไว้บน SSD ใน ClickHouse ตลอดเวลา ทำให้ต้นทุนค่าพื้นที่จัดเก็บบวมสูงเกินจำเป็น"*
* **วิธีแก้ & ผลลัพธ์:** *"ผมออกแบบ **Tiered Storage** โดยเก็บข้อมูล 7 วันล่าสุดไว้ที่ ClickHouse (Hot Tier) เพื่อให้ตอบ Query ได้ระดับเสี้ยววินาที ส่วนข้อมูลเก่าย้อนหลังจะถูกแปลงเป็นไฟล์ **Parquet บีบอัด Snappy** ส่งไปเก็บที่ MinIO S3 (Cold Tier) ช่วยลดค่าใช้จ่ายลงได้มากกว่า 70% และมี Snapshot Catalog รองรับ Time-Travel Query ด้วยครับ"*

---

## 🔄 4. Lab 3: Log-Based CDC & Transactional Outbox Pattern (DDIA Ch. 3, 5, 7, 11)

* **Key Tech:** PostgreSQL 16 (Logical Decoding / WAL), Debezium Connect, Apache Kafka, ClickHouse

### 🇬🇧 English Script
* **Problem:** *"Using application-level dual-writes to synchronize PostgreSQL and ClickHouse introduces severe data inconsistencies and race conditions whenever one service fails midway."*
* **Solution & Impact:** *"I implemented **Log-Based CDC** using Debezium to tap directly into PostgreSQL's Write-Ahead Log (WAL) alongside the **Transactional Outbox Pattern**. The application only commits locally to Postgres inside a standard ACID transaction, and Debezium asynchronously streams changes to Kafka and ClickHouse without placing analytical read load on the primary OLTP database."*

### 🇹🇭 สคริปต์ภาษาไทย
* **ปัญหา:** *"การให้โค้ดแอปพลิเคชันเขียนข้อมูลลง 2 ฐานข้อมูลพร้อมกัน (Dual-Write) เสี่ยงมากที่จะทำให้ข้อมูลไม่ตรงกัน ถ้าเกิดข้อผิดพลาดขึ้นระหว่างทาง"*
* **วิธีแก้ & ผลลัพธ์:** *"ผมใช้ **Log-Based CDC** ผ่าน Debezium ดึงเหตุการณ์ตรงจาก Write-Ahead Log (WAL) ของ Postgres ร่วมกับ **Transactional Outbox Pattern** แอปพลิเคชันจึงเขียนแค่จุดเดียวแบบ ACID แล้วให้ระบบสตรีมต่อไปยัง ClickHouse โดยไม่รบกวนประสิทธิภาพของฐานข้อมูลหลักเลยครับ"*

---

## 🎛️ 5. Lab 4: Medallion Pipeline, DQ Gate & Reconciliation Audit (DDIA Ch. 10, 12)

* **Key Tech:** Medallion Architecture (Bronze ➔ Silver ➔ Gold), Quarantine Table, Automated Reconciliation Engine

### 🇬🇧 English Script
* **Problem:** *"Malformed payloads and silent schema drift often corrupt downstream BI reports, and data teams typically lack end-to-end lineage to prove exactly where records were dropped."*
* **Solution & Impact:** *"I built a Medallion DAG pipeline featuring an **Automated Data Quality Gate**. Any record violating schema or null constraints is instantly isolated into a **Quarantine Table** without halting downstream ingestion. In addition, I implemented a **Reconciliation Audit Engine** using key-level LEFT JOINs to verify zero data loss across layers in real time."*

### 🇹🇭 สคริปต์ภาษาไทย
* **ปัญหา:** *"ข้อมูลผิดรูปแบบ (Dirty Data) หลุดเข้าไปใน Dashboard โดยทีมไม่รู้ตัว และไม่มีระบบตรวจสอบว่ามีข้อมูลสูญหายระหว่างทางหรือไม่"*
* **วิธีแก้ & ผลลัพธ์:** *"ผมสร้าง Medallion Pipeline (Bronze ➔ Silver ➔ Gold) ที่มี **Automated Quality Gate** คอยดักจับ Schema และ Null หากพบแถวที่ผิดกฎจะส่งเข้า **Quarantine Table** ทันทีโดยไม่ทำให้ Pipeline ล่ม และมี **Audit Engine** ใช้ Key-Level LEFT JOIN ตรวจสอบยอดข้อมูลข้าม Layer แบบเรียลไทม์ 100% ครับ"*

---

## 🔬 6. Lab 5: Binary Serialization & Schema Evolution (DDIA Ch. 4)

* **Key Tech:** Protocol Buffers (Protobuf), Apache Avro, Apache Thrift, MessagePack, JSON, FastAPI Simulation

### 🇬🇧 English Script
* **Problem:** *"JSON payloads carry huge network overhead due to repetitive string field keys, and continuous rolling deployments frequently break production when forward/backward schema rules are violated."*
* **Solution & Impact:** *"I benchmarked byte-level encodings across JSON, MessagePack, Thrift, Protobuf, and Avro—demonstrating that **Protobuf and Avro cut payload sizes by over 60%** using field tags and varint compression. I also developed interactive simulations for **backward and forward compatibility**, validating rules for adding/deprecating fields to ensure safe **zero-downtime rolling upgrades**."*

### 🇹🇭 สคริปต์ภาษาไทย
* **ปัญหา:** *"การส่งข้อมูลด้วย JSON เปลือง Bandwidth สูงมากเพราะต้องแนบชื่อฟิลด์ซ้ำๆ และการ Deploy แบบ Rolling Upgrade มักทำให้ระบบล่มถ้าจัดการความเข้ากันได้ของ Schema ไม่ดี"*
* **วิธีแก้ & ผลลัพธ์:** *"ผมผ่าขนาดไบต์เปรียบเทียบจริง พบว่า **Protobuf และ Avro ลดขนาด Payload ได้มากกว่า 60%** ด้วย Field Tags และ Varint พร้อมทั้งจำลองกฎ **Backward / Forward Compatibility** เพื่อพิสูจน์การทำ **Zero-Downtime Deployment** โดยที่ระบบเก่าและใหม่ทำงานร่วมกันได้โดยไม่สะดุดครับ"*

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
