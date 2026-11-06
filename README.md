# 🌊 Omni-Flow: Designing Data-Intensive Applications (DDIA) Hands-on Architecture Labs

ชุดการทดลองสถาปัตยกรรมระบบข้อมูลแบบลงมือปฏิบัติจริง (**5 Hands-on Modern Data Stack Labs**) ที่เชื่อมโยงแนวคิดจากหนังสือ ***Designing Data-Intensive Applications (DDIA)*** โดย Martin Kleppmann ทั้ง 12 บท เข้ากับเทคโนโลยีระดับ Production ได้แก่ **Apache Kafka (KRaft)**, **Apache Flink**, **ClickHouse**, **PostgreSQL + Debezium CDC**, **MinIO S3 + Apache Parquet**, และ **Medallion DAG Orchestration**

👉 **เริ่มต้นอ่านคู่มือทฤษฎีและแนวทางเตรียมสัมภาษณ์ทั้ง 12 บทได้ที่:**
* 📕 **[docs/DDIA_MASTER_GUIDE.md](docs/DDIA_MASTER_GUIDE.md)** — คู่มือเชื่อมโยง DDIA ทั้ง 12 บทเข้ากับ Omni-Flow Labs

---

## 🏛️ แผนผังทั้ง 5 โปรเจกต์ใน Repository นี้

| โฟลเดอร์โปรเจกต์ | พอร์ต Web UI | สถาปัตยกรรมและเทคโนโลยีหลัก | บทในหนังสือ DDIA ที่เกี่ยวข้อง | คู่มือการรันและทดสอบ |
| :--- | :--- | :--- | :--- | :--- |
| **[`omni-command-center`](omni-command-center)** | `http://localhost:8000` | **Kappa Architecture + Chaos & Partition Lab:** 3-Process (`producer.py`, `consumer.py`, `dashboard.py`) + Kafka 3 Partitions + ClickHouse `ReplacingMergeTree` + Latency Percentiles (`p50/p95/p99`) | **Ch. 1, 5, 6, 8, 9, 11, 12** | [📘 TESTING_GUIDE.md](omni-command-center/TESTING_GUIDE.md) |
| **[`lab1-flink-stream`](lab1-flink-stream)** | `http://localhost:8081` | **Distributed Stream Engine:** Apache Flink 1.19 Cluster (`JobManager` + `TaskManager`) + Flink SQL (`Event Time` + `5s Watermark` + `10s Tumbling Window`) + ClickHouse | **Ch. 11** | [⚡ FLINK_TESTING_GUIDE.md](lab1-flink-stream/FLINK_TESTING_GUIDE.md) |
| **[`lab2-minio-lakehouse`](lab2-minio-lakehouse)** | `http://localhost:9001` | **Hot/Cold Data Lakehouse:** Kafka ──► ClickHouse (Hot Tier) + **MinIO S3** (Cold Tier `.parquet` files) + Snapshot Metadata Catalog สำหรับทดสอบ Time-Travel Query | **Ch. 3, 4, 10, 12** | [🏛️ LAKEHOUSE_TESTING_GUIDE.md](lab2-minio-lakehouse/LAKEHOUSE_TESTING_GUIDE.md) |
| **[`lab3-debezium-cdc`](lab3-debezium-cdc)** | `http://localhost:8083` | **Log-Based Change Data Capture (CDC) & Outbox:** PostgreSQL 16 (`wal_level=logical` + `outbox_events`) ──► Debezium Connect ──► Kafka ──► ClickHouse (`ReplacingMergeTree`) | **Ch. 3, 5, 7, 11** | [🔄 CDC_TESTING_GUIDE.md](lab3-debezium-cdc/CDC_TESTING_GUIDE.md) |
| **[`lab4-orchestration-audit`](lab4-orchestration-audit)** | `http://localhost:8080` | **Medallion DAG Orchestrator & Data Quality Gate:** Kafka ──► Bronze Raw ──► **Automated DQ Gate** (`Silver Clean` vs `Quarantine Bad Rows`) ──► Gold Mart + Key-Level `LEFT JOIN` Audit | **Ch. 10, 12** | [🎛️ ORCHESTRATION_TESTING_GUIDE.md](lab4-orchestration-audit/ORCHESTRATION_TESTING_GUIDE.md) |

---

## 🚀 ข้อแนะนำในการรันแต่ละ Lab บนเครื่อง Local

1. **ติดตั้งไลบรารี Python พื้นฐาน (ครั้งเดียว):**
   ```powershell
   pip install -r omni-command-center/requirements.txt
   ```
2. **รันทีละ Lab เพื่อประหยัด RAM และป้องกันพอร์ตชนกัน (`9092` และ `8123`):**
   * ก่อนสลับไปเปิด Lab ถัดไป ให้สั่งปิด Docker ของ Lab เดิมด้วยคำสั่ง:
     ```powershell
     docker compose down
     ```
3. **เชื่อมต่อ DBeaver เพื่อตรวจสอบข้อมูลด้วย SQL:**
   * **ClickHouse (ทุก Lab):** `Host: localhost`, `Port: 8123`, `User: default` (ไม่มีรหัสผ่าน)
   * **PostgreSQL (เฉพาะ Lab 3):** `Host: localhost`, `Port: 5432`, `Database: shop_oltp`, `User: postgres`, `Password: password123`
