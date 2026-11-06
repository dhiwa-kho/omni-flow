# 📘 คู่มือการรันและทดสอบระบบ: The Real-Time Stream & OLAP Command Center (100% DDIA Edition)

โปรเจกต์จำลองสถาปัตยกรรม **Real-Time Data Streaming & OLAP Analytics (Kappa Architecture)** ด้วย **Apache Kafka (KRaft Mode)** + **Python Stateful Stream Worker** + **ClickHouse (`ReplacingMergeTree`)** ออกแบบตามหลักการในหนังสือ *Designing Data-Intensive Applications (DDIA)* สำหรับการทดสอบและสาธิตระดับ System Architect

---

## 1. โครงสร้างไฟล์ในโปรเจกต์ (`omni-command-center`)

```text
omni-command-center/
├── docker-compose.yml       # รัน Kafka (KRaft พอร์ต 9092) + ClickHouse (พอร์ต 8123 / 9000)
├── init-db.sql              # สคีมาเริ่มต้นของ ClickHouse
├── requirements.txt         # ไลบรารี Python ที่ต้องใช้
├── producer.py              # [โปรแกรมที่ 1] ตัวส่งข้อมูล 10 เครื่องจักร (SENSOR-001..010) เข้า Kafka (3 Partitions)
├── consumer.py              # [โปรแกรมที่ 2] Stateful Stream Processor (Join + CEP + 5s Window) -> ClickHouse
├── dashboard.py             # [โปรแกรมที่ 3] เว็บ Command Center Monitor (พอร์ต 8000)
├── static/
│   └── index.html           # หน้าจอ UI 3 แถว (Top / Middle / Bottom)
└── TESTING_GUIDE.md         # คู่มือการติดตั้งและทดสอบระบบฉบับพกพา
```

---

## 2. วิธีเปิดรันระบบตั้งแต่ต้น (เมื่อย้ายไปรันเครื่องที่บ้าน)

### ขั้นตอนที่ 2.1: เปิด Docker (Kafka + ClickHouse)
เปิดโปรแกรม **Docker Desktop** ให้พร้อม แล้วเปิด Terminal เข้าไปที่โฟลเดอร์โปรเจกต์:
```powershell
cd c:\Omni-Flow\omni-command-center
docker compose up -d
```

### ขั้นตอนที่ 2.2: ติดตั้งไลบรารี Python
```powershell
pip install -r requirements.txt
```

### ขั้นตอนที่ 2.3: เปิดรันแยก 3 หน้าต่าง Terminal (Decoupled Processes)
ให้เปิด PowerShell / Terminal แยกกัน 3 หน้าต่างในโฟลเดอร์ `c:\Omni-Flow\omni-command-center`:

* **หน้าต่างที่ 1 — รันหน้าเว็บ Command Center Dashboard:**
  ```powershell
  python -m uvicorn dashboard:app --host 0.0.0.0 --port 8000
  ```
  👉 เปิดเบราว์เซอร์ไปที่: **http://localhost:8000**

* **หน้าต่างที่ 2 — รันตัวส่งข้อมูล (Standalone Producer):**
  ```powershell
  python producer.py
  ```

* **หน้าต่างที่ 3 — รันตัวประมวลผลสตรีม (Stateful Stream Worker / Consumer):**
  ```powershell
  python consumer.py
  ```

---

## 3. วิธีทดสอบ 5 Scenarios บนหน้า Dashboard (`http://localhost:8000`)

### 🟢 Scenario 1: Happy Path (Entity Key & Low Latency)
* **วิธีทดสอบ:** กดปุ่ม `🟢 1. Happy Path (Entity Key)` บนแถบด้านบน
* **หลักการ DDIA:** ใช้ `key = device_id` เพื่อให้ข้อมูลของเครื่องจักรตัวเดียวกันลง Partition เดิมเสมอ (รักษา Causal Ordering รายเครื่อง)
* **สิ่งที่ต้องสังเกต:**
  * ความเร็ว `Producer TPS` และ `Consumer TPS` วิ่งสมดุลกันที่ ~35–40 TPS
  * ค่า `Total Lag` เป็น `0` (หรือใกล้ 0)
  * สัดส่วนข้อมูลใน `P0`, `P1`, `P2` กระจายสมดุลกัน

---

### ⚡ Scenario 2: Network Gap & Offset Recovery (The 3-Second Drop)
* **วิธีทดสอบ:**
  * **วิธีที่ 1 (กดจากหน้าเว็บ):** กดปุ่ม `⚡ 2. Network Gap (3s Drop)`
  * **วิธีที่ 2 (ทุบโปรเซสจริง):** ไปที่หน้าต่าง Terminal ของ `consumer.py` แล้วกด `Ctrl + C` เพื่อปิด Consumer ทิ้งสัก 5 วินาที แล้วรัน `python consumer.py` ใหม่
* **หลักการ DDIA:** Kafka ทำหน้าที่เป็น Durable Log Buffer เก็บข้อมูลไว้ตามลำดับ Offset เมื่อ Consumer กลับมาออนไลน์จะอ่านต่อจาก `Last Committed Offset` ทันที
* **สิ่งที่ต้องสังเกต:**
  * ขณะที่ Consumer หยุดทำงาน กราฟสีส้ม (`Consumer Lag`) จะพุ่งสูงขึ้นเรื่อยๆ และสถานะเปลี่ยนเป็น `⚠️ NETWORK GAP (BUFFERING IN KAFKA)`
  * เมื่อครบ 3 วินาที (หรือเปิด Consumer ใหม่) สถานะจะเปลี่ยนเป็น `🔄 REPLAYING BACKLOG` ดูดข้อมูลที่ค้างท่อทั้งหมดเข้า ClickHouse จน `Lag = 0` และในช่อง Log จะขึ้น `✅ [OFFSET RECOVERY] Replayed ... events with 0 loss!`

---

### 🔁 Scenario 3: Idempotency & Sink Deduplication Testing (ส่งซ้ำแต่ยอดปลายทางไม่เบิ้ล)
* **วิธีทดสอบ:** กดปุ่ม `🔁 3. Idempotency (Dup Retry)` (หรือกดปุ่ม `+ Inject 10 Manual Duplicates Now`)
* **หลักการ DDIA & ขอบเขตทางเทคนิค:**
  * ฝั่ง Producer เปิด `enable.idempotence=True` และ `acks=all` (หมายเหตุ: `acks=all` รอการตอบรับจากกลุ่ม ISR ปัจจุบัน ในระบบจริงต้องตั้งควบคู่กับ `min.insync.replicas` และ Replication Factor ที่เหมาะสม ส่วน KRaft ใช้ทำ Consensus เฉพาะ Metadata ของคลัสเตอร์ ไม่ใช่การเขียนข้อมูลทุกข้อความด้วย Raft)
  * ในโหมดนี้ `producer.py` จะจงใจสร้างข้อความซ้ำ (`event_id` เดิมซ้ำอีก 2 รอบ) ในระดับ Application เข้าไปใน Kafka ทุกๆ 5 รายการ เพื่อจำลองเหตุการณ์ Timeout แล้วเกิดการ Retry ซ้ำ
  * ฝั่ง ClickHouse ใช้ตาราง `ReplacingMergeTree(ingested_at) ORDER BY (event_id)` เวลาคิวรีด้วยคีย์เวิร์ด `FINAL` ระบบจะยุบรวมแถวที่ `event_id` ซ้ำกันให้เหลือเพียงแถวเดียวตอนคิวรี (ถือเป็นการทำ **Sink Deduplication / Effectively-Once ภายใต้คีย์ที่กำหนด** ไม่ใช่การทำ Distributed Transaction ข้ามระบบ)
* **สิ่งที่ต้องสังเกต:**
  * ในตาราง 10 เครื่องด้านล่าง ช่อง **`Raw`** (จำนวนแถวดิบก่อนยุบรวม) จะสูงกว่าช่อง **`FINAL`**
  * ช่อง **`Dups`** จะแสดงจำนวนข้อมูลซ้ำที่ถูกกรองออกของแต่ละเครื่อง (`SENSOR-001`..`010`)
  * เมื่อกดปุ่ม `⏸️ Pause (Audit)` เพื่อหยุดตรวจยอด จะเห็นว่า `Source Sent` เท่ากับ `ClickHouse (FINAL)` (`Diff = 0`, ขึ้นป้าย `✅ RECONCILED 100% MATCH`)

---

### 🔥 Scenario 4: High Load / Stress Test (1,500+ TPS Burst)
* **วิธีทดสอบ:** กดปุ่ม `🔥 4. Stress Test (1,500+ TPS)`
* **หลักการ DDIA:** ทดสอบ High-Throughput Batching ทั้งฝั่ง Producer (`batch.num.messages`) และฝั่ง Consumer (`consume(400)`) โดยควรสังเกตทั้ง Throughput (TPS), Consumer Lag และการใช้ CPU/RAM ควบคู่กัน
* **สิ่งที่ต้องสังเกต:**
  * ตัวเลข `Producer TPS` และ `Consumer TPS` พุ่งขึ้นไปที่ระดับ **1,000 – 2,000+ TPS** (ขึ้นอยู่กับสเปกเครื่องที่รัน)
  * ดูค่าทรัพยากรเครื่องที่มุมขวาบน **`Host CPU / RAM (Stress Monitor)`** ว่ากิน CPU และ RAM เพิ่มขึ้นเท่าไหร่

---

### ⚖️ Scenario 5: Partitioning, Data Skew & Key Salting (DDIA Chapter 6)
* **วิธีทดสอบ:**
  1. กดปุ่ม `⚖️ 5A. Data Skew (Hot-Spot P0)`: บังคับให้ข้อมูล 90% ไหลเข้า `Partition 0` พาร์ติชันเดียว (จำลองปัญหา Hot Key / Celebrity Sensor) -> สังเกตหลอดของ **`P0`** ในกล่อง `DDIA Partition Skew & ISR Status` จะเปลี่ยนเป็น **สีส้ม** และสัดส่วน `%` ของ `P0` จะพุ่งขึ้นเรื่อยๆ
  2. กดปุ่ม `🧂 5B. Key Salting (Balanced)`: เติม Salt (`device_id#salt-0..2`) ต่อท้าย Key เพื่อกระจายโหลดลง `P0, P1, P2` เท่าๆ กัน -> สังเกตสัดส่วนของ `P0, P1, P2` ค่อยๆ เกลี่ยกลับมาสมดุลกัน *(ข้อควรระวังในการสัมภาษณ์: Key Salting ช่วยกระจายโหลดการเขียนและการประมวลผลขนาน แต่แลกกับการที่ข้อมูลของ `device_id` เดียวกันกระจายไปอยู่หลาย Partition ทำให้เสียการรับประกันลำดับเหตุการณ์ภายในอุปกรณ์เดียวกัน และต้องนำมารวมผลภายหลัง)*
  3. **ทดสอบ Consumer Rebalancing:** ลองเปิด Terminal หน้าต่างที่ 4 แล้วรัน `python consumer.py` เพิ่มอีกตัว จะเห็น Log `⚖️ [DDIA REBALANCE - ASSIGNED]` แบ่งพาร์ติชัน `P0, P1, P2` ให้ Consumer 2 ตัวช่วยกันอ่านทันที!

---

## 4. การเชื่อมต่อ DBeaver และคำสั่ง SQL สำหรับตรวจสอบข้อมูล

### การตั้งค่า DBeaver (ClickHouse Connection)
* **Host:** `localhost`
* **Port:** `8123`
* **Database:** `omni_olap`
* **Username:** `default` *(Password เว้นว่าง)*

### คำสั่ง SQL สำหรับรันตรวจสอบใน DBeaver

```sql
-- 1. ตรวจกระทบยอด (Batch Reconciliation): ต้นทาง vs ClickHouse Raw vs ClickHouse FINAL
SELECT
    (SELECT count() FROM omni_olap.source_audit_kappa FINAL) AS source_sent_count,
    (SELECT count() FROM omni_olap.sensor_events_kappa) AS clickhouse_raw_rows,
    (SELECT count() FROM omni_olap.sensor_events_kappa FINAL) AS clickhouse_final_unique_rows,
    clickhouse_raw_rows - clickhouse_final_unique_rows AS duplicates_filtered_out,
    source_sent_count - clickhouse_final_unique_rows AS missing_gap_diff;

-- 2. ตรวจสัดส่วนข้อมูลในแต่ละ Partition (DDIA Chapter 6: Partition Skew Check)
SELECT
    kafka_partition,
    count() AS raw_messages,
    count(DISTINCT event_id) AS unique_events,
    round(count() * 100.0 / sum(count()) OVER (), 2) AS share_pct
FROM omni_olap.sensor_events_kappa
GROUP BY kafka_partition
ORDER BY kafka_partition;

-- 3. ตรวจผลลัพธ์ Stateful Stream Processing (5-Second Tumbling Window)
SELECT
    window_start,
    window_end,
    device_id,
    device_model,
    event_count,
    min_temp,
    max_temp,
    total_cost_thb,
    cep_alerts
FROM omni_olap.windowed_5s_olap FINAL
ORDER BY window_start DESC, device_id ASC
LIMIT 20;

-- 4. ตรวจเหตุการณ์ความร้อนสูงต่อเนื่องจากการทำ Stateful CEP (CRITICAL_STREAK)
SELECT
    event_id,
    event_time,
    device_id,
    device_model,
    temperature,
    cost_thb,
    cep_status,
    kafka_partition,
    kafka_offset
FROM omni_olap.sensor_events_kappa FINAL
WHERE cep_status = 'CRITICAL_STREAK'
ORDER BY seq_no DESC
LIMIT 20;
```

