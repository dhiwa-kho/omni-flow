# 📘 คู่มือ Lab 4: Workflow Orchestration, Automated Data Quality Gate & Medallion Architecture

ชุดทดลองการควบคุมลำดับงาน (Workflow Orchestration DAG สไตล์ Airflow / Dagster) เพื่อทำ **Data Quality Gate (คัดแยกข้อมูลขยะไปกักกันใน Quarantine)**, **Batch Reconciliation**, และสร้างตาราง **Gold Executive Mart** บน ClickHouse

---

## 1. วิธีเปิดรันระบบ (เมื่อเอาไปเล่นที่บ้าน)

```powershell
cd c:\Omni-Flow\lab4-orchestration-audit
docker compose up -d
```

จากนั้นเปิด Terminal 2 หน้าต่างรัน:
* **หน้าต่างที่ 1 (ตัวดึง Stream เข้าตาราง Bronze):**
  ```powershell
  python stream_ingester.py
  ```
* **หน้าต่างที่ 2 (ตัวรัน Orchestrator DAG + Web UI พอร์ต 8080):**
  ```powershell
  python -m uvicorn orchestrator_app:app --host 0.0.0.0 --port 8080
  ```
  👉 เปิดเบราว์เซอร์ไปที่: **http://localhost:8080**

---

## 2. วิธีทดสอบบนหน้าเว็บ Orchestrator UI (`http://localhost:8080`)

1. **ดูการทำงานของ 4-Step DAG อัตโนมัติ:**
   * ทุกๆ 15 วินาที (หรือเมื่อกดปุ่ม **`▶️ Trigger DAG Run Now`**) คุณจะเห็นกล่อง `STEP 1 ──► STEP 2 ──► STEP 3 ──► STEP 4` สว่างขึ้นทีละขั้นตามลำดับ
2. **ทดสอบฉีดข้อมูลขยะ (`☠️ Inject 5 Poison Rows (Bad Data)`):**
   * กดปุ่ม **`☠️ Inject 5 Poison Rows (Bad Data)`** เพื่อแกล้งยิงข้อมูลที่อุณหภูมิเพี้ยน (`-999.9°C`) และยอดเงินติดลบ (`-50,000 บาท`) เข้าไปในตาราง `bronze_raw_events`
   * จากนั้นกดปุ่ม **`▶️ Trigger DAG Run Now`**
   * สังเกตที่ตาราง **Quarantine / Dead-Letter Vault** ด้านล่าง: ขั้นตอน `STEP 2 (Data Quality Gate)` จะคัดแยกข้อมูลขยะทั้ง 5 แถวออกมาขังไว้พร้อมบอกสาเหตุ (`TEMP_OUT_OF_RANGE | NEGATIVE_REVENUE`) โดยไม่ปล่อยให้ไหลเข้าตาราง **Gold Executive Mart**
   * **ข้อสังเกตเชิงสถาปัตยกรรม (DDIA Ch. 10 & 12):** สมการกระทบยอด `Bronze Processed = Silver Verified + Quarantine Rejected` ใช้ตรวจสอบความครบถ้วนของชุดข้อมูลที่ถูกดึงมาประมวลผลในแต่ละรอบ (`run_id`) ภายใต้เงื่อนไขว่าทุกแถวในรอบนั้นถูกจำแนกเข้าสู่ Silver หรือ Quarantine อย่างใดอย่างหนึ่งชัดเจน

---

## 3. คำสั่ง SQL ตรวจสอบตาราง Medallion (Bronze / Silver / Quarantine / Gold) ใน DBeaver

เชื่อมต่อ **ClickHouse** ใน DBeaver (`Host: localhost`, `Port: 8123`, `Database: audit_olap`, `User: default`):

```sql
-- 1. ดูตาราง Gold Executive Mart (ข้อมูลที่ผ่านการกรอง Data Quality จากชั้น Silver)
SELECT * FROM audit_olap.gold_executive_mart FINAL ORDER BY plant;

-- 2. ดูตารางกักกันข้อมูลเสีย (Data Quality Quarantine Vault) ว่าถูกตีตกเพราะสาเหตุอะไรบ้าง
SELECT
    dag_run_id,
    event_id,
    plant,
    device_id,
    temperature,
    revenue_thb,
    dq_failure_reason,
    quarantined_at
FROM audit_olap.quarantine_bad_rows
ORDER BY quarantined_at DESC;

-- 3. ดูประวัติการรัน DAG และการกระทบยอด (Bronze Read == Silver Clean + Quarantined)
SELECT * FROM audit_olap.dag_run_history ORDER BY run_id DESC;

-- 4. [DDIA Ch. 12 Key-Level LEFT JOIN Reconciliation] ตรวจหา Missing / Extra / Mismatched Records ราย Event ID
SELECT
    count() AS total_bronze_events,
    countIf(s.event_id != '') AS matched_in_silver,
    countIf(q.event_id != '') AS matched_in_quarantine,
    countIf(s.event_id = '' AND q.event_id = '') AS missing_records,
    countIf(s.event_id != '' AND abs(b.revenue_thb - s.revenue_thb) > 0.01) AS mismatched_amount_records
FROM (SELECT event_id, revenue_thb FROM audit_olap.bronze_raw_events FINAL) AS b
LEFT JOIN (SELECT event_id, revenue_thb FROM audit_olap.silver_clean_events FINAL) AS s ON b.event_id = s.event_id
LEFT JOIN (SELECT event_id FROM audit_olap.quarantine_bad_rows) AS q ON b.event_id = q.event_id;
```

