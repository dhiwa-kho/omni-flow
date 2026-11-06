# คู่มือเชื่อมโยง DDIA กับ Omni-Flow Labs ฉบับปรับปรุง

เอกสารฉบับนี้สรุปแนวคิดจากทั้ง 12 บทของ *Designing Data-Intensive Applications* (DDIA) และจับคู่กับการทดลองในโปรเจกต์ Omni-Flow ตามรายละเอียดที่ให้มา ใช้เป็นแผนทบทวนและแนวทางเตรียมสัมภาษณ์ได้ แต่ไม่ใช่สิ่งทดแทนหนังสือหรือหลักฐานยืนยันว่าโปรเจกต์ทำงานตามคำอธิบายทุกประการ—ควรตรวจสอบโค้ด การตั้งค่า และผลทดสอบจริงประกอบ

## ภาพรวม DDIA ทั้ง 12 บท

| บท | หัวข้อ | คำถามหลัก | การเชื่อมโยงกับ Omni-Flow |
|---|---|---|---|
| 1 | Reliable, Scalable, and Maintainable Applications | ระบบเชื่อถือได้ วัดผลได้ และขยายได้อย่างไร | Command Center: ทดสอบการขัดข้องและโหลด |
| 2 | Data Models and Query Languages | เลือกแบบจำลองข้อมูลและภาษา query อย่างไร | PostgreSQL, ClickHouse, event payloads และ data mart |
| 3 | Storage and Retrieval | โครงสร้างจัดเก็บและดัชนีส่งผลต่อ workload อย่างไร | PostgreSQL, ClickHouse, Parquet |
| 4 | Encoding and Evolution | เปลี่ยน schema โดยให้ระบบหลายรุ่นทำงานร่วมกันได้อย่างไร | Kafka payloads และ Parquet |
| 5 | Replication | ทำสำเนาข้อมูลและรับมือ replica ที่ตามไม่ทันอย่างไร | Kafka replication, ISR และ producer acknowledgments |
| 6 | Partitioning | กระจายข้อมูลและโหลดโดยยังตอบโจทย์การ query อย่างไร | Kafka partitions, hot key และ key salting |
| 7 | Transactions | รักษาความถูกต้องเมื่อมีการอ่านและเขียนพร้อมกันอย่างไร | PostgreSQL transactions และการเขียนข้อมูลปลายทาง |
| 8 | The Trouble with Distributed Systems | รับมือความล่าช้า ความล้มเหลว และเวลาที่คลาดเคลื่อนอย่างไร | Network gap, retry และ consumer recovery |
| 9 | Consistency and Consensus | นิยามความสอดคล้องและเลือกผู้นำในระบบกระจายอย่างไร | Kafka KRaft และความหมายของ consistency |
| 10 | Batch Processing | ประมวลผลข้อมูลจำนวนมากจากข้อมูลนำเข้าที่กำหนดชัดเจนอย่างไร | Lab 4, Bronze data และการประมวลผลซ้ำ |
| 11 | Stream Processing | คำนวณจากเหตุการณ์ที่ไหลเข้ามาต่อเนื่องอย่างไร | Flink, Kafka และ Debezium CDC |
| 12 | The Future of Data Systems | ผสานระบบข้อมูลหลายแบบให้ได้ผลลัพธ์ที่ถูกต้องและตรวจสอบได้อย่างไร | Medallion pipeline, DQ gate และ reconciliation |

## สถาปัตยกรรมโปรเจกต์ตามข้อมูลที่ให้มา

| โปรเจกต์ | จุดประสงค์และเทคโนโลยีหลัก | ไฟล์โค้ดและ Config ที่ใช้ตรวจสอบจริง | บท DDIA ที่เกี่ยวข้อง |
|---|---|---|---|
| [`omni-command-center`](file:///c:/Omni-Flow/omni-command-center) | Kafka, producer/consumer, ClickHouse และแดชบอร์ดสำหรับทดสอบ fault, throughput, partition skew และ reconciliation | [`docker-compose.yml`](file:///c:/Omni-Flow/omni-command-center/docker-compose.yml), [`producer.py`](file:///c:/Omni-Flow/omni-command-center/producer.py), [`consumer.py`](file:///c:/Omni-Flow/omni-command-center/consumer.py), [`dashboard.py`](file:///c:/Omni-Flow/omni-command-center/dashboard.py) | 1, 5, 6, 8, 9, 11, 12 |
| [`lab1-flink-stream`](file:///c:/Omni-Flow/lab1-flink-stream) | Apache Flink สำหรับกรองและสรุปเหตุการณ์แบบ stream ก่อนส่งต่อ | [`docker-compose.yml`](file:///c:/Omni-Flow/lab1-flink-stream/docker-compose.yml), [`flink_job.sql`](file:///c:/Omni-Flow/lab1-flink-stream/flink_job.sql), [`payment_producer.py`](file:///c:/Omni-Flow/lab1-flink-stream/payment_producer.py), [`clickhouse_sink.py`](file:///c:/Omni-Flow/lab1-flink-stream/clickhouse_sink.py) | 11 |
| [`lab2-minio-lakehouse`](file:///c:/Omni-Flow/lab2-minio-lakehouse) | Kafka, ClickHouse และ MinIO สำหรับจัดเก็บข้อมูลแบบ Parquet และทดลอง snapshot | [`docker-compose.yml`](file:///c:/Omni-Flow/lab2-minio-lakehouse/docker-compose.yml), [`lakehouse_producer.py`](file:///c:/Omni-Flow/lab2-minio-lakehouse/lakehouse_producer.py), [`lakehouse_sink_worker.py`](file:///c:/Omni-Flow/lab2-minio-lakehouse/lakehouse_sink_worker.py) | 3, 4, 10, 12 |
| [`lab3-debezium-cdc`](file:///c:/Omni-Flow/lab3-debezium-cdc) | PostgreSQL WAL → Debezium → Kafka → ClickHouse | [`docker-compose.yml`](file:///c:/Omni-Flow/lab3-debezium-cdc/docker-compose.yml), [`init-postgres.sql`](file:///c:/Omni-Flow/lab3-debezium-cdc/init-postgres.sql), [`register_connector.py`](file:///c:/Omni-Flow/lab3-debezium-cdc/register_connector.py), [`cdc_sink_worker.py`](file:///c:/Omni-Flow/lab3-debezium-cdc/cdc_sink_worker.py) | 3, 5, 7, 11 |
| [`lab4-orchestration-audit`](file:///c:/Omni-Flow/lab4-orchestration-audit) | DAG, Bronze/Silver/Gold, data quality checks, quarantine และ reconciliation | [`docker-compose.yml`](file:///c:/Omni-Flow/lab4-orchestration-audit/docker-compose.yml), [`stream_ingester.py`](file:///c:/Omni-Flow/lab4-orchestration-audit/stream_ingester.py), [`orchestrator_app.py`](file:///c:/Omni-Flow/lab4-orchestration-audit/orchestrator_app.py) | 10, 12 |

การจับคู่ข้างต้นเป็นการจับคู่ตามคำอธิบายของโปรเจกต์ ไม่ได้หมายความว่า lab หนึ่งชุดครอบคลุมทฤษฎีทั้งบทโดยสมบูรณ์

---

## บทที่ 1: Reliable, Scalable, and Maintainable Applications

### แนวคิดหลัก

- **Reliability:** ระบบทำงานได้ถูกต้อง แม้บางส่วนเกิดปัญหา
- **Fault กับ failure:** fault คือความผิดปกติขององค์ประกอบหนึ่ง ส่วน failure คือผู้ใช้ได้รับผลกระทบจนระบบให้บริการไม่ได้ตามที่คาดหวัง
- **Scalability:** ความสามารถในการรองรับโหลดที่เพิ่มขึ้น ไม่ได้หมายถึงการเพิ่มเครื่องเพียงอย่างเดียว
- **Maintainability:** ระบบเข้าใจ ปรับปรุง และดูแลได้โดยไม่ทำให้เปลี่ยนแปลงยากเกินไป
- การวัดประสิทธิภาพควรดูทั้ง throughput และ latency โดย latency มักรายงานเป็น percentile เช่น p50, p95 และ p99 ไม่ใช่ดูแค่ค่าเฉลี่ย

### เชื่อมโยงกับ Omni-Flow

- **Network gap:** จำลองช่วงที่ consumer หรือปลายทางหยุดรับข้อมูล แล้วตรวจว่า producer และ Kafka ทำงานต่อได้หรือไม่ (ตรวจสอบโค้ดจำลองใน [`consumer.py`](file:///c:/Omni-Flow/omni-command-center/consumer.py))
- **Stress test:** เพิ่มอัตราข้อมูลและติดตาม throughput, latency, consumer lag, CPU และหน่วยความจำ (ตรวจสอบการปรับ `sleep_sec` และ batching ใน [`producer.py`](file:///c:/Omni-Flow/omni-command-center/producer.py))
- แยกให้ชัดระหว่าง “ระบบยังรับข้อมูลได้” กับ “ระบบประมวลผลข้อมูลทันและส่งผลลัพธ์ถูกต้อง”

### คำถามสำหรับสัมภาษณ์

- ถ้า throughput เพิ่มขึ้น แต่ p99 latency แย่ลง จะตรวจสอบจุดใดก่อน?
- ระบบมี backpressure หรือการจำกัดโหลดอย่างไร?
- วัดอย่างไรว่า fault หนึ่งจุดไม่ลุกลามเป็น failure ทั้งระบบ?

---

## บทที่ 2: Data Models and Query Languages

### แนวคิดหลัก

- **Relational model:** เหมาะกับข้อมูลที่มีโครงสร้าง ความสัมพันธ์ และข้อกำหนดความถูกต้อง เช่น PostgreSQL
- **Document model:** เก็บข้อมูลที่มีโครงสร้างซ้อนหรือเปลี่ยนแปลงบ่อยได้สะดวก แต่ต้องพิจารณาการเชื่อมโยงข้อมูลและการทำซ้ำข้อมูล
- **Graph model:** เหมาะกับข้อมูลที่ความสัมพันธ์ระหว่าง entity เป็นหัวใจของการ query
- การเลือก data model ขึ้นกับรูปแบบ query และการเปลี่ยนแปลงข้อมูล ไม่ใช่เลือกจากความนิยมของฐานข้อมูล
- **Declarative query** ระบุผลลัพธ์ที่ต้องการ ส่วนระบบเลือกวิธีดำเนินการ เช่น SQL; ต่างจากการระบุขั้นตอนการทำงานโดยตรง

### เชื่อมโยงกับ Omni-Flow

- ใช้ PostgreSQL สำหรับธุรกรรมและข้อมูลที่ต้องอาศัย constraints/transactions ([`lab3-debezium-cdc/init-postgres.sql`](file:///c:/Omni-Flow/lab3-debezium-cdc/init-postgres.sql))
- ใช้ ClickHouse สำหรับ query เชิงวิเคราะห์ โดยออกแบบตารางและ sorting key ให้ตรงกับรูปแบบการอ่าน
- กำหนดรูปแบบ event ใน Kafka ให้มีโครงสร้างและนิยามความหมายของ field ชัดเจน
- อธิบายเหตุผลที่เลือกแต่ละ data model ด้วย workload จริง เช่น อ่านข้อมูลรายอุปกรณ์ สรุปตามเวลา หรือค้นหาธุรกรรมรายรายการ

### สิ่งที่ควรทดสอบใน lab

- เปรียบเทียบ query แบบค้นทีละรายการกับ query สรุปข้อมูลจำนวนมาก
- ทดลองว่าการเปลี่ยนรูปแบบ event ส่งผลต่อ producer, consumer และ query ปลายทางอย่างไร
- ระบุให้ชัดว่าข้อมูลใดเป็น source of truth และข้อมูลใดเป็น derived data

---

## บทที่ 3: Storage and Retrieval

### แนวคิดหลัก

- **B-tree:** โครงสร้างดัชนีที่ใช้แพร่หลายใน relational database เหมาะกับการค้นหาและปรับปรุงข้อมูลตาม key หลายรูปแบบ
- **LSM-tree:** ใช้การเขียนข้อมูลเป็นลำดับและการรวมส่วนข้อมูลภายหลัง พบได้ใน storage engine หลายชนิด มี trade-off ระหว่างงานเขียน งานอ่าน และ compaction
- **Column-oriented storage:** เก็บข้อมูลตามคอลัมน์ เหมาะกับการอ่านบางคอลัมน์เพื่อคำนวณเชิงวิเคราะห์
- ดัชนีมีต้นทุนด้านพื้นที่และการเขียน จึงควรเลือกจากรูปแบบ query ที่ต้องรองรับจริง

### เชื่อมโยงกับ Omni-Flow

- PostgreSQL เป็นตัวอย่างฐานข้อมูลสำหรับ transactional workload และ row-level operations
- ClickHouse `MergeTree` เหมาะกับ analytical workload และจัดเก็บข้อมูลเป็น parts ที่มีการ merge ตามกลไกของระบบ ไม่ควรสรุปว่าเป็น LSM-tree แบบเดียวกับ storage engine ทุกชนิด
- Parquet เป็นรูปแบบไฟล์ columnar ที่เหมาะกับการอ่านข้อมูลจำนวนมากบางคอลัมน์ ([`lab2-minio-lakehouse/lakehouse_sink_worker.py`](file:///c:/Omni-Flow/lab2-minio-lakehouse/lakehouse_sink_worker.py))
- การเปรียบเทียบ PostgreSQL, ClickHouse และ Parquet ควรใช้ข้อมูล ชุด query และสภาพแวดล้อมเดียวกัน

### ข้อควรระวัง

อย่าระบุว่า columnar ทำงานเร็วกว่า row-oriented “เป็นพันเท่า” เป็นข้อสรุปทั่วไป ความเร็วขึ้นกับขนาดข้อมูล การบีบอัด การออกแบบตาราง query และฮาร์ดแวร์

---

## บทที่ 4: Encoding and Evolution

### แนวคิดหลัก

ระบบที่มี producer และ consumer หลายรุ่นต้องเปลี่ยนรูปแบบข้อมูลโดยไม่ทำให้บริการหยุดชะงัก

- **Backward compatibility:** โค้ดรุ่นใหม่อ่านข้อมูลที่เขียนด้วย schema รุ่นเก่าได้
- **Forward compatibility:** โค้ดรุ่นเก่าอ่านข้อมูลจาก schema รุ่นใหม่ได้ ภายใต้เงื่อนไขที่กำหนด
- การเพิ่ม field ที่มีค่าเริ่มต้นอาจเข้ากันได้ง่ายกว่าการเปลี่ยนชนิดหรือลบ field ที่ใช้งานอยู่
- JSON อ่านง่ายแต่มี overhead และการตรวจ schema อาจไม่เข้มงวดเท่ารูปแบบที่กำหนด schema ชัดเจน
- Parquet มี metadata และ schema ของไฟล์ ส่วนการอ่านข้ามไฟล์หลาย schema ยังต้องพิจารณากลยุทธ์จัดการ schema เพิ่มเติม

### เชื่อมโยงกับ Omni-Flow

- เปรียบเทียบ payload ที่ส่งผ่าน Kafka กับไฟล์ Parquet ที่เก็บใน MinIO ([`lab2-minio-lakehouse/lakehouse_sink_worker.py`](file:///c:/Omni-Flow/lab2-minio-lakehouse/lakehouse_sink_worker.py))
- ทดลองเพิ่ม field ใหม่ โดยให้ consumer รุ่นเดิมและรุ่นใหม่ทำงานร่วมกัน
- กำหนดกติกาเรื่องการเปลี่ยนชื่อ ลบ หรือเปลี่ยนชนิด field
- Parquet เก็บ metadata เช่น schema และสถิติระดับ row group ซึ่ง query engine อาจใช้ช่วยเลือกข้อมูลที่ต้องอ่าน ทั้งนี้ขึ้นกับสถิติและความสามารถของ engine

---

## บทที่ 5: Replication

### แนวคิดหลัก

Replication ใช้ทำสำเนาข้อมูล เพื่อเพิ่มความพร้อมใช้งาน ความทนทาน และรองรับการอ่าน แต่ต้องจัดการกรณี replica ตามไม่ทันหรือมีผู้นำล้มเหลว

- **Leader-based replication:** leader รับการเขียน และ follower ทำสำเนาตาม
- **Synchronous/asynchronous replication:** มี trade-off ระหว่าง latency, availability และโอกาสสูญเสียข้อมูลเมื่อเกิดความล้มเหลว
- **Replication lag:** replica อาจยังไม่มีข้อมูลล่าสุด จึงทำให้การอ่านจาก replica เห็นข้อมูลเก่ากว่า
- การรับประกัน read-after-write หรือ monotonic reads ต้องพิจารณารูปแบบ replication และเส้นทางการอ่านจริง

### เชื่อมโยงกับ Kafka

- `acks=all` หมายถึง producer รอการตอบรับจาก replica ใน ISR ตามเงื่อนไขของ Kafka ขณะนั้น ไม่ได้หมายความว่า replica ทุกตัวในคลัสเตอร์ตอบรับ
- `min.insync.replicas` กำหนดจำนวนขั้นต่ำของ ISR ที่ต้องมีสำหรับการเขียนแบบ `acks=all` จึงช่วยกำหนด trade-off ระหว่างความพร้อมเขียนกับความทนทาน
- ISR คือกลุ่ม replica ที่ตาม leader ทันตามเกณฑ์ของ Kafka
- High watermark เป็นขอบเขตสำคัญของ log ที่ถือว่า replicated ตามกลไกของ Kafka; การมองเห็นข้อมูลและ transactional visibility ยังขึ้นกับโหมดอ่าน เช่น `read_committed`

### เชื่อมโยงกับ Omni-Flow

ตรวจสอบ replication factor, leader, ISR, `acks`, `min.insync.replicas` และพฤติกรรมเมื่อ replica หลุดจาก ISR โดยทดสอบกับคลัสเตอร์จริง (ดูการตั้งค่าใน [`omni-command-center/docker-compose.yml`](file:///c:/Omni-Flow/omni-command-center/docker-compose.yml) และ [`producer.py`](file:///c:/Omni-Flow/omni-command-center/producer.py)) ไม่สรุปว่า `acks=all` เพียงค่าเดียวรับประกัน zero data loss ได้ทุกกรณี

---

## บทที่ 6: Partitioning

### แนวคิดหลัก

Partitioning แบ่งข้อมูลออกเป็นส่วนเพื่อกระจายพื้นที่จัดเก็บและงานประมวลผล

- การเลือก partition key มีผลต่อการกระจายโหลดและการ query
- ข้อมูลที่ใช้ key เดียวกันมักถูกส่งไป partition เดียวกัน จึงรักษาลำดับของ key นั้นภายใน partition ได้
- ลำดับข้ามหลาย partition ไม่ได้รับประกันเป็นลำดับเดียวกัน
- **Hot key/data skew:** key บางตัวมีข้อมูลมากผิดปกติ ทำให้ partition หนึ่งกลายเป็นคอขวด
- **Key salting:** เติมส่วนประกอบเพิ่มเติมเพื่อกระจาย hot key แต่ทำให้ข้อมูล entity เดียวกระจายหลาย partition และอาจต้องรวมผลภายหลัง

### เชื่อมโยงกับ Omni-Flow

- **Entity key:** ใช้ `device_id` เมื่อจำเป็นต้องรักษาลำดับเหตุการณ์ของอุปกรณ์เดียวกัน ([`producer.py`](file:///c:/Omni-Flow/omni-command-center/producer.py))
- **Hot-spot test:** จำลองให้ key หนึ่งสร้างข้อมูลส่วนใหญ่ แล้ววัดสัดส่วนข้อมูลต่อ partition และ consumer lag
- **Key salting:** ทดลองกระจาย hot key ไปหลาย partition แล้ววัดผลต่อ throughput และความซับซ้อนในการรวมผล
- ก่อนใช้ salting ต้องระบุว่าระบบยอมเสีย ordering ระดับ entity ได้หรือไม่ และจะจัดการการรวมข้อมูลอย่างไร

---

## บทที่ 7: Transactions

### แนวคิดหลัก

Transactions ช่วยรวมการเปลี่ยนแปลงหลายรายการให้สอดคล้องกัน แต่ขอบเขตการรับประกันขึ้นกับฐานข้อมูลและ isolation level

- **Atomicity:** การเปลี่ยนแปลงใน transaction สำเร็จทั้งหมดหรือไม่สำเร็จทั้งหมด
- **Isolation:** ควบคุมผลกระทบระหว่าง transaction ที่ทำงานพร้อมกัน
- **Serializability:** ผลลัพธ์เทียบเท่าการทำ transaction แบบเรียงลำดับ แม้มี concurrency
- Distributed transactions เช่น two-phase commit ช่วยประสานหลายระบบ แต่เพิ่มความซับซ้อนและมี trade-off ด้าน availability และการปฏิบัติการ
- Transaction ในฐานข้อมูลต้นทางไม่ได้ทำให้การเขียนไปยังระบบปลายทางหลายตัวเป็น atomic โดยอัตโนมัติ

### เชื่อมโยงกับ Omni-Flow

- PostgreSQL ใช้ transaction รักษาความถูกต้องของการเปลี่ยนแปลงในฐานข้อมูลต้นทาง ([`lab3-debezium-cdc/oltp_traffic_simulator.py`](file:///c:/Omni-Flow/lab3-debezium-cdc/oltp_traffic_simulator.py))
- Debezium CDC อ่านการเปลี่ยนแปลงจาก WAL แล้วเผยแพร่เป็น event แต่การส่งต่อไป ClickHouse ยังต้องจัดการ retry, duplicate และลำดับของ event ([`lab3-debezium-cdc/cdc_sink_worker.py`](file:///c:/Omni-Flow/lab3-debezium-cdc/cdc_sink_worker.py))
- หากต้องทำ dual write จากแอปไปฐานข้อมูลและ Kafka โดยตรง ควรพิจารณาปัญหาที่หนึ่งสำเร็จแต่อีกที่ล้มเหลว และรูปแบบอย่าง transactional outbox ตามความเหมาะสม

---

## บทที่ 8: The Trouble with Distributed Systems

### แนวคิดหลัก

ระบบกระจายต้องทำงานภายใต้ความไม่แน่นอน ซึ่งไม่มีองค์ประกอบใดสามารถรู้สถานะของอีกองค์ประกอบได้ทันที

- network delay, timeout และ network partition
- process crash, restart หรือหยุดทำงานชั่วคราว
- clock ที่คลาดเคลื่อนและลำดับเหตุการณ์ที่ไม่ชัดเจน
- timeout ไม่ได้พิสูจน์ว่าปลายทางทำงานล้มเหลว อาจเพียงตอบช้า
- retry อาจทำให้เกิดการส่งซ้ำ จึงต้องออกแบบ idempotency และ deduplication ให้เหมาะกับขอบเขตของระบบ

### เชื่อมโยงกับ Omni-Flow

- จำลอง consumer หยุดรับข้อมูลหรือ network gap
- ตรวจสอบการ retry, consumer recovery, lag และการทำงานหลังเชื่อมต่อกลับ
- ตรวจว่าการส่งซ้ำไม่ทำให้ยอดปลายทางผิด โดยใช้ event ID หรือกลไก deduplication ที่มีเงื่อนไขชัดเจน
- อธิบายให้ได้ว่า retry ได้กี่ครั้ง ใช้ backoff แบบใด และจัดการ poison message อย่างไร

---

## บทที่ 9: Consistency and Consensus

### แนวคิดหลัก

คำว่า consistency มีหลายความหมาย ต้องระบุให้ชัดว่าระบบต้องการคุณสมบัติใด

- **Linearizability:** การดำเนินการดูเสมือนเกิดขึ้นทันที ณ จุดหนึ่งตามลำดับเวลาที่สอดคล้องกัน
- **Causal consistency:** รักษาลำดับความสัมพันธ์เชิงเหตุและผล
- **Total order broadcast:** สมาชิกเห็นลำดับข้อความเดียวกันภายใต้เงื่อนไขของระบบ
- **Consensus:** ทำให้สมาชิกตกลงในค่าหรือสถานะร่วมกัน แม้บางส่วนล้มเหลว ภายใต้สมมติฐานที่กำหนด

### เชื่อมโยงกับ Kafka

- Kafka ใช้ **KRaft** เพื่อจัดการ metadata และเลือก controller ผ่านกลไก consensus ของ metadata quorum
- การ replicate ข้อมูลใน topic partition เป็นอีกกลไกหนึ่ง: partition มี leader และ follower ของตนเอง
- จึงไม่ควรอธิบายว่า topic data replication ทั้งหมดเป็น Raft replication
- Epoch/term ช่วยระบุยุคของผู้นำและป้องกันคำสั่งจากผู้นำเก่าภายใต้กลไกของระบบ แต่ไม่ควรนำไปเหมารวมว่าเป็นคำรับประกันทุกด้านของ end-to-end consistency

### เชื่อมโยงกับ Omni-Flow

ทดสอบการเปลี่ยน controller หรือ broker leader แล้วสังเกตการเขียน การอ่าน และการกู้คืน พร้อมแยกให้ออกว่ากำลังทดสอบ metadata quorum หรือ replication ของ topic partition

---

## บทที่ 10: Batch Processing

### แนวคิดหลัก

Batch processing ประมวลผลข้อมูลที่มีขอบเขตชัดเจน และเหมาะกับการคำนวณซ้ำหรือสร้าง derived dataset

- Input และ output ควรระบุได้ชัด เพื่อให้ตรวจสอบและประมวลผลซ้ำได้
- การใช้ input ที่เก็บไว้และไม่เปลี่ยนแปลงช่วยให้สร้างผลลัพธ์ใหม่จาก logic รุ่นที่แก้ไขแล้วได้
- การประมวลผลซ้ำจะปลอดภัยเพียงใดขึ้นกับ deterministic behavior, side effects และการจัดการ output
- Immutable raw data เป็นประโยชน์ต่อการ audit และ replay แต่ต้องจัดการ retention, privacy และต้นทุนพื้นที่ด้วย

### เชื่อมโยงกับ Omni-Flow

- เก็บข้อมูลดิบใน Bronze layer ก่อนแปลงเป็น Silver และ Gold ([`lab4-orchestration-audit/stream_ingester.py`](file:///c:/Omni-Flow/lab4-orchestration-audit/stream_ingester.py))
- ใช้ DAG ระบุลำดับงาน dependency และการ retry ([`lab4-orchestration-audit/orchestrator_app.py`](file:///c:/Omni-Flow/lab4-orchestration-audit/orchestrator_app.py))
- รัน pipeline ซ้ำจากชุดข้อมูลเดิม แล้วเปรียบเทียบผลลัพธ์ก่อนและหลังแก้ logic
- ระบุวิธีป้องกันการเขียนผลซ้ำ เช่น ใช้ run ID, partition ของข้อมูล หรือการเขียนแบบแทนที่อย่างมีขอบเขต

---

## บทที่ 11: Stream Processing

### แนวคิดหลัก

Stream processing ประมวลผล event ที่เข้ามาต่อเนื่อง โดยต้องคิดทั้งสถานะ เวลา และการส่งซ้ำ

- **Event time:** เวลาที่เหตุการณ์เกิดขึ้น
- **Processing time:** เวลาที่ระบบประมวลผลเหตุการณ์
- **Watermark:** ตัวบ่งชี้ความคืบหน้าของ event time เพื่อจัดการ event ที่มาช้า
- **Windowing:** การรวมเหตุการณ์ตามช่วงเวลา เช่น tumbling หรือ sliding window
- **Stateful processing:** งานที่ต้องเก็บสถานะข้ามหลาย event ต้องออกแบบ checkpoint และ recovery
- “Exactly-once” ต้องระบุขอบเขตให้ชัด เช่น ภายใน engine, Kafka transaction หรือผลข้างเคียงปลายทาง ไม่ควรใช้เป็นคำรับประกันครอบคลุมทุกระบบโดยไม่มีเงื่อนไข

### เชื่อมโยงกับ Omni-Flow

- **Flink:** ทดลอง window, filter และ stateful computation โดยระบุว่าใช้ event time หรือ processing time ([`lab1-flink-stream/flink_job.sql`](file:///c:/Omni-Flow/lab1-flink-stream/flink_job.sql))
- **Debezium CDC:** ใช้ WAL ของ PostgreSQL เพื่อเผยแพร่การเปลี่ยนแปลงฐานข้อมูลเป็น event ([`lab3-debezium-cdc/register_connector.py`](file:///c:/Omni-Flow/lab3-debezium-cdc/register_connector.py))
- **ClickHouse sink:** ตรวจการ retry, event ซ้ำ, update และ delete ว่าถูกแทนค่าอย่างถูกต้องตาม schema และคีย์ที่ออกแบบไว้
- หากใช้ `ReplacingMergeTree` ต้องตรวจ sorting key, version column และรูปแบบ query จริง การใช้ `FINAL` ไม่ได้ทำให้ระบบเป็น exactly-once โดยอัตโนมัติ

---

## บทที่ 12: The Future of Data Systems

### แนวคิดหลัก

ระบบข้อมูลจริงมักประกอบด้วยฐานข้อมูล ระบบค้นหา data warehouse และ stream processor หลายส่วน ซึ่งเก็บข้อมูลที่ได้มาจากต้นทางเดียวกันในรูปแบบที่ต่างกัน

- ข้อมูลที่สร้างจากข้อมูลต้นทางเรียกว่า **derived data**
- ต้องจัดการความแตกต่างของ schema, ความล่าช้า, การอัปเดต และการสร้างข้อมูลซ้ำระหว่างระบบ
- ความถูกต้องควรตรวจสอบจากผลลัพธ์ปลายทาง ไม่พึ่งการตั้งค่าของแต่ละ component เพียงอย่างเดียว
- Audit และ reconciliation ช่วยตรวจพบข้อมูลตกหล่น ซ้ำ หรือถูกแปลงผิด แต่ต้องกำหนดนิยามและขอบเขตการตรวจให้ชัด

### เชื่อมโยงกับ Omni-Flow

- ตรวจข้อมูลที่ผ่าน validation ให้เข้า Silver และข้อมูลที่ไม่ผ่านให้เข้า Quarantine พร้อมเหตุผล ([`lab4-orchestration-audit/orchestrator_app.py`](file:///c:/Omni-Flow/lab4-orchestration-audit/orchestrator_app.py))
- สร้าง Gold mart จากข้อมูลที่ผ่านเกณฑ์ตามนิยามที่ระบุ
- ทำ reconciliation โดยกำหนดหน่วยนับและช่วงเวลาให้ตรงกัน เช่น จำนวน unique event หรือยอดรวมตาม business key
- สมการตัวอย่าง `Bronze = Silver + Quarantine` ใช้ได้เมื่อทั้งสามฝั่งนับข้อมูลชุดเดียวกัน ใช้นิยามเดียวกัน และไม่มีสถานะอื่นที่ตกหล่น เช่น event ที่ยังรอประมวลผลหรือถูกกรองด้วยเงื่อนไขอื่น

---

## กฎสรุปสำหรับทบทวน

- **อย่ารวมหลายการรับประกันเป็นคำเดียว:** replication, consistency, durability และ availability เป็นคนละคุณสมบัติ
- **ระบุขอบเขตของ exactly-once:** ระบุว่าครอบคลุมตั้งแต่ต้นทางถึง sink หรือเฉพาะภายในองค์ประกอบหนึ่ง
- **เลือก partition key จากข้อกำหนดด้าน ordering และการกระจายโหลด:** ทั้งสองเป้าหมายอาจขัดกัน
- **แยก metadata consensus ออกจากการ replicate ข้อมูลของ topic:** โดยเฉพาะเมื่ออธิบาย Kafka KRaft
- **ทดสอบด้วย workload และผลลัพธ์ที่ตรวจซ้ำได้:** รายงาน throughput, latency, lag, duplicate, missing records และเงื่อนไขของการทดสอบ
- **ใช้ตัวเลขอย่างมีที่มา:** เช่น “0 loss” หรือ “1,500 TPS” ควรแนบวิธีทดสอบ ระยะเวลา config และการตรวจสอบผลลัพธ์

## สรุป

ฉบับนี้ครอบคลุมทั้ง 12 บท โดยเพิ่มบทที่ 2 และขยายหัวข้อ transactions, distributed systems, consistency/consensus และ stream processing ให้ชัดขึ้น ส่วน Omni-Flow ใช้เป็นชุดทดลองประกอบแนวคิดได้ โดยควรยืนยันรายละเอียดแต่ละข้อจากโค้ดและ config จริงก่อนนำเสนอเป็นผลการทดสอบหรือคำรับประกันของระบบ
