import json
import pickle
import struct
from pathlib import Path
from typing import Any, Dict

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

app = FastAPI(title="Lab 5: DDIA Chapter 4 — Encoding & Schema Evolution Playground")

# ============================================================================
# 1. REAL BYTE-LEVEL ENCODERS FOR DDIA CHAPTER 4 CANONICAL EXAMPLE
#    {"userName": "Martin", "favoriteNumber": 1337, "interests": ["daydreaming", "hacking"]}
# ============================================================================


def encode_varint(val: int) -> bytes:
    out = bytearray()
    while val > 0x7F:
        out.append((val & 0x7F) | 0x80)
        val >>= 7
    out.append(val & 0x7F)
    return bytes(out)


def encode_zigzag_varint(val: int) -> bytes:
    zz = (val << 1) ^ (val >> 63)
    return encode_varint(zz)


def build_encoding_comparison(user_name: str = "Martin", fav_num: int = 1337, interests: list = None):
    if interests is None:
        interests = ["daydreaming", "hacking"]

    record = {
        "userName": user_name,
        "favoriteNumber": fav_num,
        "interests": interests,
    }

    # 1. Python pickle (ตอนที่ 3: รูปแบบเฉพาะของแต่ละภาษา)
    pickle_bytes = pickle.dumps(record, protocol=4)

    # 2. JSON without whitespace (ตอนที่ 4: 81 ไบต์สำหรับตัวอย่างมาตรฐาน)
    json_str = json.dumps(record, separators=(",", ":"), ensure_ascii=False)
    json_bytes = json_str.encode("utf-8")

    # 3. MessagePack / Binary JSON (ตอนที่ 5: 66 ไบต์ — ยังต้องเก็บชื่อช่อง userName, favoriteNumber, interests)
    u_b = user_name.encode("utf-8")
    i0_b = interests[0].encode("utf-8") if len(interests) > 0 else b""
    i1_b = interests[1].encode("utf-8") if len(interests) > 1 else b""
    msgpack_bytes = (
        bytes([0x83])  # 3-element map
        + bytes([0xA8]) + b"userName" + bytes([0xA0 | len(u_b)]) + u_b
        + bytes([0xAE]) + b"favoriteNumber" + bytes([0xCD]) + struct.pack(">H", fav_num & 0xFFFF)
        + bytes([0xA9]) + b"interests" + bytes([0x92])
        + bytes([0xA0 | len(i0_b)]) + i0_b
        + bytes([0xA0 | len(i1_b)]) + i1_b
    )

    # 4. Thrift BinaryProtocol (ตอนที่ 6: 59 ไบต์ — ใช้ Field Tag 1, 2, 3 แต่ใช้ Fixed-width integers)
    thrift_binary_bytes = (
        bytes([0x0B, 0x00, 0x01]) + struct.pack(">I", len(u_b)) + u_b
        + bytes([0x0A, 0x00, 0x02]) + struct.pack(">q", fav_num)
        + bytes([0x0F, 0x00, 0x03, 0x0B]) + struct.pack(">I", 2)
        + struct.pack(">I", len(i0_b)) + i0_b
        + struct.pack(">I", len(i1_b)) + i1_b
        + bytes([0x00])
    )

    # 5. Thrift CompactProtocol (ตอนที่ 6: 34 ไบต์ — ใช้ Field Tag Delta + ZigZag Varint)
    thrift_compact_bytes = (
        bytes([0x18, len(u_b)]) + u_b
        + bytes([0x16]) + encode_zigzag_varint(fav_num)
        + bytes([0x19, 0x28])
        + bytes([len(i0_b)]) + i0_b
        + bytes([len(i1_b)]) + i1_b
        + bytes([0x00])
    )

    # 6. Protocol Buffers / Protobuf (ตอนที่ 6: 33 ไบต์ — Field Tag 1, 2, 3 + Varint)
    pb_fav = encode_varint(fav_num)
    protobuf_bytes = (
        bytes([0x0A, len(u_b)]) + u_b
        + bytes([0x10]) + pb_fav
        + bytes([0x1A, len(i0_b)]) + i0_b
        + bytes([0x1A, len(i1_b)]) + i1_b
    )

    # 7. Apache Avro (ตอนที่ 7: 32 ไบต์ — เล็กที่สุด! ไม่มีชื่อช่องและไม่มีหมายเลข Field Tag)
    # ในหนังสือ DDIA (Figure 4-5) ช่อง favoriteNumber เป็น union ["long", "null"] จึงมีไบต์บอกตำแหน่ง union (0x00) 1 ไบต์ รวมเป็น 32 ไบต์เป๊ะ!
    avro_bytes = (
        encode_zigzag_varint(len(u_b)) + u_b
        + bytes([0x00]) + encode_zigzag_varint(fav_num)
        + encode_zigzag_varint(2)  # Array block count = 2
        + encode_zigzag_varint(len(i0_b)) + i0_b
        + encode_zigzag_varint(len(i1_b)) + i1_b
        + bytes([0x00])  # End of array
    )

    return {
        "record": record,
        "formats": [
            {
                "id": "pickle",
                "name": "1. Python pickle (Language-Specific)",
                "episode": "ตอนที่ 3: รูปแบบเฉพาะของแต่ละภาษา",
                "byte_size": len(pickle_bytes),
                "hex_preview": pickle_bytes.hex(" "),
                "has_field_names": True,
                "has_field_tags": False,
                "verdict": "❌ ไม่ควรใช้ข้ามระบบ: ขนาดใหญ่ ผูกติดกับภาษา Python และเสี่ยงช่องโหว่ Remote Code Execution ตอนถอดรหัส",
            },
            {
                "id": "json",
                "name": "2. JSON (No Whitespace)",
                "episode": "ตอนที่ 4: JSON, XML และ CSV",
                "byte_size": len(json_bytes),
                "hex_preview": json_bytes.hex(" "),
                "has_field_names": True,
                "has_field_tags": False,
                "verdict": "⚠️ 81 ไบต์ (ใหญ่ที่สุดในกลุ่มมาตรฐาน): คนอ่านออกง่าย แต่เปลืองพื้นที่เก็บชื่อช่อง ('userName', 'favoriteNumber') ซ้ำทุกแถว และไม่แยกจำนวนเต็มกับทศนิยม",
            },
            {
                "id": "msgpack",
                "name": "3. MessagePack (Binary JSON)",
                "episode": "ตอนที่ 5: การเข้ารหัสแบบไบนารี",
                "byte_size": len(msgpack_bytes),
                "hex_preview": msgpack_bytes.hex(" "),
                "has_field_names": True,
                "has_field_tags": False,
                "verdict": "🟡 66 ไบต์: เล็กลงจาก 81 เหลือ 66 ไบต์ (ลดลงนิดเดียว) เพราะไม่มีสคีมา จึงยังต้องแบกชื่อช่อง 'userName', 'favoriteNumber' ไว้ในทุกข้อความ",
            },
            {
                "id": "thrift_bin",
                "name": "4. Thrift (BinaryProtocol)",
                "episode": "ตอนที่ 6: Thrift และ Protocol Buffers",
                "byte_size": len(thrift_binary_bytes),
                "hex_preview": thrift_binary_bytes.hex(" "),
                "has_field_names": False,
                "has_field_tags": True,
                "verdict": "🟢 59 ไบต์: ตัดชื่อช่องทิ้งทั้งหมด เปลี่ยนไปใช้หมายเลขประจำช่อง (Field Tag #1, #2, #3) แทน",
            },
            {
                "id": "thrift_compact",
                "name": "5. Thrift (CompactProtocol)",
                "episode": "ตอนที่ 6: Thrift และ Protocol Buffers",
                "byte_size": len(thrift_compact_bytes),
                "hex_preview": thrift_compact_bytes.hex(" "),
                "has_field_names": False,
                "has_field_tags": True,
                "verdict": "🟢 34 ไบต์: อัดชนิดข้อมูลกับหมายเลขช่องไว้ในไบต์เดียวกัน และใช้ตัวเลขความยาวผันแปร (Varint)",
            },
            {
                "id": "protobuf",
                "name": "6. Protocol Buffers (Protobuf)",
                "episode": "ตอนที่ 6: Thrift และ Protocol Buffers",
                "byte_size": len(protobuf_bytes),
                "hex_preview": protobuf_bytes.hex(" "),
                "has_field_names": False,
                "has_field_tags": True,
                "verdict": "🏆 33 ไบต์ (เล็กกว่า JSON เกินครึ่ง!): ใช้ Field Tag (#1, #2, #3) จึงเปลี่ยนชื่อช่องได้ตามสบายโดยโค้ดเก่าไม่พัง",
            },
            {
                "id": "avro",
                "name": "7. Apache Avro (Writer + Reader Schema)",
                "episode": "ตอนที่ 7: Avro",
                "byte_size": len(avro_bytes),
                "hex_preview": avro_bytes.hex(" "),
                "has_field_names": False,
                "has_field_tags": False,
                "verdict": "👑 32 ไบต์ (เล็กที่สุดในทุกแบบ!): ในชุดไบต์ไม่มีแม้แต่หมายเลขช่อง มีแต่ค่าข้อมูลเรียงต่อกันล้วนๆ โดยอาศัยการเทียบสคีมาของผู้เขียน (Writer) กับผู้อ่าน (Reader)",
            },
        ],
        "byte_breakdown": {
            "protobuf_33_segments": [
                {"bytes": "0a 06", "meaning": "Field Tag #1 (wire type 2 = string), ความยาว 6 ไบต์"},
                {"bytes": "4d 61 72 74 69 6e", "meaning": "ค่าข้อความ 'Martin'"},
                {"bytes": "10 b9 0a", "meaning": "Field Tag #2 (wire type 0 = varint), ค่าตัวเลข 1337"},
                {"bytes": "1a 0b 64..67", "meaning": "Field Tag #3 (repeated string), ความยาว 11 ไบต์ 'daydreaming'"},
                {"bytes": "1a 07 68..67", "meaning": "Field Tag #3 (repeated string), ความยาว 7 ไบต์ 'hacking'"},
            ],
            "avro_32_segments": [
                {"bytes": "0c 4d 61 72 74 69 6e", "meaning": "ความยาว 6 (zigzag 0x0c) + 'Martin' (ไม่มี Tag!)"},
                {"bytes": "00 f2 14", "meaning": "Union branch 0 (long = 0x00) + เลข 1337 แบบ ZigZag Varint (0xf2 0x14)"},
                {"bytes": "04 16 64..67 0e 68..67 00", "meaning": "อาเรย์ 2 ช่อง: 'daydreaming' + 'hacking' + ปิดท้ายอาเรย์ (0x00)"},
            ],
        },
    }


# ============================================================================
# 2. ROLLING UPGRADE & SCHEMA EVOLUTION SIMULATOR (EPISODES 1, 4, 6, 7)
# ============================================================================


@app.post("/api/simulate-evolution/{scenario_id}")
def simulate_schema_evolution(scenario_id: str):
    if scenario_id == "ADD_OPTIONAL_WITH_DEFAULT":
        return {
            "scenario": "เพิ่มช่องใหม่ 'email' (Tag #4) พร้อมกำหนดค่าเริ่มต้น (default = 'none@omni.io')",
            "episode": "ตอนที่ 1, 6 และ 7: Backward & Forward Compatibility",
            "writer_v2_payload": {
                "1:userName": "Martin",
                "2:favoriteNumber": 1337,
                "3:interests": ["daydreaming", "hacking"],
                "4:email": "martin@ddia.io",
            },
            "forward_check": {
                "direction": "Forward Compatibility (โค้ดเก่า v1 อ่านข้อมูลที่โค้ดใหม่ v2 เขียน)",
                "status": "✅ PASS (ผ่านฉลุย)",
                "result_seen_by_v1": {
                    "userName": "Martin",
                    "favoriteNumber": 1337,
                    "interests": ["daydreaming", "hacking"],
                },
                "explanation": "โค้ดเก่า (v1) เจอหมายเลขช่อง #4 (Protobuf) หรือชื่อช่อง 'email' ที่ไม่มีใน Reader's Schema (Avro) มันจึงรู้ขนาดไบต์และกระโดดข้ามไปได้โดยไม่พัง!",
            },
            "backward_check": {
                "direction": "Backward Compatibility (โค้ดใหม่ v2 อ่านข้อมูลเก่าที่ v1 เขียนไว้)",
                "status": "✅ PASS (ผ่านฉลุย)",
                "result_seen_by_v2": {
                    "userName": "Martin",
                    "favoriteNumber": 1337,
                    "interests": ["daydreaming", "hacking"],
                    "email": "none@omni.io (เติมค่า Default อัตโนมัติ)",
                },
                "explanation": "ข้อมูลเก่าจาก v1 ไม่มีช่อง #4 ('email') แต่เพราะเราตั้งเป็นช่องไม่บังคับและมีค่าเริ่มต้น โค้ดใหม่ v2 จึงเติมค่าเริ่มต้น 'none@omni.io' ให้ทันที",
            },
        }

    if scenario_id == "ADD_REQUIRED_NO_DEFAULT":
        return {
            "scenario": "เพิ่มช่องใหม่ 'phoneNumber' แบบบังคับ (Required / ไม่มีค่าเริ่มต้น)",
            "episode": "ตอนที่ 6 และ 7: กฎเหล็กของการเพิ่มและลบช่อง",
            "writer_v2_payload": {
                "1:userName": "Martin",
                "2:favoriteNumber": 1337,
                "4:phoneNumber (REQUIRED)": "+66-81-000-0000",
            },
            "forward_check": {
                "direction": "Forward Compatibility (โค้ดเก่า v1 อ่านข้อมูลจากโค้ดใหม่ v2)",
                "status": "✅ PASS (โค้ดเก่าข้ามช่อง #4 ได้)",
                "result_seen_by_v1": {"userName": "Martin", "favoriteNumber": 1337},
                "explanation": "โค้ดเก่า v1 ไม่รู้จักช่อง #4 จึงมองข้ามไปได้",
            },
            "backward_check": {
                "direction": "Backward Compatibility (โค้ดใหม่ v2 อ่านข้อมูลเก่าจาก v1)",
                "status": "❌ BROKEN! (พังทันที — SchemaIncompatibleException)",
                "result_seen_by_v2": {
                    "error": "ERROR: Missing required field 'phoneNumber' (Tag #4) with no default value in old record!"
                },
                "explanation": "ตรงกับ PDF หน้า 3–4 เป๊ะ: 'ถ้าตั้งเป็นช่องบังคับ โค้ดใหม่จะอ่านข้อมูลเก่าไม่ได้!' เพราะข้อมูลเก่าที่เขียนไว้เมื่อปีก่อนไม่มีช่อง phoneNumber และระบบไม่รู้จะเอาค่าอะไรมาใส่",
            },
        }

    if scenario_id == "RENAME_FIELD":
        return {
            "scenario": "เปลี่ยนชื่อช่องจาก 'userName' ──► 'fullName' (แต่ยังใช้หมายเลขช่อง Field Tag #1 เดิม)",
            "episode": "ตอนที่ 6 vs ตอนที่ 7: ความต่างระหว่าง Protobuf/Thrift กับ Avro/JSON",
            "writer_v2_payload": {
                "Tag #1 (fullName)": "Martin Kleppmann",
                "Tag #2 (favoriteNumber)": 1337,
            },
            "forward_check": {
                "direction": "ใน Protobuf / Thrift (ใช้หมายเลขประจำช่อง Field Tag #1)",
                "status": "✅ PASS 100% (เปลี่ยนชื่อช่องได้ตามสบาย!)",
                "result_seen_by_v1": {"userName (อ่านจาก Tag #1)": "Martin Kleppmann", "favoriteNumber": 1337},
                "explanation": "ตรงกับ PDF หน้า 3: 'เราเปลี่ยนชื่อช่องได้ตามสบาย เพราะในข้อมูลไบนารีไม่มีชื่อช่องอยู่เลย มีแค่หมายเลข #1!' แต่ห้ามเปลี่ยนหมายเลขประจำช่องเด็ดขาด",
            },
            "backward_check": {
                "direction": "ใน JSON / Avro (จับคู่ช่องด้วยชื่อข้อความ ถ้าไม่ได้ตั้ง Alias)",
                "status": "⚠️ MISMATCH / ข้อมูลหายถ้าไม่ตั้ง Alias",
                "result_seen_by_v2": {
                    "fullName": "NULL / ERROR (หาช่องชื่อ 'fullName' ใน JSON เก่าไม่เจอ เพราะของเก่าชื่อ 'userName')"
                },
                "explanation": "ใน JSON และ Avro ระบบจับคู่ด้วยชื่อช่อง ดังนั้นการเปลี่ยนชื่อช่องใน Avro ต้องระบุ aliases: ['userName'] ในสคีมาเสมอ มิฉะนั้นจะอ่านค่าเดิมไม่ออก",
            },
        }

    # TWITTER_64BIT_NUMBER_TRAP (ตอนที่ 4 & ตอนที่ 6)
    tweet_id_int64 = 1854321098765432199
    # Simulate IEEE-754 double precision loss (53-bit significand in JavaScript)
    corrupted_js_num = int(float(tweet_id_int64))
    truncated_32bit = tweet_id_int64 & 0xFFFFFFFF
    return {
        "scenario": "ปัญหาตัวเลข 64-bit ของ Twitter ใน JSON/JavaScript และการลดขนาด 64-bit -> 32-bit",
        "episode": "ตอนที่ 4 (หน้า 2) และ ตอนที่ 6 (หน้า 3): ปัญหาเรื่องความแม่นยำของตัวเลข",
        "writer_v2_payload": {
            "original_64bit_tweet_id": str(tweet_id_int64),
            "safe_twitter_json": {
                "id": corrupted_js_num,
                "id_str": str(tweet_id_int64),
            },
        },
        "forward_check": {
            "direction": "เมื่อเว็บเบราว์เซอร์ (JavaScript) อ่านตัวเลข 64-bit จาก JSON ตรงๆ",
            "status": "❌ ตัวเลขเพี้ยน! (Precision Loss เกิน 2^53)",
            "result_seen_by_v1": {
                "รหัสทวีตจริงที่ส่งมา": str(tweet_id_int64),
                "ค่าที่ JavaScript อ่านได้": f"{corrupted_js_num} (เลขท้ายเพี้ยนจาก 199 กลายเป็น {str(corrupted_js_num)[-3:]})",
            },
            "explanation": "ตรงกับ PDF หน้า 2: JSON ไม่ได้แยกจำนวนเต็มกับทศนิยม และ JavaScript เก็บตัวเลขเป็นทศนิยม 64-bit (แม่นยำแค่ 53 บิต) รหัสทวีตที่ใหญ่กว่านั้นจึงเพี้ยน! Twitter จึงต้องส่งมาทั้งสองแบบคือ id (ตัวเลข) และ id_str (ข้อความ)",
        },
        "backward_check": {
            "direction": "เมื่อโค้ดใหม่ส่งเลข 64-bit แต่โค้ดเก่าใช้ตัวแปร 32-bit อ่าน",
            "status": "❌ ตัวเลขถูกตัดจนเพี้ยน! (Bit Truncation)",
            "result_seen_by_v2": {
                "ค่าที่โค้ดใหม่ (64-bit) เขียน": str(tweet_id_int64),
                "ค่าที่โค้ดเก่า (32-bit) อ่านได้": f"{truncated_32bit} (ข้อมูลหายไปครึ่งหนึ่ง!)",
            },
            "explanation": "ตรงกับ PDF หน้า 3 ตอนท้าย: 'ถ้าโค้ดเก่าอ่านตัวเลขใหญ่ (64 บิต) ที่โค้ดใหม่เขียน ตัวเลขจะถูกตัดจนเพี้ยน!'",
        },
    }


# ============================================================================
# 3. EPISODE 8 TRAP: DATAFLOW THROUGH DATABASES (OLD CODE OVERWRITING NEW FIELD)
# ============================================================================
db_storage: Dict[str, Any] = {
    "raw_bytes_on_disk": {
        "1:userName": "Martin",
        "2:favoriteNumber": 1337,
        "3:interests": ["daydreaming", "hacking"],
        "4:loyaltyPoints (ช่องใหม่จากโค้ด v2)": 500,
    },
    "last_writer": "Service v2 (New Code)",
    "status": "INTACT",
    "history": [
        "1. [Service v2 - โค้ดใหม่] เขียนข้อมูล Martin พร้อมช่องใหม่ '4:loyaltyPoints = 500' ลงฐานข้อมูล",
    ],
}


@app.post("/api/db-trap/{mode}")
def run_database_trap(mode: str):
    global db_storage
    if mode == "RESET_V2":
        db_storage = {
            "raw_bytes_on_disk": {
                "1:userName": "Martin",
                "2:favoriteNumber": 1337,
                "3:interests": ["daydreaming", "hacking"],
                "4:loyaltyPoints (ช่องใหม่จากโค้ด v2)": 500,
            },
            "last_writer": "Service v2 (New Code)",
            "status": "INTACT",
            "history": [
                "1. [Service v2 - โค้ดใหม่] เขียนข้อมูล Martin พร้อมช่องใหม่ '4:loyaltyPoints = 500' ลงฐานข้อมูลเรียบร้อย",
            ],
        }
        return db_storage

    if mode == "NAIVE_V1_UPDATE":
        # โค้ดเก่า v1 อ่านข้อมูลขึ้นมา รู้จักแค่ช่อง 1, 2, 3 แล้วเขียนทับกลับลงไป ทำให้ช่องที่ 4 หาย!
        db_storage["raw_bytes_on_disk"] = {
            "1:userName": "Martin",
            "2:favoriteNumber": 9999,
            "3:interests": ["daydreaming", "hacking"],
        }
        db_storage["last_writer"] = "Service v1 (Old Code - ไม่ได้เก็บ Unknown Fields!)"
        db_storage["status"] = "DATA_LOST"
        db_storage["history"].insert(
            0,
            "❌ [กับดักตอนที่ 8 ทำงาน!] โค้ดเก่า (v1) อ่านแถวนี้มาแก้เลข 1337 -> 9999 แล้วเขียนกลับลง DB โดยทิ้งช่อง '4:loyaltyPoints' ที่ตัวเองไม่รู้จัก ทำให้แต้มสะสม 500 คะแนนหายไปโดยไม่มีใครรู้!",
        )
        return db_storage

    if mode == "SAFE_V1_UPDATE":
        # โค้ดเก่า v1 ที่ออกแบบมาอย่างถูกต้อง (เก็บ Unknown Fields ไว้ครบตอนเขียนกลับ)
        unknown_preserved = db_storage["raw_bytes_on_disk"].get(
            "4:loyaltyPoints (ช่องใหม่จากโค้ด v2)", 500
        )
        db_storage["raw_bytes_on_disk"] = {
            "1:userName": "Martin",
            "2:favoriteNumber": 9999,
            "3:interests": ["daydreaming", "hacking"],
            "4:loyaltyPoints (ช่องใหม่จากโค้ด v2)": unknown_preserved,
        }
        db_storage["last_writer"] = "Service v1 (Old Code - Preserved Unknown Fields ✅)"
        db_storage["status"] = "SAFE_PRESERVED"
        db_storage["history"].insert(
            0,
            "✅ [ป้องกันสำเร็จ!] โค้ดเก่า (v1) แก้เลขเป็น 9999 และแนบไบต์ของช่องที่ไม่รู้จัก ('4:loyaltyPoints = 500') กลับลงฐานข้อมูลครบถ้วน!",
        )
        return db_storage

    return db_storage


class CustomEncodeReq(BaseModel):
    userName: str = "Martin"
    favoriteNumber: int = 1337
    interest1: str = "daydreaming"
    interest2: str = "hacking"


@app.post("/api/encode")
def custom_encode(req: CustomEncodeReq):
    return build_encoding_comparison(
        req.userName, req.favoriteNumber, [req.interest1, req.interest2]
    )


@app.get("/api/init")
def get_initial_state():
    return {
        "encoding_comparison": build_encoding_comparison(),
        "db_trap": db_storage,
    }


static_dir = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


@app.get("/")
def serve_index():
    return FileResponse(str(static_dir / "index.html"))
