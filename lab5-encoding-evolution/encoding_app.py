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


# ============================================================================
# CHAPTER 2: DATA MODELS AND QUERY LANGUAGES INTERACTIVE API
# ============================================================================


@app.post("/api/ch2/linkedin/{action}")
def simulate_ch2_linkedin(action: str):
    if action == "COMPARE_STRUCTURE":
        return {
            "title": "เปรียบเทียบการเก็บโปรไฟล์สมัครงาน (LinkedIn) — ตอนที่ 2 หน้า 2",
            "left_title": "📄 โมเดลเอกสาร (Document Model — JSON ต้นไม้ก้อนเดียว)",
            "left_badge": "✅ Locality สูง (ดึงครั้งเดียวได้ครบทั้งโปรไฟล์)",
            "left_data": {
                "user_id": 251,
                "name": "สมชาย ใจดี",
                "region": "กรุงเทพมหานคร",
                "positions": [
                    {"job_title": "Senior Data Architect", "company": "OmniCorp", "years": "2023-ปัจจุบัน"},
                    {"job_title": "Data Engineer", "company": "TechThai", "years": "2020-2023"},
                ],
                "education": [
                    {"school": "จุฬาลงกรณ์มหาวิทยาลัย", "degree": "วิศวกรรมคอมพิวเตอร์"},
                ],
            },
            "left_note": "มองคนหนึ่งคนเป็น 'รากของต้นไม้' แล้วแตกกิ่งประวัติงานและการศึกษาออกไป เวลาดึงโปรไฟล์มาโชว์หน้าเว็บ ดึงแค่ JSON ก้อนเดียวจบ ไม่ต้อง JOIN!",
            "right_title": "🗄️ โมเดลเชิงสัมพันธ์ (Relational Model — แยก 4 ตาราง)",
            "right_badge": "🔗 ต้องเชื่อมข้อมูลข้ามตาราง (Multi-Table JOIN)",
            "right_data": {
                "ตารางที่ 1 (users)": [{"user_id": 251, "name": "สมชาย ใจดี", "region_id": 10}],
                "ตารางที่ 2 (regions)": [{"region_id": 10, "region_name": "กรุงเทพมหานคร"}],
                "ตารางที่ 3 (positions)": [
                    {"user_id": 251, "job_title": "Senior Data Architect", "company": "OmniCorp"},
                    {"user_id": 251, "job_title": "Data Engineer", "company": "TechThai"},
                ],
                "ตารางที่ 4 (education)": [
                    {"user_id": 251, "school": "จุฬาลงกรณ์มหาวิทยาลัย"},
                ],
            },
            "right_note": "ต้องแปลงออบเจกต์ในโค้ดแยกไปเก็บ 4 ตาราง (ความไม่เข้ากันนี้เรียกว่า Impedance Mismatch) เวลาจะดึงโปรไฟล์ 1 คน ต้อง JOIN หลายตารางเข้าด้วยกัน",
        }

    if action == "UPDATE_BANGKOK":
        return {
            "title": "ทดสอบเปลี่ยนชื่อจังหวัด 'กรุงเทพมหานคร' ──► 'กรุงเทพฯ (Bangkok)' (Normalization vs Denormalization)",
            "left_title": "📄 โมเดลเอกสาร (เก็บข้อความ 'กรุงเทพมหานคร' ซ้ำในทุกโปรไฟล์)",
            "left_badge": "⚠️ ข้อมูลซ้ำซ้อน (ต้องตามแก้ทุกโปรไฟล์!)",
            "left_data": {
                "โปรไฟล์ที่ 1 (สมชาย)": {"name": "สมชาย", "region": "กรุงเทพฯ (Bangkok) ✅ แก้แล้ว"},
                "โปรไฟล์ที่ 2 (สุดา)": {"name": "สุดา", "region": "กรุงเทพฯ (Bangkok) ✅ แก้แล้ว"},
                "โปรไฟล์ที่ 3 (อนันต์ - แก้ตกหล่น!)": {"name": "อนันต์", "region": "กรุงเทพมหานคร ❌ ข้อมูลเก่าค้าง!"},
            },
            "left_note": "ตรงกับ PDF หน้า 2: ถ้าเก็บคำว่า 'กรุงเทพมหานคร' เป็นข้อความซ้ำในทุกโปรไฟล์ วันหนึ่งถ้าต้องแก้ชื่อ ต้องตามแก้ทุกโปรไฟล์! ถ้ามีล้านคนก็ต้องเขียนทับล้านจุด",
            "right_title": "🗄️ โมเดลเชิงสัมพันธ์ (Normalization — เก็บแค่รหัส region_id = 10)",
            "right_badge": "✅ แก้ที่เดียว เปลี่ยนครบทุกคนทันที!",
            "right_data": {
                "ตารางจังหวัด (regions — แก้แค่ 1 แถวนี้!)": {
                    "region_id": 10,
                    "region_name": "กรุงเทพฯ (Bangkok) ✅ อัปเดตจุดเดียวจบ!",
                },
                "ตารางผู้ใช้ (users — ไม่ต้องแตะเลย!)": [
                    {"name": "สมชาย", "region_id": 10, "เห็นชื่อใหม่ทันที": "กรุงเทพฯ (Bangkok)"},
                    {"name": "สุดา", "region_id": 10, "เห็นชื่อใหม่ทันที": "กรุงเทพฯ (Bangkok)"},
                    {"name": "อนันต์", "region_id": 10, "เห็นชื่อใหม่ทันที": "กรุงเทพฯ (Bangkok)"},
                ],
            },
            "right_note": "ตรงกับ PDF หน้า 2: 'ทางที่ดีกว่าคือเก็บเป็นรหัส (region_id = 10) แล้วให้รหัสนั้นชี้ไปยังข้อมูลจังหวัดที่เก็บไว้ที่เดียว แนวคิดนี้เรียกว่า Normalization หรือการลดข้อมูลซ้ำซ้อน'",
        }

    # SCHEMA_ON_READ_VS_WRITE
    return {
        "title": "ทดสอบแยกช่องชื่อเต็ม ('สมชาย ใจดี') ──► ชื่อต้น (first_name) กับ นามสกุล (last_name) (หน้า 3)",
        "left_title": "🐍 Schema-on-Read (โมเดลเอกสาร — ตีความโครงสร้างตอนอ่าน)",
        "left_badge": "⚡ ไม่ต้องหยุดระบบ ไม่ต้องแปลงข้อมูลเก่าทันที",
        "left_data": {
            "เอกสารรุ่นใหม่ (เขียนวันนี้)": {"first_name": "สมชาย", "last_name": "ใจดี"},
            "เอกสารรุ่นเก่า (เขียนเมื่อ 5 ปีก่อน)": {"name": "วิชัย รักดี"},
            "โค้ดฝั่งอ่าน (จัดการให้ตอนดึงข้อมูล)": (
                "if 'name' in doc and 'first_name' not in doc:\n"
                "    doc['first_name'] = doc['name'].split(' ')[0]\n"
                "    doc['last_name']  = doc['name'].split(' ')[1]"
            ),
        },
        "left_note": "ตรงกับ PDF หน้า 3: 'ในโมเดลเอกสาร เราแค่เขียนโค้ดให้รองรับเอกสารทั้งแบบเก่าและแบบใหม่ (คล้าย Python ที่ตรวจชนิดตอนรัน) เรียกว่า Schema-on-read'",
        "right_title": "☕ Schema-on-Write (โมเดลเชิงสัมพันธ์ — บังคับโครงสร้างตอนเขียน)",
        "right_badge": "🔒 สะอาดตั้งแต่ต้น แต่ถ้าตารางใหญ่ต้องรัน Migration นาน",
        "right_data": {
            "คำสั่งบังคับเปลี่ยนโครงสร้างตาราง (SQL Migration)": (
                "ALTER TABLE users ADD COLUMN first_name VARCHAR(100);\n"
                "ALTER TABLE users ADD COLUMN last_name VARCHAR(100);\n"
                "UPDATE users SET\n"
                "  first_name = split_part(name, ' ', 1),\n"
                "  last_name  = split_part(name, ' ', 2);"
            ),
        },
        "right_note": "ตรงกับ PDF หน้า 3: 'ในโมเดลเชิงสัมพันธ์ ต้องสั่งแก้โครงสร้างตาราง แล้วแปลงข้อมูลเก่าทั้งหมด ซึ่งในตารางใหญ่ๆ อาจใช้เวลานาน เรียกว่า Schema-on-write'",
    }


@app.post("/api/ch2/sharks/{style}")
def simulate_ch2_sharks(style: str):
    observations = [
        {"month": "2026-01", "animal": "ฉลามขาว (Great White Shark)", "family": "Sharks", "count": 3},
        {"month": "2026-01", "animal": "โลมาปากขวด (Dolphin)", "family": "Mammals", "count": 12},
        {"month": "2026-01", "animal": "ฉลามหัวค้อน (Hammerhead Shark)", "family": "Sharks", "count": 4},
        {"month": "2026-02", "animal": "ฉลามวาฬ (Whale Shark)", "family": "Sharks", "count": 2},
        {"month": "2026-02", "animal": "เต่าตนุ (Green Turtle)", "family": "Reptiles", "count": 5},
        {"month": "2026-02", "animal": "ฉลามเสือ (Tiger Shark)", "family": "Sharks", "count": 6},
    ]
    result = {"2026-01 (มกราคม)": "พบฉลามรวม 7 ตัว (3 + 4)", "2026-02 (กุมภาพันธ์)": "พบฉลามรวม 8 ตัว (2 + 6)"}

    if style == "IMPERATIVE":
        return {
            "style_name": "1. แบบสั่งทีละขั้น (Imperative Query — บอกว่า 'ต้องทำอย่างไร' ทีละบรรทัด)",
            "code": (
                "totals = {}\n"
                "for obs in observations:              # 1. สั่งวนลูปดูข้อมูลทีละแถวเอง\n"
                "    if obs['family'] == 'Sharks':     # 2. เช็กเงื่อนไขเองว่าใช่ฉลามไหม\n"
                "        m = obs['month']\n"
                "        if m not in totals:\n"
                "            totals[m] = 0\n"
                "        totals[m] += obs['count']     # 3. บวกสะสมยอดเข้าตัวแปรเอง"
            ),
            "pros_cons": "❌ โค้ดยาว ต้องกำหนดลำดับขั้นตอนตายตัว ทำให้ฐานข้อมูลเอาไปรันขนานหลายซีพียูหรือเปลี่ยนไปใช้ Index อัตโนมัติได้ยาก",
            "observations": observations,
            "result": result,
        }

    if style == "MAPREDUCE":
        return {
            "style_name": "2. MapReduce (ลูกผสม — เขียนฟังก์ชัน map ดึงข้อมูล + ฟังก์ชัน reduce รวมยอด)",
            "code": (
                "// ขั้นที่ 1: ฟังก์ชัน Map (ทำงานกับเอกสารแต่ละใบ)\n"
                "function map() {\n"
                "    if (this.family === 'Sharks') {\n"
                "        emit(this.month, this.count); // ส่งออก (เดือน, จำนวนตัว)\n"
                "    }\n"
                "}\n\n"
                "// ขั้นที่ 2: ฟังก์ชัน Reduce (รวมยอดของเดือนเดียวกันเข้าด้วยกัน)\n"
                "function reduce(month, counts) {\n"
                "    return Array.sum(counts);\n"
                "}"
            ),
            "pros_cons": "🟡 กระจายรันบนหลายเครื่องได้ แต่การต้องเขียนฟังก์ชัน 2 ตัวให้ประสานกันนั้นยุ่งยาก ภายหลังแม้แต่ MongoDB จึงหันมาทำ Aggregation Pipeline แบบประกาศแทน",
            "observations": observations,
            "result": result,
        }

    # DECLARATIVE
    return {
        "style_name": "3. แบบประกาศ (Declarative SQL — บอกแค่ว่า 'ต้องการผลลัพธ์อะไร' เหมือนเขียน CSS)",
        "code": (
            "SELECT month, SUM(count) AS total_sharks\n"
            "FROM observations\n"
            "WHERE family = 'Sharks'\n"
            "GROUP BY month;"
        ),
        "pros_cons": "🏆 สั้นและเข้าใจง่ายที่สุด! ซ่อนวิธีทำงานเบื้องหลังไว้ ให้ Query Optimizer เลือกเส้นทางและรันขนานบนหลายซีพียูให้อัตโนมัติ",
        "observations": observations,
        "result": result,
    }


@app.post("/api/ch2/graph/{mode}")
def simulate_ch2_graph(mode: str):
    if mode == "CYPHER_VS_SQL":
        return {
            "question": "โจทย์ในหนังสือ (หน้า 5): หาว่ามีใครบ้างที่ 'เกิดในสหรัฐอเมริกา (USA)' แต่ปัจจุบัน 'อาศัยอยู่ในทวีปยุโรป (Europe)'?",
            "matched_person": "🎯 คำตอบที่ค้นพบ: ลูซี่ (Lucy)!",
            "traversal_steps": [
                "1. เส้นทางเกิดที่ (BORN_IN): ลูซี่ ──► รัฐไอดาโฮ (Idaho) ──► สหรัฐอเมริกา (USA) ✅ (ไล่ขึ้นไป 2 ชั้นเจอ USA!)",
                "2. เส้นทางอาศัยอยู่ที่ (LIVES_IN): ลูซี่ ──► ลอนดอน (London) ──► อังกฤษ (UK) ──► ทวีปยุโรป (Europe) ✅ (ไล่ขึ้นไป 3 ชั้นเจอ Europe!)",
                "3. ส่วน อแลง (Alain): อาศัยอยู่ในยุโรปจริง แต่เกิดที่ฝรั่งเศส (ไม่ใช่ USA) จึงถูกคัดออก",
            ],
            "cypher_code": (
                "// ภาษา Cypher ของกราฟ Neo4j (สั้นแค่ 4 บรรทัด!)\n"
                "MATCH (person) -[:BORN_IN]->  () -[:WITHIN*0..]-> (:Location {name: 'USA'}),\n"
                "      (person) -[:LIVES_IN]-> () -[:WITHIN*0..]-> (:Location {name: 'Europe'})\n"
                "RETURN person.name"
            ),
            "sql_recursive_code": (
                "-- ถ้าฝืนใช้ SQL ทำเรื่องเดียวกัน ต้องใช้ WITH RECURSIVE ยาวถึง ~29 บรรทัด!\n"
                "WITH RECURSIVE\n"
                "  in_usa(vertex_id) AS (\n"
                "    SELECT vertex_id FROM vertices WHERE name = 'USA'\n"
                "    UNION\n"
                "    SELECT edges.tail_vertex FROM edges\n"
                "      JOIN in_usa ON edges.head_vertex = in_usa.vertex_id\n"
                "      WHERE edges.label = 'within'\n"
                "  ),\n"
                "  in_europe(vertex_id) AS (\n"
                "    SELECT vertex_id FROM vertices WHERE name = 'Europe'\n"
                "    UNION\n"
                "    SELECT edges.tail_vertex FROM edges\n"
                "      JOIN in_europe ON edges.head_vertex = in_europe.vertex_id\n"
                "      WHERE edges.label = 'within'\n"
                "  ),\n"
                "  born_in_usa(vertex_id) AS (\n"
                "    SELECT edges.tail_vertex FROM edges\n"
                "      JOIN in_usa ON edges.head_vertex = in_usa.vertex_id\n"
                "      WHERE edges.label = 'born_in'\n"
                "  ),\n"
                "  lives_in_europe(vertex_id) AS (\n"
                "    SELECT edges.tail_vertex FROM edges\n"
                "      JOIN in_europe ON edges.head_vertex = in_europe.vertex_id\n"
                "      WHERE edges.label = 'lives_in'\n"
                "  )\n"
                "SELECT vertices.name\n"
                "FROM vertices\n"
                "JOIN born_in_usa     ON vertices.vertex_id = born_in_usa.vertex_id\n"
                "JOIN lives_in_europe ON vertices.vertex_id = lives_in_europe.vertex_id;"
            ),
            "triples": [
                "(ประธาน: ลูซี่)   ──► [กริยา: อายุ]       ──► (กรรม: 33 ปี)",
                "(ประธาน: ลูซี่)   ──► [กริยา: แต่งงานกับ] ──► (กรรม: อแลง)",
                "(ประธาน: ลูซี่)   ──► [กริยา: เกิดที่]     ──► (กรรม: ไอดาโฮ)",
                "(ประธาน: ไอดาโฮ) ──► [กริยา: อยู่ใน]     ──► (กรรม: สหรัฐอเมริกา)",
                "(ประธาน: ลูซี่)   ──► [กริยา: อาศัยอยู่ที่] ──► (กรรม: ลอนดอน)",
                "(ประธาน: ลอนดอน) ──► [กริยา: อยู่ใน]     ──► (กรรม: อังกฤษ)",
                "(ประธาน: อังกฤษ) ──► [กริยา: อยู่ใน]     ──► (กรรม: ยุโรป)",
            ],
        }
    return {}


# ============================================================================
# CHAPTER 3: STORAGE AND RETRIEVAL INTERACTIVE API
# ============================================================================

bitcask_state = {
    "segment_name": "segment_001.log (กำลังเขียนต่อท้าย Append-Only)",
    "compacted": False,
    "views_counter": 500,
    "log_entries": [
        {"offset": 0,   "size": 28, "key": "video_42",  "value": '{"views": 100}',  "status": "STALE (ค่าเก่าถูกเขียนทับ)"},
        {"offset": 28,  "size": 29, "key": "video_99",  "value": '{"views": 1200}', "status": "LIVE (ค่าล่าสุด)"},
        {"offset": 57,  "size": 28, "key": "video_42",  "value": '{"views": 250}',  "status": "STALE (ค่าเก่าถูกเขียนทับ)"},
        {"offset": 85,  "size": 27, "key": "user_temp", "value": '{"views": 10}',   "status": "LIVE (ค่าล่าสุด)"},
        {"offset": 112, "size": 28, "key": "video_42",  "value": '{"views": 500}',  "status": "LIVE (ค่าล่าสุด)"},
    ],
    "last_action_msg": "สถานะเริ่มต้น: ไฟล์บันทึกแบบต่อท้าย (Append-only Log) มี 5 บรรทัด โดยคีย์ 'video_42' ถูกอัปเดตยอดวิว 3 ครั้ง",
}


def _reset_bitcask():
    bitcask_state["segment_name"] = "segment_001.log (กำลังเขียนต่อท้าย Append-Only)"
    bitcask_state["compacted"] = False
    bitcask_state["views_counter"] = 500
    bitcask_state["log_entries"] = [
        {"offset": 0,   "size": 28, "key": "video_42",  "value": '{"views": 100}',  "status": "STALE (ค่าเก่าถูกเขียนทับ)"},
        {"offset": 28,  "size": 29, "key": "video_99",  "value": '{"views": 1200}', "status": "LIVE (ค่าล่าสุด)"},
        {"offset": 57,  "size": 28, "key": "video_42",  "value": '{"views": 250}',  "status": "STALE (ค่าเก่าถูกเขียนทับ)"},
        {"offset": 85,  "size": 27, "key": "user_temp", "value": '{"views": 10}',   "status": "LIVE (ค่าล่าสุด)"},
        {"offset": 112, "size": 28, "key": "video_42",  "value": '{"views": 500}',  "status": "LIVE (ค่าล่าสุด)"},
    ]
    bitcask_state["last_action_msg"] = "รีเซ็ตกลับสู่สถานะเริ่มต้นเรียบร้อย (มีทั้งค่าเก่าที่ซ้ำซ้อนและคีย์ที่ยังไม่ได้บีบอัด)"


def _build_bitcask_response():
    entries = bitcask_state["log_entries"]
    hash_index = {}
    for e in entries:
        if e["value"] == "🪦 TOMBSTONE (ป้ายหลุมศพ: ลบคีย์นี้)":
            hash_index.pop(e["key"], None)
        else:
            hash_index[e["key"]] = {
                "byte_offset": e["offset"],
                "byte_size": e["size"],
                "points_to_value": e["value"],
            }
    total_bytes = sum(e["size"] for e in entries)
    return {
        "segment_name": bitcask_state["segment_name"],
        "total_bytes_on_disk": total_bytes,
        "total_lines": len(entries),
        "log_entries": entries,
        "hash_index_in_ram": hash_index,
        "last_action_msg": bitcask_state["last_action_msg"],
        "lookup_comparison": {
            "without_index": f"ต้องกวาดอ่านทั้งไฟล์ตั้งแต่ต้นจนจบ ({len(entries)} บรรทัด / O(n)) ถ้าไฟล์ใหญ่ขึ้น 2 เท่า ก็ช้าลง 2 เท่า",
            "with_hash_index": (
                f"ดูตาราง Hash Index ใน RAM พบ 'video_42' อยู่ที่ตำแหน่ง Byte Offset = {hash_index['video_42']['byte_offset']} "
                f"──► กระโดดหัวอ่านดิสก์ไปอ่านค่า {hash_index['video_42']['points_to_value']} ได้ทันทีใน 1 ครั้ง (O(1))!"
            ),
        },
    }


@app.post("/api/ch3/bitcask/{action}")
def simulate_ch3_bitcask(action: str):
    if action == "RESET":
        _reset_bitcask()
        return _build_bitcask_response()

    if action == "WRITE_VIDEO_42":
        bitcask_state["views_counter"] += 150
        new_views = bitcask_state["views_counter"]
        entries = bitcask_state["log_entries"]
        for e in entries:
            if e["key"] == "video_42" and "LIVE" in e["status"]:
                e["status"] = "STALE (ค่าเก่าถูกเขียนทับ)"
        next_offset = sum(e["size"] for e in entries)
        entries.append({
            "offset": next_offset,
            "size": 28,
            "key": "video_42",
            "value": f'{{"views": {new_views}}}',
            "status": "LIVE (ค่าล่าสุดเพิ่งต่อท้ายไฟล์!)",
        })
        bitcask_state["last_action_msg"] = (
            f"✍️ เขียนต่อท้ายไฟล์ (Append-only): เพิ่มยอดวิว 'video_42' เป็น {new_views} ที่ Byte Offset {next_offset} "
            f"และอัปเดตตาราง Hash Index ในหน่วยความจำให้ชี้มาที่ตำแหน่ง {next_offset} ทันที!"
        )
        return _build_bitcask_response()

    if action == "DELETE_USER_TEMP":
        entries = bitcask_state["log_entries"]
        for e in entries:
            if e["key"] == "user_temp":
                e["status"] = "STALE (ถูกสั่งลบด้วยป้ายหลุมศพ)"
        next_offset = sum(e["size"] for e in entries)
        entries.append({
            "offset": next_offset,
            "size": 24,
            "key": "user_temp",
            "value": "🪦 TOMBSTONE (ป้ายหลุมศพ: ลบคีย์นี้)",
            "status": "TOMBSTONE (รอลบจริงตอนรวมไฟล์ Compaction)",
        })
        bitcask_state["last_action_msg"] = (
            f"🪦 ลบคีย์ 'user_temp': เขียนป้ายหลุมศพ (Tombstone) ต่อท้ายที่ Byte Offset {next_offset} "
            "และลบคีย์ 'user_temp' ออกจาก Hash Index ใน RAM ทันที!"
        )
        return _build_bitcask_response()

    if action == "COMPACT":
        old_entries = bitcask_state["log_entries"]
        old_bytes = sum(e["size"] for e in old_entries)
        latest_by_key = {}
        for e in old_entries:
            if e["value"] == "🪦 TOMBSTONE (ป้ายหลุมศพ: ลบคีย์นี้)":
                latest_by_key.pop(e["key"], None)
            else:
                latest_by_key[e["key"]] = e["value"]

        new_entries = []
        offset = 0
        for k, v in latest_by_key.items():
            sz = 28 if k == "video_42" else 29
            new_entries.append({
                "offset": offset,
                "size": sz,
                "key": k,
                "value": v,
                "status": "LIVE (หลังบีบอัด Compaction)",
            })
            offset += sz

        bitcask_state["log_entries"] = new_entries
        bitcask_state["segment_name"] = "segment_002_compacted.log (บีบอัดและรวมไฟล์เสร็จแล้ว!)"
        bitcask_state["compacted"] = True
        bitcask_state["last_action_msg"] = (
            f"🗜️ บีบอัดไฟล์ (Compaction) สำเร็จ! ทิ้งค่าเก่าที่ถูกเขียนทับและป้ายหลุมศพ (Tombstone) ทั้งหมด "
            f"ขนาดไฟล์ลดจาก {old_bytes} ไบต์ เหลือเพียง {offset} ไบต์ ({len(new_entries)} บรรทัด)!"
        )
        return _build_bitcask_response()

    return _build_bitcask_response()


@app.post("/api/ch3/engine/{mode}")
def simulate_ch3_engine(mode: str):
    if mode == "WRITE_COMPARE":
        return {
            "title": "เปรียบเทียบตอน 'เขียนข้อมูลใหม่แบบสุ่ม' (Random Write) — ตอนที่ 4 ถึง ตอนที่ 6",
            "lsm": {
                "name": "🌲 LSM-Tree (RocksDB / LevelDB / Cassandra)",
                "badge": "🚀 เขียนเร็วมาก (Sequential Write + Write Amplification ต่ำ)",
                "steps": [
                    "1. เขียนต่อท้ายไฟล์บันทึกกู้คืน (Write-Ahead Log บนดิสก์) เผื่อไฟดับ",
                    "2. แทรกคีย์ใส่ 'Memtable' (โครงสร้างต้นไม้ในหน่วยความจำ RAM ที่เรียงคีย์อัตโนมัติ) ──► เสร็จทันที!",
                    "3. เมื่อ Memtable เต็ม (เช่นครบไม่กี่ MB) จึงเขียนรวดเดียวลงดิสก์เป็นไฟล์ 'SSTable' (Sorted String Table) ที่เรียงคีย์แล้ว",
                    "4. เบื้องหลังคอยรวมไฟล์ SSTable เล็กเป็นใหญ่ (Size-Tiered หรือ Leveled Compaction)",
                ],
                "verdict": "✅ เหมาะกับงานเขียนหนักๆ เพราะเปลี่ยนการเขียนแบบสุ่มให้เป็นการเรียงใน RAM แล้วเขียนลงดิสก์รวดเดียว และบีบอัดไฟล์ได้แน่นกว่า",
            },
            "btree": {
                "name": "🌳 B-Tree (PostgreSQL / MySQL InnoDB / Oracle)",
                "badge": "⚠️ เขียนทับหน้าเดิม (In-place 4 KB Page + Write Amplification สูง)",
                "steps": [
                    "1. เขียนสิ่งที่กำลังจะแก้ลงไฟล์บันทึกกู้คืน (Write-Ahead Log — WAL) ก่อนเสมอ เพื่อกันต้นไม้พังถ้าไฟดับกลางคัน",
                    "2. ล็อกหน้าข้อมูลด้วยกลไกเบาๆ ที่เรียกว่า 'Latch' เพื่อไม่ให้คนอื่นเห็นต้นไม้ตอนแก้ครึ่งๆ กลางๆ",
                    "3. วิ่งจากราก (Root Page) ลงไปหาหน้าใบ (Leaf Page ขนาดคงที่ 4 KB) แล้วเขียนทับทั้งหน้า 4 KB บนดิสก์!",
                    "4. ถ้าหน้า 4 KB เต็ม ต้อง 'แบ่งหน้าเป็นสองหน้า (Page Split)' แล้วขึ้นไปแก้ตัวชี้ในหน้าพ่อด้วย (ต้องเขียนดิสก์หลายหน้าพร้อมกัน)",
                ],
                "verdict": "⚠️ แก้ข้อมูลแค่ไม่กี่ไบต์ แต่ต้องเขียนทับดิสก์ทั้งหน้า 4 KB (Write Amplification สูงกว่า โดยเฉพาะบน SSD)",
            },
        }

    if mode == "READ_EXISTING":
        return {
            "title": "เปรียบเทียบตอน 'อ่านข้อมูลที่มีอยู่จริง' และการล็อกช่วงคีย์สำหรับธุรกรรม — ตอนที่ 4 ถึง ตอนที่ 6",
            "lsm": {
                "name": "🌲 LSM-Tree (อ่านจาก Memtable ไล่ลงไปหา SSTable เก่า)",
                "badge": "🔍 ใช้ Sparse Index (ดัชนีแบบห่าง เหมือนคำแรกบนหัวหน้าพจนานุกรม)",
                "steps": [
                    "1. ค้นใน 'Memtable' ในหน่วยความจำก่อน ถ้าเจอคือค่าใหม่ล่าสุด จบเลย",
                    "2. ถ้าไม่เจอใน RAM ต้องไปเปิดดูไฟล์ 'SSTable' รุ่นล่าสุดบนดิสก์ โดยดูจากดัชนีแบบห่าง (Sparse Index) แล้วไล่ไปไฟล์ SSTable ที่เก่ากว่า",
                    "3. เนื่องจาก 1 คีย์อาจมีสำเนาตกค้างอยู่ในหลายไฟล์ SSTable (ก่อน Compaction) การล็อกช่วงคีย์และการอ่านจึงอาจช้ากว่า B-Tree",
                    "4. ระวัง: ตอนระบบเบื้องหลังกำลังรวมไฟล์ (Compaction) อาจแย่งดิสก์จนคำขออ่านบางตัวช้ากระตุก (p99 Latency พุ่ง!)",
                ],
                "verdict": "🟡 ประหยัดแรมมากเพราะใช้ดัชนีแบบห่าง (Sparse Index) ไม่ต้องเก็บทุกคีย์ใน RAM แต่ถ้าต้องไล่หลายไฟล์ SSTable จะช้ากว่า B-Tree",
            },
            "btree": {
                "name": "🌳 B-Tree (แบ่งข้อมูลเป็นหน้า 4 KB — 4 ชั้นจุได้ถึง 256 เทราไบต์!)",
                "badge": "🏆 อ่านเร็วและนิ่งสม่ำเสมอ + แต่ละคีย์มีอยู่ที่เดียวในระบบ!",
                "steps": [
                    "1. เริ่มจากหน้าราก (Root Page 4 KB) ──► แต่ละหน้าชี้ไปหน้าลูกได้ถึง 500 หน้า (Branching Factor = 500)",
                    "2. ไล่ลงไปเพียง 3–4 ชั้น ก็เจอหน้าใบ (Leaf Page 4 KB) ที่เก็บข้อมูลจริงทันที (4 ชั้นเก็บข้อมูลได้ถึง 256 TB!)",
                    "3. ทุกคีย์มีอยู่เพียงจุดเดียวแน่นอน ไม่กระจายหลายไฟล์เหมือน LSM-Tree",
                    "4. สามารถแปะล็อกช่วงคีย์ (Range Lock / Predicate Lock) ลงบนตัวต้นไม้ได้โดยตรง ซึ่งสำคัญมากในบทที่ 7 (Transactions)",
                ],
                "verdict": "🏆 ชนะขาดเรื่องความเร็วการอ่านที่คาดเดาได้สม่ำเสมอ และเหมาะกับงานธุรกรรม (OLTP) ที่ต้องล็อกช่วงคีย์",
            },
        }

    # READ_MISSING_BLOOM
    return {
        "title": "ค้นหาคีย์ที่ 'ไม่มีอยู่จริงในระบบ' (เช่น key = 'ghost_9999') — พลังของ Bloom Filter (ตอนที่ 4 หน้า 3)",
        "lsm": {
            "name": "🛡️ LSM-Tree เมื่อเปิดใช้ตัวกรอง 'Bloom Filter' ในหน่วยความจำ",
            "badge": "⚡ ประหยัดดิสก์ 100% (ไม่ต้องเปิดไฟล์ SSTable เลยสักไฟล์!)",
            "steps": [
                "❌ ถ้าไม่มี Bloom Filter: เวลาค้นหาคีย์ที่ไม่มีอยู่จริง ระบบต้องเปิดดู Memtable แล้วไล่เปิดไฟล์ SSTable ทุกไฟล์บนดิสก์จนครบถึงจะรู้ว่าไม่มี!",
                "✅ เมื่อมี Bloom Filter: เป็นโครงสร้างข้อมูลขนาดจิ๋วในหน่วยความจำที่คำนวณแฮชแล้วตอบได้ทันทีว่า 'คีย์นี้ไม่มีอยู่ในระบบแน่นอน 100%!'",
                "🎯 ผลลัพธ์สำหรับคีย์ 'ghost_9999': Bloom Filter บอกว่า 'ไม่มีคีย์นี้แน่นอน' ──► ระบบตอบกลับทันทีโดยไม่ต้องแตะดิสก์แม้แต่ครั้งเดียว (0 Disk Read)!",
            ],
            "verdict": "🏆 Bloom Filter คือหัวใจสำคัญที่ช่วยให้ LSM-Tree (เช่น RocksDB และ Cassandra) ไม่ต้องเสียเวลาไล่เปิดไฟล์ดิสก์ฟรีเมื่อค้นหาคีย์ที่ไม่มีอยู่จริง",
        },
        "btree": {
            "name": "🌳 B-Tree เมื่อค้นหาคีย์ที่ไม่มีอยู่จริง",
            "badge": "📖 เดินตามชั้นต้นไม้ 3–4 ชั้นตามปกติ",
            "steps": [
                "1. เดินจาก Root Page (มักอยู่ในหน่วยความจำอยู่แล้ว) ──► Internal Page ──► Leaf Page",
                "2. เมื่อถึงหน้าใบ (Leaf Page) ที่ครอบคลุมช่วงของ 'ghost_9999' แล้วไม่พบคีย์นั้น ก็สรุปได้ทันทีว่าไม่มีข้อมูล",
                "3. ไม่ต้องไปค้นหน้าอื่นต่อ เพราะแต่ละช่วงคีย์ถูกกำหนดหน้าดูแลไว้ชัดเจนที่เดียว",
            ],
            "verdict": "✅ B-Tree มีช่วงคีย์ชัดเจนในแต่ละหน้าอยู่แล้ว จึงตรวจสอบคีย์ที่ไม่มีอยู่จริงได้ภายใน 3–4 ก้าวเสมอ",
        },
    }


@app.post("/api/ch3/olap/{mode}")
def simulate_ch3_olap(mode: str):
    if mode == "ROW_VS_COLUMN":
        return {
            "title": "เปรียบเทียบการสแกนวิเคราะห์ยอดขาย (OLAP) จากตารางข้อเท็จจริง (Fact Table 100 คอลัมน์) — ตอนที่ 8–9",
            "query": "SELECT store_id, SUM(net_total) FROM fact_sales WHERE date_sk = 20261107 GROUP BY store_id;",
            "left_title": "🐢 เก็บทีละแถว (Row-Oriented Storage — เช่น PostgreSQL / MySQL)",
            "left_badge": "❌ เปลืองแบนด์วิดท์ดิสก์ (ต้องอ่านทั้ง 100 คอลัมน์!)",
            "left_detail": (
                "บนดิสก์: [แถว 1: date_sk | product_sk | store_id | customer_id | ...อีก 95 คอลัมน์... | net_total]\n"
                "         [แถว 2: date_sk | product_sk | store_id | customer_id | ...อีก 95 คอลัมน์... | net_total]\n\n"
                "ปัญหา: คำสั่งวิเคราะห์ต้องการใช้แค่ 3 คอลัมน์ (date_sk, store_id, net_total) "
                "แต่ฐานข้อมูลที่เก็บทีละแถวต้องกวาดอ่านข้อมูลทั้ง 100 คอลัมน์ของทุกแถวขึ้นมาจากดิสก์ แล้วค่อยทิ้ง 97 คอลัมน์ที่ไม่ใช้!"
            ),
            "right_title": "🚀 เก็บทีละคอลัมน์ (Column-Oriented Storage — เช่น ClickHouse / Parquet / Redshift)",
            "right_badge": "✅ อ่านเฉพาะ 3 ไฟล์คอลัมน์ที่ใช้จริง (ประหยัดดิสก์ 97%!)",
            "right_detail": (
                "แยกแต่ละคอลัมน์เป็นไฟล์ของตัวเอง:\n"
                "📁 date_sk.col    : [20261107, 20261107, 20261107, 20261108, ...]  ✅ อ่านไฟล์นี้\n"
                "📁 store_id.col   : [10,       10,       25,       10,       ...]  ✅ อ่านไฟล์นี้\n"
                "📁 net_total.col  : [199.0,    450.0,    89.0,     1200.0,   ...]  ✅ อ่านไฟล์นี้\n"
                "📁 อีก 97 ไฟล์คอลัมน์ที่เหลือ ──► ไม่ต้องเปิดอ่านจากดิสก์เลยแม้แต่ไบต์เดียว!"
            ),
        }

    # BITMAP_RLE
    return {
        "title": "การบีบอัดข้อมูลระดับโหดในคอลัมน์: Bitmap Encoding + Run-Length Encoding (RLE) — ตอนที่ 9 หน้า 7",
        "query": "ตัวอย่างคอลัมน์รหัสสินค้า (product_sk) จำนวน 12 แถวที่เรียงลำดับแล้ว: [69, 69, 69, 69, 69, 69, 30, 30, 30, 31, 31, 31]",
        "left_title": "1️⃣ แปลงเป็นบิตแมป (Bitmap Encoding — แถวของเลข 0 กับ 1 สำหรับแต่ละค่า)",
        "left_badge": "เช็กเงื่อนไข WHERE ด้วย Bitwise AND/OR ได้ทันที",
        "left_detail": (
            "ค่าต้นฉบับ 12 แถว : [69, 69, 69, 69, 69, 69, 30, 30, 30, 31, 31, 31]\n"
            "───────────────────────────────────────────────────────────────────\n"
            "บิตแมป product=69 :  1,  1,  1,  1,  1,  1,  0,  0,  0,  0,  0,  0\n"
            "บิตแมป product=30 :  0,  0,  0,  0,  0,  0,  1,  1,  1,  0,  0,  0\n"
            "บิตแมป product=31 :  0,  0,  0,  0,  0,  0,  0,  0,  0,  1,  1,  1"
        ),
        "right_title": "2️⃣ บีบอัดต่อด้วย Run-Length Encoding (RLE) + Vectorized Processing",
        "right_badge": "🔥 ย่อเลขซ้ำติดกัน + ยัดลง CPU L1 Cache พอดี!",
        "right_detail": (
            "แทนที่จะเขียนเลขศูนย์หรือหนึ่งซ้ำกันเป็นร้อยตัว ก็ยุบเป็น (ค่าบิต, จำนวนที่ซ้ำต่อเนื่อง):\n"
            "───────────────────────────────────────────────────────────────────\n"
            "RLE ของ product=69 : (เลข 1 ซ้ำ 6 ตัว), (เลข 0 ซ้ำ 6 ตัว)\n"
            "RLE ของ product=30 : (เลข 0 ซ้ำ 6 ตัว), (เลข 1 ซ้ำ 3 ตัว), (เลข 0 ซ้ำ 3 ตัว)\n"
            "RLE ของ product=31 : (เลข 0 ซ้ำ 9 ตัว), (เลข 1 ซ้ำ 3 ตัว)\n\n"
            "⚡ Vectorized Processing: ข้อมูลที่บีบอัดแน่นแบบนี้ใส่ในหน่วยความจำแคชของซีพียู (CPU Cache) ได้พอดี "
            "ซีพียูจึงรันคำสั่งประมวลผลทีละก้อนได้เร็วกว่าปกติหลายสิบเท่า!"
        ),
    }


# ============================================================================
# CHAPTER 5: REPLICATION INTERACTIVE API
# ============================================================================


@app.post("/api/ch5/leader/{mode}")
def simulate_ch5_leader(mode: str):
    if mode == "SEMI_SYNC":
        return {
            "title": "การทำสำเนาแบบมีผู้นำคนเดียว (Single-Leader) — แบบกึ่งรอ (Semi-Synchronous) & 4 วิธีส่งรายการเปลี่ยนแปลง (ตอนที่ 2 & 4)",
            "status_badge": "✅ ปลอดภัย + ไม่ช้า: ผู้นำรอผู้ตามแค่ 1 ตัว (Synchronous) ส่วนตัวที่เหลือส่งตามทีหลัง (Asynchronous)",
            "nodes": [
                {
                    "role": "👑 ผู้นำ (Leader — รับคำสั่งเขียนทั้งหมด)",
                    "mode": "Primary Write Node",
                    "state": "เขียนแถว id=5001 ('สมชาย') ลง WAL สำเร็จ",
                    "color": "emerald",
                },
                {
                    "role": "🛡️ ผู้ตามที่ 1 (Follower 1 — แบบรอ Synchronous)",
                    "mode": "ยืนยันก่อนตอบผู้ใช้",
                    "state": "รับ id=5001 และบันทึกสำเร็จแล้ว ✅ (รับประกันมีสำเนาล่าสุดอย่างน้อย 2 เครื่องเสมอ)",
                    "color": "sky",
                },
                {
                    "role": "⚡ ผู้ตามที่ 2 (Follower 2 — แบบไม่รอ Asynchronous)",
                    "mode": "ส่งตามหลัง",
                    "state": "กำลังรับข้อมูลตามหลัง 150 มิลลิวินาที... (ไม่บล็อกผู้ใช้แม้เครือข่ายข้ามทวีปจะช้า)",
                    "color": "amber",
                },
            ],
            "log_methods": [
                "1. ส่งคำสั่งตรงๆ (Statement-based): ส่ง SQL ไปรันซ้ำ — มีปัญหากับ NOW() หรือ RAND() ที่ได้ผลไม่เหมือนกัน",
                "2. ส่งไฟล์บันทึกดิสก์ (Write-Ahead Log — PostgreSQL): ส่งไบต์ระดับดิสก์ — เร็วแต่ผูกติดกับเวอร์ชันฐานข้อมูล",
                "3. ส่งรายการระดับแถว (Logical Row-based Log — MySQL binlog): บอกว่าแถวไหนเพิ่ม/ลบ/แก้ — ไม่ขึ้นกับเวอร์ชัน และทำ Change Data Capture (CDC) ต่อได้ง่าย",
                "4. ใช้ Trigger ในแอป: ยืดหยุ่นที่สุด แต่ช้ากว่าและเสี่ยงเกิดบั๊กง่ายกว่า",
            ],
        }

    if mode == "GITHUB_FAILOVER":
        return {
            "title": "กับดักตอนเปลี่ยนผู้นำ (Failover) แบบไม่รอ: เหตุการณ์จริงที่ GitHub ข้อมูลส่วนตัวรั่วข้ามบัญชี! (ตอนที่ 3 หน้า 2)",
            "status_badge": "💥 อันตราย! ผู้นำเก่าเสียก่อนส่งข้อมูลชุดสุดท้ายออกไป ──► เลขรหัสถูกนำกลับมาใช้ซ้ำ!",
            "nodes": [
                {
                    "role": "💀 ผู้นำเก่า (Old Leader — เครื่องดับกะทันหัน!)",
                    "mode": "ใช้แบบไม่รอ (Asynchronous)",
                    "state": "เขียน id=5001 (ข้อมูลส่วนตัวของ 'สมชาย') ลงเครื่องตัวเองและแคช Redis แล้ว แต่เพิ่งไฟดับก่อนส่งให้ผู้ตาม!",
                    "color": "rose",
                },
                {
                    "role": "👑 ผู้นำคนใหม่ (New Leader — เลื่อนขั้นจากผู้ตามที่ข้อมูลล้าหลัง)",
                    "mode": "ยังไม่ได้รับ id=5001",
                    "state": "คิดว่าเลขล่าสุดคือ 5000! พอ 'สุดา' สมัครเข้ามา จึงแจกเลขซ้ำ id=5001 ให้สุดา!",
                    "color": "amber",
                },
                {
                    "role": "🚨 แคช Redis (ระบบภายนอกที่ใช้เลขรหัสร่วมกัน)",
                    "mode": "ข้อมูลขัดแย้งกัน!",
                    "state": "ใน Redis เลข id=5001 ยังผูกอยู่กับข้อมูลส่วนตัวของ 'สมชาย' ──► สุดาเลยมองเห็นข้อมูลส่วนตัวของสมชายทันที!",
                    "color": "rose",
                },
            ],
            "log_methods": [
                "📌 บทเรียนจากหนังสือ (หน้า 2): ถ้าใช้การทำสำเนาแบบไม่รอ (Asynchronous) ตอนผู้นำล่มและตั้งผู้ตามเป็นผู้นำแทน (Failover) ข้อมูลชุดสุดท้ายที่ยังส่งไม่ถึงมักถูกทิ้งไป",
                "📌 หากมีระบบอื่นข้างนอก (เช่น Redis Cache) เคยเห็นเลขรหัสนั้นไปแล้ว การที่ผู้นำใหม่เอาเลขรหัสเดิมมาแจกซ้ำจะทำให้ข้อมูลข้ามบัญชีอย่างรุนแรง!",
            ],
        }

    # SPLIT_BRAIN
    return {
        "title": "ภาวะสมองแยก (Split Brain) — เมื่อมีผู้นำ 2 คนพร้อมกันในระบบ! (ตอนที่ 3 หน้า 2–3)",
        "status_badge": "🧠⚡ วิกฤตสมองแยก! ต่างฝ่ายต่างคิดว่าตัวเองเป็นผู้นำและรับการเขียนทั้งคู่ ──► ข้อมูลพังยับเยิน!",
        "nodes": [
            {
                "role": "👑 ผู้นำคนเดิม (เครือข่ายกระตุกชั่วคราว ไม่รู้ตัวว่าถูกปลด)",
                "mode": "ยังรับคำสั่งเขียนฝั่งซ้าย!",
                "state": "รับคำสั่งตัดสต็อกสินค้าชิ้นสุดท้ายให้ 'ลูกค้า A' (คิดว่าตัวเองยังเป็นผู้นำอยู่)",
                "color": "rose",
            },
            {
                "role": "👑 ผู้นำคนใหม่ (ถูกแต่งตั้งขึ้นมาเพราะนึกว่าผู้นำเดิมตายแล้ว)",
                "mode": "รับคำสั่งเขียนฝั่งขวา!",
                "state": "รับคำสั่งตัดสต็อกสินค้าชิ้นเดียวกันให้ 'ลูกค้า B' ──► ข้อมูลสองฝั่งขัดแย้งกันและเสียหาย!",
                "color": "rose",
            },
            {
                "role": "🔌 วิธีแก้ในระบบจริง (Fencing / STONITH — ตัดไฟเครื่องเก่าทิ้ง!)",
                "mode": "ป้องกันผู้นำซ้อน",
                "state": "ต้องมีกลไกบังคับให้ผู้นำเก่าหยุดทำงานทันที (เช่นสั่งสวิตช์ไฟตัดไฟเครื่องเก่าทิ้ง หรือใช้หมายเลขยุค Epoch/Fencing Token)",
                "color": "emerald",
            },
        ],
        "log_methods": [
            "📌 กับดักของการตั้งเวลา (Timeout): ถ้าตั้งเวลาจับผู้นำตายสั้นเกินไป แค่เครือข่ายช้าชั่วคราวก็จะเปลี่ยนผู้นำโดยไม่จำเป็น แต่ถ้าตั้งนานเกินไป ระบบก็หยุดรับการเขียนนาน",
            "📌 ด้วยเหตุนี้ ทีมวิศวกรหลายแห่งจึงเลือกให้คนกดเปลี่ยนผู้นำด้วยมือ (Manual Failover) แทนอัตโนมัติ",
        ],
    }


@app.post("/api/ch5/lag/{anomaly}")
def simulate_ch5_lag(anomaly: str):
    if anomaly == "READ_OWN_WRITES":
        return {
            "title": "ปัญหาที่ 1: เขียนแล้วอ่านไม่เจอ ──► แก้ด้วย Read-After-Write Consistency (ตอนที่ 5 หน้า 3)",
            "story": "ผู้ใช้เพิ่งโพสต์ความคิดเห็นหรือแก้โปรไฟล์ตัวเอง (ส่งไปเขียนที่ 'ผู้นำ') แล้วกดรีเฟรชหน้าเว็บทันที แต่คำขออ่านดันถูกส่งไปหา 'ผู้ตาม' ที่ยังไม่ได้รับข้อมูล!",
            "what_user_sees_bad": "😱 มุมมองผู้ใช้ (ตอนพัง): 'อ้าว! ความคิดเห็นที่ฉันเพิ่งโพสต์เมื่อกี้หายไปไหน? ระบบพังหรือโพสต์ไม่สำเร็จ?!'",
            "timeline": [
                "1. [เขียน] ผู้ใช้ส่งคอมเมนต์ใหม่ ──► บันทึกลง 'ผู้นำ (Leader)' สำเร็จแล้ว",
                "2. [อ่านทันที] ผู้ใช้กดรีเฟรชหน้าจอ ──► ระบบสุ่มส่งคำขออ่านไปที่ 'ผู้ตามที่ 2' ซึ่งยังล้าหลังอยู่ 2 วินาที",
                "3. [ผลลัพธ์] ผู้ตามที่ 2 ตอบกลับมาว่ายังไม่มีคอมเมนต์นั้น ผู้ใช้จึงไม่เห็นสิ่งที่ตัวเองเพิ่งเขียน!",
            ],
            "solution_name": "✅ วิธีแก้: Read-After-Write Consistency (รับประกันว่าผู้ใช้ต้องเห็นสิ่งที่ตัวเองเพิ่งเขียนเสมอ)",
            "solution_steps": [
                "• วิธีที่ 1: ข้อมูลที่เจ้าตัวแก้ไขได้คนเดียว (เช่น โปรไฟล์ของตัวเอง) ให้ดึงอ่านจาก 'ผู้นำ (Leader)' เสมอ ส่วนคนอื่นดูโปรไฟล์เราค่อยอ่านจากผู้ตาม",
                "• วิธีที่ 2: ภายใน 1 นาทีหลังจากผู้ใช้มีการแก้ไขข้อมูล ให้ส่งคำขออ่านของผู้ใช้คนนั้นไปที่ 'ผู้นำ' ชั่วคราว",
                "• วิธีที่ 3: ให้ฝั่งผู้ใช้จำเวลาที่เขียนล่าสุดไว้ แล้วเลือกอ่านเฉพาะจากสำเนาที่อัปเดตมาถึงเวลานั้นแล้ว",
            ],
        }

    if anomaly == "MONOTONIC_READS":
        return {
            "title": "ปัญหาที่ 2: ข้อมูลย้อนเวลา (Moving Backward in Time) ──► แก้ด้วย Monotonic Reads (ตอนที่ 5 หน้า 4)",
            "story": "ผู้ใช้กดรีเฟรชหน้าเว็บครั้งแรกเห็นความคิดเห็นใหม่ของเพื่อนแล้ว แต่พอกดรีเฟรชครั้งที่สอง ความคิดเห็นนั้นกลับหายวับไปเหมือนย้อนเวลา!",
            "what_user_sees_bad": "👻 มุมมองผู้ใช้ (ตอนหลอน): 'รีเฟรชรอบแรกเห็นคอมเมนต์ใหม่แล้วนะ พอรีเฟรชอีกรอบ ทำไมคอมเมนต์หายไปเฉยเลย?!'",
            "timeline": [
                "1. [รีเฟรชครั้งที่ 1] คำขออ่านไปตกที่ 'ผู้ตามที่ 1' (ซึ่งตามทันผู้นำแล้ว) ──► ผู้ใช้มองเห็นคอมเมนต์ใหม่เรียบร้อย",
                "2. [รีเฟรชครั้งที่ 2] คำขออ่านถูกสุ่มไปตกที่ 'ผู้ตามที่ 2' (ซึ่งยังล้าหลังผู้นำอยู่!) ──► คอมเมนต์ใหม่ยังไม่มาถึงเครื่องนี้",
                "3. [ผลลัพธ์] ผู้ใช้เห็นข้อมูลเก่ากว่าที่ตัวเองเคยเห็นเมื่อกี้ ราวกับเวลาเดินถอยหลัง!",
            ],
            "solution_name": "✅ วิธีแก้: Monotonic Reads (เมื่อเห็นข้อมูลใหม่แล้ว ต้องไม่เห็นข้อมูลที่เก่ากว่านั้นอีก)",
            "solution_steps": [
                "• วิธีทำที่ง่ายและได้ผลชะงัด: บังคับให้ผู้ใช้แต่ละคนอ่านข้อมูลจาก 'สำเนาเครื่องเดิมเสมอ' (เช่น เอา User ID มาคำนวณแฮชเลือกเครื่องสำเนาประจำตัว ไม่ใช่สุ่มเครื่องใหม่ทุกครั้งที่กดรีเฟรช)",
                "• ถ้าสำเนาเครื่องนั้นเสีย ค่อยย้ายผู้ใช้ไปอ่านจากสำเนาเครื่องอื่น",
            ],
        }

    # CONSISTENT_PREFIX
    return {
        "title": "ปัญหาที่ 3: เห็นคำตอบก่อนคำถาม ('คุณพูนส์ กับ คุณเค้ก มองเห็นอนาคต') ──► แก้ด้วย Consistent Prefix Reads (ตอนที่ 5 หน้า 4)",
        "story": "ตัวอย่างสุดคลาสสิกในหนังสือ: คุณพูนส์ถามคำถาม แล้วคุณเค้กตอบกลับ แต่สำเนาของผู้สังเกตการณ์ดันได้รับข้อความคำตอบของคุณเค้กก่อนคำถามของคุณพูนส์!",
        "what_user_sees_bad": (
            "🔮 มุมมองผู้สังเกตการณ์ (เห็นคำตอบขึ้นก่อนคำถาม!):\n"
            "   บรรทัดที่ 1 — คุณเค้ก: 'ปกติก็ประมาณสิบวินาทีค่ะ คุณพูนส์'\n"
            "   บรรทัดที่ 2 — คุณพูนส์: 'คุณเค้ก คุณมองเห็นอนาคตได้ไกลแค่ไหน?'"
        ),
        "timeline": [
            "1. คุณพูนส์ส่งคำถาม: 'คุณเค้ก คุณมองเห็นอนาคตได้ไกลแค่ไหน?' (เก็บในส่วนข้อมูลชุดที่ 1 — เดินทางช้า)",
            "2. คุณเค้กเห็นคำถามแล้วส่งคำตอบ: 'ปกติก็ประมาณสิบวินาทีค่ะ คุณพูนส์' (เก็บในส่วนข้อมูลชุดที่ 2 — เดินทางเร็วกว่า!)",
            "3. ผู้สังเกตการณ์จึงเห็นคำตอบของคุณเค้กโผล่ขึ้นมาก่อนคำถามของคุณพูนส์ ราวกับคุณเค้กมีญาณทิพย์มองเห็นอนาคตจริงๆ!",
        ],
        "solution_name": "✅ วิธีแก้: Consistent Prefix Reads (ถ้าข้อมูลถูกเขียนตามลำดับเหตุและผล คนอ่านต้องเห็นตามลำดับนั้นเสมอ)",
        "solution_steps": [
            "• จัดให้ข้อมูลที่เกี่ยวข้องกันเป็นเหตุเป็นผลกัน (เช่น ข้อความในห้องแชตเดียวกัน หรือกระทู้เดียวกัน) ถูกเขียนลงในส่วนข้อมูล (Partition) เดียวกันเสมอ",
            "• หรือใช้กลไกติดตามลำดับเหตุและผล (Causal Dependency / Version Vector) เพื่อให้สำเนารอแสดงคำถามก่อนคำตอบเสมอ",
        ],
    }


@app.post("/api/ch5/dynamo/{mode}")
def simulate_ch5_dynamo(mode: str):
    if mode == "QUORUM_VALID":
        return {
            "title": "แบบไม่มีผู้นำ (Leaderless / Dynamo) & กฎเสียงข้างมาก Quorum: n = 3, w = 2, r = 2 (ตอนที่ 7 หน้า 5–6)",
            "formula_verdict": "✅ w + r > n  (2 + 2 = 4 > 3) ──► รับประกันว่ากลุ่มเครื่องที่อ่านต้องซ้อนทับกับกลุ่มที่เขียนอย่างน้อย 1 เครื่องเสมอ!",
            "replicas": [
                {"name": "Replica 1", "write_state": "✅ เขียนสำเร็จ (v2: ค่าใหม่)", "read_state": "📖 ถูกอ่าน (ได้ v2 ใหม่ล่าสุด!)"},
                {"name": "Replica 2", "write_state": "✅ เขียนสำเร็จ (v2: ค่าใหม่)", "read_state": "— (ไม่ได้อ่าน)"},
                {"name": "Replica 3 (เพิ่งฟื้นจากไฟดับ)", "write_state": "❌ ตอนเขียนเครื่องดับอยู่ (ยังมีแค่ v1 เก่า)", "read_state": "📖 ถูกอ่าน (เจอ v1 เก่า ──► สั่งซ่อม Read Repair ทันที!)"},
            ],
            "explanation": (
                "เมื่อผู้อ่านดึงจาก r=2 เครื่อง (Replica 1 ได้ v2 และ Replica 3 ได้ v1) ผู้อ่านจะเปรียบเทียบหมายเลขเวอร์ชันแล้วรู้ทันทีว่า v2 ใหม่กว่า "
                "พร้อมส่งค่า v2 กลับไปซ่อมให้ Replica 3 อัตโนมัติ (กระบวนการนี้เรียกว่า Read Repair)!"
            ),
        }

    if mode == "SLOPPY_QUORUM":
        return {
            "title": "Sloppy Quorum & Hinted Handoff — 'ลืมกุญแจบ้าน เลยไปขอนอนโซฟาบ้านเพื่อนบ้านชั่วคราว' (ตอนที่ 7 หน้า 6)",
            "formula_verdict": "🛋️ เน้นให้เขียนผ่านได้เสมอแม้เครือข่ายขาด (High Write Availability) แต่ช่วงนั้นยังไม่อ่านเจอใน Quorum ปกติ",
            "replicas": [
                {"name": "Replica 1 (บ้านจริง)", "write_state": "✅ เขียนสำเร็จ", "read_state": "เก็บข้อมูลจริง"},
                {"name": "Replica 2 (บ้านจริง — เครือข่ายขาด!)", "write_state": "❌ ติดต่อไม่ได้ชั่วคราว", "read_state": "รอรับข้อมูลคืนเมื่อเครือข่ายกลับมา"},
                {"name": "Node 4 (โซฟาบ้านเพื่อนบ้าน!)", "write_state": "📝 รับฝากข้อมูลชั่วคราว + แปะโน้ต (Hint) ว่าของ Replica 2", "read_state": "พอเครือข่ายหายดี จะส่งคืนให้ Replica 2 (Hinted Handoff)"},
            ],
            "explanation": (
                "ตรงกับคำเปรียบเทียบในหนังสือหน้า 6: 'เหมือนลืมกุญแจบ้าน เลยไปขออาศัยนอนโซฟาบ้านเพื่อนบ้านก่อน พอได้กุญแจแล้วค่อยกลับบ้าน' "
                "ช่วยให้ระบบรับการเขียนได้ไม่สะดุดแม้เครื่องหลักจะติดต่อไม่ได้ครบ w เครื่อง"
            ),
        }

    if mode == "CART_LWW_LOST":
        return {
            "title": "การเขียนพร้อมกันในตะกร้าสินค้า: ถ้าใช้ Last Write Wins (LWW — เวลาล่าสุดชนะ) ของในตะกร้าจะหายเงียบๆ! (ตอนที่ 8 หน้า 7)",
            "formula_verdict": "❌ อันตราย! การเขียนที่สำเร็จแล้วถูกทับทิ้งไปเงียบๆ โดยที่ผู้ใช้ไม่รู้ตัวเลย",
            "replicas": [
                {"name": "1. ลูกค้าคนที่ 1 (กดใส่นม)", "write_state": "อ่านตะกร้าว่าง [] ──► ส่ง ['นม'] (เวลา 12:00:00.001)", "read_state": "ระบบตอบว่าบันทึกสำเร็จแล้ว ✅"},
                {"name": "2. ลูกค้าคนที่ 2 (กดใส่ไข่พร้อมกัน)", "write_state": "อ่านตะกร้าว่าง [] ──► ส่ง ['ไข่'] (เวลา 12:00:00.002)", "read_state": "ระบบตอบว่าบันทึกสำเร็จแล้ว ✅"},
                {"name": "3. ผลลัพธ์แบบ LWW (เวลาล่าสุดชนะ)", "write_state": "12:00:00.002 > 12:00:00.001", "read_state": "😱 ตะกร้าเหลือแค่ ['ไข่'] ส่วน 'นม' ถูกลบทิ้งหายไปไร้ร่องรอย!"},
            ],
            "explanation": (
                "การเขียนพร้อมกันไม่ได้แปลว่าเกิดวินาทีเดียวกันเป๊ะ แต่หมายถึง 'ต่างฝ่ายต่างไม่รู้ว่าอีกฝ่ายเกิดขึ้น' "
                "ถ้าใช้แค่เวลาจากนาฬิกาตัดสินว่าใครชนะ (LWW) ข้อมูลของอีกคนจะสูญหายทันที"
            ),
        }

    # CART_SIBLING_MERGE
    return {
        "title": "ทางออกที่ถูกต้อง: ใช้หมายเลขเวอร์ชัน (Version Vector) เก็บค่าพี่น้อง (Sibling) แล้วรวมตะกร้าเข้าด้วยกัน! (ตอนที่ 8 หน้า 7)",
        "formula_verdict": "🏆 ไม่เสียของแม้แต่ชิ้นเดียว! ระบบตรวจพบการเขียนพร้อมกัน จึงเก็บทั้งคู่ไว้เป็นค่าพี่น้อง (Sibling) แล้วรวมกันได้ ['นม', 'ไข่']",
        "replicas": [
            {"name": "1. ลูกค้าคนที่ 1 (เห็นเวอร์ชัน v1)", "write_state": "ส่ง ['นม'] พร้อมอ้างอิงเวอร์ชันที่ตัวเองเห็น (v1)", "read_state": "เซิร์ฟเวอร์บันทึกเป็น v2_A = ['นม']"},
            {"name": "2. ลูกค้าคนที่ 2 (เห็นเวอร์ชัน v1 เช่นกัน)", "write_state": "ส่ง ['ไข่'] พร้อมอ้างอิงเวอร์ชันที่ตัวเองเห็น (v1)", "read_state": "เซิร์ฟเวอร์รู้ทันทีว่าคนนี้ยังไม่เคยเห็น v2_A จึงเก็บคู่กันเป็น Sibling (v2_B = ['ไข่'])!"},
            {"name": "3. โค้ดของแอปรวมค่าพี่น้อง (Merge Siblings)", "write_state": "รวมเซต ['นม'] ∪ ['ไข่']", "read_state": "🛒 ตะกร้าสุดท้ายมีครบทั้ง ['นม', 'ไข่'] (เว้นแต่มีป้ายหลุมศพ Tombstone สั่งลบ)!"},
        ],
        "explanation": (
            "เซิร์ฟเวอร์ดูจากหมายเลขเวอร์ชัน (หรือ Version Vector เมื่อมีหลายสำเนา) จึงแยกแยะได้ว่าอันไหนเกิดก่อน-หลัง (Happens-before) "
            "และอันไหนเกิดพร้อมกัน (Concurrent) แล้วเก็บเป็นค่าพี่น้อง (Sibling) ให้แอปรวมตะกร้าได้อย่างปลอดภัย!"
        ),
    }


# ============================================================================
# CHAPTER 7: TRANSACTIONS INTERACTIVE API
# ============================================================================


@app.post("/api/ch7/race/{case_id}")
def simulate_ch7_race(case_id: str):
    if case_id == "DIRTY_READ":
        return {
            "title": "1. ปัญหาการอ่านข้อมูลที่ยังไม่สมบูรณ์ (Dirty Read) — ตอนที่ 3 หน้า 2",
            "badge": "❌ อ่านเจอตัวเลขปลอมที่ยังไม่ได้กดบันทึก (Uncommitted Data)",
            "initial_state": "ยอดเงินในบัญชีตั้งต้น = 1,000 บาท",
            "steps": [
                "ขั้นที่ 1 [ธุรกรรม A]: กำลังแก้ยอดเงินเป็น 50,000 บาท แต่ยังไม่กดบันทึก (ยังไม่ Commit)",
                "ขั้นที่ 2 [ธุรกรรม B]: เข้ามาอ่านยอดเงินจังหวะนั้นพอดี ──► เห็นตัวเลข 50,000 บาท แล้วนำไปอนุมัติวงเงินต่อ!",
                "ขั้นที่ 3 [ธุรกรรม A]: เกิดทำพลาดในขั้นตอนถัดไป จึงสั่งยกเลิกทั้งหมด (Abort / Rollback กลับเป็น 1,000 บาท)",
            ],
            "final_outcome": "💥 พัง! ธุรกรรม B เอาตัวเลขปลอม (50,000 บาท) ที่ไม่มีอยู่จริงไปใช้งานต่อแล้ว!",
            "fix_level": "🛡️ ป้องกันได้ด้วยระดับขั้นต่ำสุด: Read Committed (ห้ามใครอ่านข้อมูลที่ยังไม่ Commit เด็ดขาด)",
        }

    if case_id == "READ_SKEW":
        return {
            "title": "2. ปัญหาการอ่านซ้ำแล้วได้ค่าไม่เหมือนกัน (Non-Repeatable Read / Read Skew) — โอนเงิน 100 บาทระหว่าง 2 บัญชี (หน้า 2)",
            "badge": "👀 เงินหายไปไหน 100 บาทต่อหน้าต่อตา?! (อ่านเจอจังหวะโอนครึ่งทาง)",
            "initial_state": "ผู้ใช้มี 2 บัญชี: บัญชีที่ 1 = 500 บาท, บัญชีที่ 2 = 500 บาท (ยอดรวมต้องเป็น 1,000 บาทเสมอ)",
            "steps": [
                "ขั้นที่ 1 [ธุรกรรมโอนเงิน A]: หักเงินจาก 'บัญชีที่ 1' ไป 100 บาท (บัญชีที่ 1 เหลือ 400 บาท)",
                "ขั้นที่ 2 [ผู้ใช้เปิดแอปดูยอดรวม]: อ่าน 'บัญชีที่ 1' ได้ 400 บาท + อ่าน 'บัญชีที่ 2' (ที่ยังไม่ได้บวกเงิน) ได้ 500 บาท!",
                "ขั้นที่ 3 [ธุรกรรมโอนเงิน A]: เติมเงินเข้า 'บัญชีที่ 2' อีก 100 บาท เป็น 600 บาท แล้วกด Commit",
            ],
            "final_outcome": "😱 ตอนผู้ใช้เปิดดูยอดรวม เห็นเงินรวมเหลือแค่ 400 + 500 = 900 บาท! (ทั้งที่เงินไม่ได้หาย แต่เปิดมาดูจังหวะที่โอนยังทำไม่เสร็จ)",
            "fix_level": "🛡️ ป้องกันได้ด้วยระดับ: Snapshot Isolation (Repeatable Read ด้วย MVCC ถ่ายภาพโลกส่วนตัว ณ เวลาเริ่มต้น)",
        }

    if case_id == "LOST_UPDATE":
        return {
            "title": "3. ปัญหาการเขียนทับซ้อนกัน (Lost Update) — พนักงาน 2 คนบวกเลขจาก 10 พร้อมกัน (หน้า 2)",
            "badge": "🗑️ บวกเพิ่ม 2 ครั้งจาก 10 ควรเป็น 12 แต่เหลือแค่ 11!",
            "initial_state": "ตัวเลขในฐานข้อมูลตั้งต้น = 10",
            "steps": [
                "ขั้นที่ 1 [พนักงานคนที่ 1 และ คนที่ 2]: เปิดหน้าจออ่านข้อมูลพร้อมกัน ──► ทั้งคู่เห็นตัวเลข = 10",
                "ขั้นที่ 2 [พนักงานคนที่ 1]: คำนวณ 10 + 1 = 11 แล้วกดบันทึก (ฐานข้อมูลเป็น 11)",
                "ขั้นที่ 3 [พนักงานคนที่ 2]: คำนวณจากค่าที่ตัวเองอ่านมาตอนแรก (10 + 1 = 11) แล้วกดบันทึกทับลงไป!",
            ],
            "final_outcome": "❌ ยอดที่ควรจะเพิ่มเป็น 12 กลับเหลือแค่ 11 เพราะการแก้ไขของพนักงานคนแรกถูกเขียนทับหายไป!",
            "fix_level": "🛡️ ป้องกันด้วย: Atomic Update (SET val = val + 1), การล็อกแถว (SELECT FOR UPDATE), หรือตรวจจับอัตโนมัติใน Snapshot Isolation / Serializable",
        }

    # WRITE_SKEW
    return {
        "title": "4. ปัญหาหมอเวร 2 คนกดขอลาหยุดพร้อมกัน (Write Skew & Phantom Reads) — ตอนที่ 3 หน้า 2",
        "badge": "🏥 กฎโรงพยาบาลต้องมีหมอเข้าเวรอย่างน้อย 2 คน แต่สุดท้ายเหลือหมอ 0 คน!",
        "initial_state": "กฎเหล็ก (Consistency): ต้องมีแพทย์เข้าเวรอย่างน้อย 2 คนเสมอ | ปัจจุบันมี 'หมอสมชาย' และ 'หมอสุดา' เข้าเวรอยู่ (รวม 2 คนพอดี)",
        "steps": [
            "ขั้นที่ 1 [หมอสมชาย & หมอสุดา กดขอลาพร้อมกัน]: ทั้งคู่สั่ง SELECT COUNT(*) เช็กจำนวนหมอเวร ──► ต่างคนต่างเห็นว่ามีอยู่ 2 คนพอดี!",
            "ขั้นที่ 2 [ตรวจสอบเงื่อนไข]: โค้ดของทั้งคู่เห็นว่า 'ถ้าฉันลาออก 1 คน ก็ยังเหลือหมออีก 1 คน (หรือเข้าใจว่ายังผ่านเงื่อนไข)'",
            "ขั้นที่ 3 [บันทึกคนละแถว!]: หมอสมชายแก้แถวตัวเองเป็น 'ลาหยุด' | หมอสุดาแก้แถวตัวเองเป็น 'ลาหยุด' แล้วกด Commit พร้อมกัน!",
        ],
        "final_outcome": "🚨 โรงพยาบาลไม่มีแพทย์อยู่เวรเลยสักคน (เหลือ 0 คน)! แม้เปิดใช้ Snapshot Isolation ก็กันไม่ได้ เพราะทั้งคู่แก้คนละแถวกัน!",
        "fix_level": "🏆 ป้องกันได้เฉพาะระดับสูงสุดเท่านั้น: Serializable (ด้วย 2PL ล็อกช่วงข้อมูล หรือ SSI ตรวจจับการอ่านที่ล้าสมัย)",
    }


@app.post("/api/ch7/isolation/{level}")
def simulate_ch7_isolation(level: str):
    if level == "READ_COMMITTED":
        return {
            "level_name": "ระดับที่ 1: Read Committed (ระดับเริ่มต้นของ PostgreSQL, SQL Server, Oracle)",
            "mechanism": "เบื้องหลัง: ตอนเขียนจะล็อกแถวไว้ (Row-level lock) ส่วนถ้ามีคนมาขออ่านระหว่างนั้น ระบบจะส่ง 'ค่าเก่าก่อนแก้' ให้คนอ่านไปก่อน จะได้ไม่ต้องรอ",
            "matrix": [
                {"problem": "1. Dirty Read (อ่านข้อมูลที่ยังไม่ Commit)", "status": "✅ ป้องกันได้เด็ดขาด"},
                {"problem": "2. Dirty Write (เขียนทับข้อมูลที่ยังไม่ Commit)", "status": "✅ ป้องกันได้เด็ดขาด"},
                {"problem": "3. Non-Repeatable Read / Read Skew (โอนเงิน 2 บัญชีเห็น 900 บาท)", "status": "❌ ป้องกันไม่ได้!"},
                {"problem": "4. Lost Update (พนักงาน 2 คนบวกเลขทับกัน)", "status": "❌ ป้องกันไม่ได้!"},
                {"problem": "5. Write Skew (หมอเวร 2 คนกดลาหยุดพร้อมกัน)", "status": "❌ ป้องกันไม่ได้!"},
            ],
        }

    if level == "SNAPSHOT_ISOLATION":
        return {
            "level_name": "ระดับที่ 2: Snapshot Isolation / Repeatable Read (ใช้กลไก MVCC — เก็บข้อมูลหลายเวอร์ชัน)",
            "mechanism": "เบื้องหลัง (MVCC): เมื่อธุรกรรมเริ่มขึ้น ระบบจะถ่ายภาพ (Snapshot) ณ เวลานั้นไว้ให้ธุรกรรมมองเห็นแต่โลกใบเดิมตอนเริ่มต้น คนอ่านไม่ต้องบล็อกคนเขียน และคนเขียนไม่ต้องบล็อกคนอ่าน!",
            "matrix": [
                {"problem": "1. Dirty Read (อ่านข้อมูลที่ยังไม่ Commit)", "status": "✅ ป้องกันได้เด็ดขาด"},
                {"problem": "2. Dirty Write (เขียนทับข้อมูลที่ยังไม่ Commit)", "status": "✅ ป้องกันได้เด็ดขาด"},
                {"problem": "3. Non-Repeatable Read / Read Skew (โอนเงิน 2 บัญชีเห็น 900 บาท)", "status": "✅ ป้องกันได้เด็ดขาด! (เห็นครบ 1,000 บาทเสมอ)"},
                {"problem": "4. Lost Update (พนักงาน 2 คนบวกเลขทับกัน)", "status": "🟡 บางระบบตรวจจับให้อัตโนมัติ (เช่น PostgreSQL) แต่บางระบบต้องล็อกเอง"},
                {"problem": "5. Write Skew (หมอเวร 2 คนกดลาหยุดพร้อมกัน)", "status": "❌ ป้องกันไม่ได้! (ต้องใช้ Serializable เท่านั้น)"},
            ],
        }

    # SERIALIZABLE
    return {
        "level_name": "ระดับที่ 3 (สูงสุด): Serializable (ปลอดภัยไร้ที่ติ เหมือนรันทีละตัวต่อแถวกันเป๊ะๆ!)",
        "mechanism": "เบื้องหลัง: รับประกันว่าผลลัพธ์สุดท้ายของการรันธุรกรรมหลายตัวพร้อมกัน จะถูกต้องเหมือนรันทีละอันเรียงคิวกัน 100% ป้องกันช่องโหว่ทุกชนิดรวมถึงหมอเวรลาหยุดพร้อมกัน!",
        "matrix": [
            {"problem": "1. Dirty Read (อ่านข้อมูลที่ยังไม่ Commit)", "status": "✅ ป้องกันได้เด็ดขาด"},
            {"problem": "2. Dirty Write (เขียนทับข้อมูลที่ยังไม่ Commit)", "status": "✅ ป้องกันได้เด็ดขาด"},
            {"problem": "3. Non-Repeatable Read / Read Skew (โอนเงิน 2 บัญชีเห็น 900 บาท)", "status": "✅ ป้องกันได้เด็ดขาด"},
            {"problem": "4. Lost Update (พนักงาน 2 คนบวกเลขทับกัน)", "status": "✅ ป้องกันได้เด็ดขาด"},
            {"problem": "5. Write Skew & Phantom Reads (หมอเวร 2 คนกดลาหยุดพร้อมกัน)", "status": "🏆 ป้องกันได้เด็ดขาด 100%!"},
        ],
    }


@app.post("/api/ch7/serializable/{method}")
def simulate_ch7_serializable(method: str):
    if method == "ACTUAL_SERIAL":
        return {
            "name": "วิธีที่ 1: รันทีละคำสั่งจริงๆ บนเธรดเดียว (Actual Serial Execution — เช่น Redis, VoltDB)",
            "badge": "⚡ เร็วมากถ้าข้อมูลทั้งหมดอยู่ใน RAM และธุรกรรมสั้นนิดเดียว",
            "how_it_works": [
                "• เลิกให้ซีพียูหลายหัวแย่งกันแก้ข้อมูลชุดเดียวกัน แต่จัดคิวให้มีเธรดเดียว (Single-threaded) ประมวลผลธุรกรรมทีละอันต่อแถวกันไปเลย",
                "• ไม่ต้องเสียเวลาตั้งล็อก (Locking overhead = 0) เพราะไม่มีใครมาแย่งระหว่างทำอยู่แล้ว",
                "• ข้อจำกัด: ปริมาณงานถูกจำกัดด้วยความเร็วของซีพียูหัวเดียว (เว้นแต่แบ่งส่วนข้อมูล Partition แยกกัน) และถ้ามีธุรกรรมไหนรันช้า งานข้างหลังจะค้างรอทั้งหมดทันที!",
            ],
        }

    if method == "TWO_PHASE_LOCKING":
        return {
            "name": "วิธีที่ 2: การล็อกแบบสองขั้นตอน (Two-Phase Locking — 2PL) — มาตรฐานดั้งเดิมหลายสิบปี",
            "badge": "🔒 ปลอดภัยแต่ช้า และเสี่ยงติดล็อกตายหมู่ (Deadlock!)",
            "how_it_works": [
                "• กฎเหล็กของ 2PL: ถ้าจะอ่านต้องขอ 'Shared Lock' (คนอื่นอ่านได้แต่ห้ามแก้) ถ้าจะแก้ต้องขอ 'Exclusive Lock' (ห้ามใครอ่านหรือแก้เด็ดขาดจนกว่าจะจบธุรกรรม!)",
                "• คนอ่านบล็อกคนเขียน และคนเขียนก็บล็อกคนอ่าน!",
                "• กับดัก Deadlock (ติดล็อกตายหมู่): ธุรกรรม A ถือล็อกชิ้นที่ 1 แล้วรอชิ้นที่ 2 ส่วนธุรกรรม B ถือล็อกชิ้นที่ 2 แล้วรอชิ้นที่ 1 ──► ยืนจ้องตากันไปไหนไม่ได้ จนฐานข้อมูลต้องสั่งฆ่า (Abort) ฝ่ายหนึ่งทิ้ง!",
            ],
        }

    # SSI
    return {
        "name": "วิธีที่ 3: แบบมองโลกในแง่ดี (SSI — Serializable Snapshot Isolation — เช่นใน PostgreSQL ยุคใหม่)",
        "badge": "🏆 ไม่ต้องล็อกรอตอนอ่าน! ค่อยตรวจจับตอนจะกดบันทึก (Commit)",
        "how_it_works": [
            "• ปล่อยให้ทุกธุรกรรมอ่านและทำงานไปตามสบายด้วย Snapshot Isolation (MVCC) โดยไม่ต้องล็อกรอใครเลย",
            "• เบื้องหลังฐานข้อมูลคอยแอบจดบันทึกไว้ว่า ธุรกรรมไหนอ่านข้อมูลอะไรไปใช้ตัดสินใจบ้าง (เช่น หมอสมชายและหมอสุดาต่างก็อ่านตารางเวรแพทย์)",
            "• ตอนกดบันทึก (Commit): หมอคนแรกบันทึกผ่านฉลุย พอหมอคนที่สองจะกดบันทึก ระบบตรวจพบทันทีว่า 'ข้อมูลเวรแพทย์ที่คุณเคยอ่านตอนต้นถูกเปลี่ยนไปแล้ว!' จึงสั่งยกเลิก (Abort) คนที่สองให้ไปเริ่มทำใหม่ ──► โรงพยาบาลปลอดภัยและระบบไม่ค้าง!",
        ],
    }


# ============================================================================
# CHAPTER 8: THE TROUBLE WITH DISTRIBUTED SYSTEMS INTERACTIVE API
# ============================================================================


@app.post("/api/ch8/network/{mode}")
def simulate_ch8_network(mode: str):
    if mode == "THREE_MYSTERIES":
        return {
            "title": "1. ปัญหาคลาสสิกของเครือข่ายแบบไม่ประสานเวลา (Asynchronous Network) — ตอนที่ 2 หน้า 1–2",
            "badge": "❓ ส่งข้อความไปแล้วเงียบกริบ... เราไม่มีทางรู้เลยว่าเกิดอะไรขึ้น!",
            "scenarios": [
                "กรณีที่ 1 (คำขอหายกลางทาง): ข้อความของเรายังเดินทางไปไม่ถึงอีกเครื่องด้วยซ้ำ (อีกฝั่งยังไม่ทำอะไรเลย)",
                "กรณีที่ 2 (คิวคับคั่ง Congestion): อีกฝ่ายได้รับข้อความแล้ว กำลังประมวลผลอยู่แต่ช้ามาก หรือข้อมูลไปกองรวมกันจนเครือข่ายหน่วง",
                "กรณีที่ 3 (คำตอบขากลับหาย!): อีกฝ่ายประมวลผลและตัดเงินเสร็จเรียบร้อยแล้ว! แต่ตอนส่งคำตอบกลับมาระหว่างทาง คำตอบดันหล่นหายบนเครือข่าย!",
            ],
            "outcome": "⚠️ ความคลุมเครือนี้ทำให้ระบบกระจายปวดหัวที่สุด เพราะเราแยกไม่ออกเลยว่า 'อีกฝั่งพังไปแล้ว' หรือ 'ทำเสร็จแล้วแต่คำตอบหาย'",
            "takeaway": "ทางแก้มาตรฐานคือตั้งเวลารอ (Timeout) แล้วลองส่งใหม่ (Retry) แต่การส่งใหม่จะนำมาซึ่งกับดักการตัดเงินซ้ำในปุ่มที่ 2!",
        }

    if mode == "RETRY_DOUBLE_CHARGE":
        return {
            "title": "2. กับดักของการส่งซ้ำหลังหมดเวลา (Timeout + Retry): ลูกค้าโดนตัดเงินสองรอบ! — ตอนที่ 2 หน้า 1",
            "badge": "💸 คำสั่งแบบไม่ปลอดภัย: 'ให้บวก/ตัดเงินเพิ่ม 10 บาท' (Non-Idempotent)",
            "scenarios": [
                "รอบที่ 1: ส่งคำสั่ง 'ให้บวกยอดเงินเพิ่ม 10 บาท' (จาก 100 ──► 110 บาท) ──► อีกฝั่งทำสำเร็จแล้ว แต่คำตอบขากลับหายบนเครือข่าย!",
                "หมดเวลารอ (Timeout): ฝั่งผู้ส่งไม่เห็นคำตอบ จึงคิดว่าส่งไม่ถึง เลยหวังดีส่งคำสั่ง 'ให้บวกยอดเงินเพิ่ม 10 บาท' ซ้ำไปอีกรอบ (Retry)!",
                "รอบที่ 2: อีกฝั่งได้รับคำสั่งซ้ำ จึงบวกเพิ่มอีก 10 บาท (จาก 110 ──► 120 บาท)!",
            ],
            "outcome": "💥 พัง! ลูกค้าโดนคิดเงิน/ตัดเงินเบิ้ลสองรอบ (กลายเป็น 120 บาท แทนที่จะเป็น 110 บาท) เพียงเพราะเครือข่ายกระตุกตอนส่งคำตอบกลับ!",
            "takeaway": "❌ ห้ามออกแบบคำสั่งผ่านเครือข่ายแบบ 'บวกเพิ่ม/ลบออกลอยๆ' โดยไม่มีรหัสคำสั่งหรือค่าเป้าหมายที่ชัดเจน",
        }

    # RETRY_IDEMPOTENT
    return {
        "title": "3. ทางรอด: ออกแบบคำสั่งให้ทำซ้ำได้ปลอดภัย (Idempotence) — ตอนที่ 2 หน้า 1",
        "badge": "🛡️ ส่งซ้ำกี่รอบผลลัพธ์ก็เหมือนเดิม 100%!",
        "scenarios": [
            "เปลี่ยนวิธีสั่งงานตามหนังสือหน้า 1: แทนที่จะบอกว่า 'ให้บวกเงินเพิ่ม 10 บาท' ให้เปลี่ยนเป็น 'ให้ตั้งยอดเงินเป็น 110 บาท' (หรือแนบรหัสธุรกรรมเฉพาะตัว Transaction ID)",
            "รอบที่ 1: ส่งคำสั่ง 'ให้ตั้งยอดเงินเป็น 110 บาท' ──► อีกฝั่งตั้งยอดเป็น 110 บาทสำเร็จ แต่คำตอบขากลับหายบนเครือข่าย",
            "รอบที่ 2 (Retry หลัง Timeout): ส่งคำสั่งเดิมซ้ำ 'ให้ตั้งยอดเงินเป็น 110 บาท' ──► อีกฝั่งก็ตั้งเป็น 110 บาทเหมือนเดิม ไม่มีการบวกเบิ้ล!",
        ],
        "outcome": "🏆 ปลอดภัยไร้ที่ติ! ไม่ว่าเครือข่ายจะหลุดจนต้องส่งซ้ำ (Retry) กี่สิบรอบ ยอดเงินของลูกค้าก็ยังคงเป็น 110 บาทเป๊ะตามความจริง!",
        "takeaway": "✅ เทคนิคนี้เรียกว่า Idempotence (ทำซ้ำแล้วผลลัพธ์เหมือนทำครั้งเดียว) เป็นหัวใจสำคัญของระบบกระจายทุกตัว",
    }


@app.post("/api/ch8/clock/{mode}")
def simulate_ch8_clock(mode: str):
    if mode == "LWW_CLOCK_SKEW":
        return {
            "title": "อันตรายของการใช้นาฬิกาเครื่องตัดสินผู้ชนะ (Last Write Wins — LWW) เมื่อนาฬิกาเพี้ยน! — ตอนที่ 3 หน้า 2",
            "badge": "⏰ นาฬิกาเครื่อง B เดินเร็วกว่าความจริงไป 2 วินาที ──► ข้อมูลใหม่ถูกลบทิ้ง!",
            "left_box_title": "ลำดับเหตุการณ์ที่เกิดขึ้นจริงตามธรรมชาติ",
            "left_box_lines": [
                "1️⃣ เหตุการณ์แรก (เกิดก่อน): เขียนค่าเก่า 'v1' ลงที่ [เครื่อง B] แต่บังเอิญนาฬิกาเครื่อง B เพี้ยนเดินเร็วไป 2 วินาที มันจึงแปะป้ายเวลาเป็น 12:00:02!",
                "2️⃣ เหตุการณ์ที่สอง (เกิดทีหลัง — ค่าใหม่ล่าสุด!): เขียนค่าใหม่ 'v2' ลงที่ [เครื่อง A] ซึ่งนาฬิกาเดินตรงปกติ จึงแปะป้ายเวลาเป็น 12:00:01",
            ],
            "right_box_title": "เมื่อฐานข้อมูลใช้ Last Write Wins (LWW — ดูป้ายเวลาใครมากกว่าชนะ)",
            "right_box_lines": [
                "• ระบบนำป้ายเวลามาเทียบกันดื้อๆ: เครื่อง B (12:00:02) vs เครื่อง A (12:00:01)",
                "• ระบบสรุปผิดทันทีว่า เครื่อง B (12:00:02) เกิดทีหลังเครื่อง A (12:00:01)!",
                "• 💥 ผลลัพธ์: ข้อมูลใหม่ล่าสุด 'v2' ของเครื่อง A ถูกลบทิ้งอย่างไร้เยื่อใย เพราะแพ้ป้ายเวลาของเครื่อง B ที่นาฬิกาเดินเร็ว!",
            ],
            "verdict": "⚠️ ผู้เขียนเตือนตัวโตๆ ในหนังสือหน้า 2 ว่า การอาศัยเวลาจากนาฬิกาเครื่องปกติ (NTP / Crystal Oscillator) มาตัดสิน LWW อันตรายมาก เพราะนาฬิกาเดินเร็ว-ช้าหรือกระโดดถอยหลังได้เสมอ!",
        }

    # SPANNER_TRUETIME
    return {
        "title": "การซิงก์เวลาแบบปลอดภัยระดับโลก: Google Spanner (GPS Receiver + Atomic Clock + Wait Out) — ตอนที่ 3 หน้า 2",
        "badge": "🛰️ จำกัดขอบเขตความคลาดเคลื่อน + คำนวณรอก่อนปล่อยข้อมูล (Wait Out)",
        "left_box_title": "1. ติดตั้งฮาร์ดแวร์พิเศษในศูนย์ข้อมูล",
        "left_box_lines": [
            "• แทนที่จะพึ่งอินเทอร์เน็ต (NTP) อย่างเดียว Google Spanner ติดตั้งตัวรับสัญญาณดาวเทียม (GPS Receiver) และนาฬิกาอะตอม (Atomic Clock) ไว้ที่ศูนย์ข้อมูลโดยตรง",
            "• ทำให้ระบบรู้ 'ขอบเขตความคลาดเคลื่อน (Uncertainty Bound)' ที่แคบมากๆ เช่น รู้ว่าเวลาจริงต้องอยู่ระหว่าง [12:00:01.001 ถึง 12:00:01.007] แน่นอน",
        ],
        "right_box_title": "2. กลไกคำนวณรอให้พ้นช่วงคลาดเคลื่อน (Wait Out)",
        "right_box_lines": [
            "• ก่อนที่ธุรกรรมจะกดบันทึกเสร็จสมบูรณ์ ระบบจะบังคับให้ 'รอ (Wait out)' เป็นเวลาเท่ากับช่วงความคลาดเคลื่อนของนาฬิกา (เช่น รอไม่กี่มิลลิวินาที)",
            "• เพื่อให้มั่นใจ 100% ว่าช่วงเวลาของธุรกรรมใหม่ ไม่ทับซ้อนกับธุรกรรมเก่าของเครื่องอื่นเลยแม้แต่นิดเดียว!",
        ],
        "verdict": "🏆 ทำให้เรียงลำดับเหตุการณ์ทั่วโลกได้อย่างถูกต้องแม่นยำโดยที่ข้อมูลไม่ตีกัน แต่มีต้นทุนฮาร์ดแวร์และศูนย์ข้อมูลสูงมาก",
    }


@app.post("/api/ch8/consensus/{mode}")
def simulate_ch8_consensus(mode: str):
    if mode == "SPLIT_BRAIN_NO_QUORUM":
        return {
            "title": "เมื่อเครือข่ายขาดกลางคัน (2 เครื่อง vs 3 เครื่อง) โดยไม่มีกฎเสียงข้างมาก — ตอนที่ 4 หน้า 3",
            "badge": "💥 ภาวะสมองแยก (Split Brain): มีผู้ใหญ่บ้าน 2 คนพร้อมกัน ข้อมูลพังยับ!",
            "left_side": {
                "title": "ฝั่งซ้าย (มี 2 เครื่อง — ถูกตัดขาดจากฝั่งขวา)",
                "state": "❌ คิดว่าฝั่งขวาตายหมดแล้ว เลยตั้งตัวเองเป็น 'ผู้ใหญ่บ้านคนที่ 1' และรับเขียนข้อมูลจากผู้ใช้ฝั่งซ้าย!",
            },
            "right_side": {
                "title": "ฝั่งขวา (มี 3 เครื่อง — ถูกตัดขาดจากฝั่งซ้าย)",
                "state": "❌ ตั้งตัวเองเป็น 'ผู้ใหญ่บ้านคนที่ 2' และรับเขียนข้อมูลจากผู้ใช้ฝั่งขวาเหมือนกัน!",
            },
            "rules": [
                "🚨 ผลลัพธ์: ต่างฝ่ายต่างคิดว่าตัวเองคือผู้ใหญ่บ้านตัวจริง ข้อมูลสองฝั่งแยกทางกันและขัดแย้งกันจนกู้คืนไม่ได้!",
            ],
        }

    # RAFT_PAXOS_MAJORITY
    return {
        "title": "กระบวนการเห็นพ้องต้องกัน (Consensus — Paxos, Raft, Zab) ด้วยเสียงข้างมากที่แท้จริง (Strict Majority) — ตอนที่ 4–5 หน้า 3",
        "badge": "🗳️ ต้องได้โหวตมากกว่าครึ่งหนึ่งของระบบทั้งหมดเสมอ (อย่างน้อย 3 ใน 5 เครื่อง)!",
        "left_side": {
            "title": "🛑 ฝั่งซ้าย (มีแค่ 2 ใน 5 เครื่อง = ไม่ถึงครึ่ง!)",
            "state": "✅ รู้ตัวว่ามีเสียงแค่ 2 จาก 5 เครื่อง (ไม่ถึง Strict Majority) จึงปฏิเสธการตัดสินใจและหยุดรับการเขียนทันที เพื่อความปลอดภัย!",
        },
        "right_side": {
            "title": "👑 ฝั่งขวา (มี 3 ใน 5 เครื่อง = เกินครึ่งหนึ่งแน่นอน!)",
            "state": "🏆 โหวตผ่านด้วยเสียงข้างมาก (3 ใน 5) จึงมีสิทธิ์ตั้งผู้นำเพียงคนเดียวของระบบ และบันทึกข้อมูลเดินหน้าต่อไปได้อย่างปลอดภัย!",
        },
        "rules": [
            "1. รับประกันว่าถ้าเครื่องส่วนใหญ่ตกลงเลือกค่าหนึ่งแล้ว ทุกเครื่องจะต้องจำค่านั้นเหมือนกันหมด",
            "2. จะไม่มีการตัดสินใจสองแบบที่ขัดแย้งกันเองในระบบเด็ดขาด (ป้องกัน Split Brain 100%)",
            "3. รับประกันความก้าวหน้า: ตราบใดที่เครื่องส่วนใหญ่ (เช่น 3 ใน 5) ยังไม่ตาย ระบบจะหาข้อสรุปและเดินหน้าต่อไปได้เสมอ (อัลกอริทึมดัง: Paxos, Raft, Zab)",
        ],
    }


# ============================================================================
# CHAPTERS 1, 6, 9, 10, 11, 12 INTERACTIVE API (COMPLETE 12-CHAPTER SUITE)
# ============================================================================


@app.post("/api/ch1/{topic}")
def simulate_ch1(topic: str):
    if topic == "TWITTER_TIMELINE":
        return {
            "title": "ตัวอย่างคลาสสิกของ Twitter: โพสต์ข้อความ (4,600 ครั้ง/วินาที) vs เปิดดูไทม์ไลน์ (300,000 ครั้ง/วินาที) — ตอนที่ 3 หน้า 3",
            "badge": "🐦 ตัวเลขที่สำคัญที่สุดไม่ใช่จำนวนโพสต์ แต่คือ 'จำนวนผู้ติดตามของแต่ละคน'!",
            "cards": [
                {
                    "name": "วิธีที่ 1: ดึงตอนอ่าน (Pull on Read)",
                    "tag": "เขียนง่าย แต่อ่านหนักมาก!",
                    "detail": "ตอนผู้ใช้เปิดไทม์ไลน์ ระบบค่อยไปดึงโพสต์จากทุกคนที่เขาติดตามมารวมกัน ──► เมื่อคนเปิดอ่านถึง 300,000 ครั้ง/วินาที ฐานข้อมูลจึงรับไม่ไหว!",
                },
                {
                    "name": "วิธีที่ 2: ส่งล่วงหน้าตอนโพสต์ (Fan-out on Write)",
                    "tag": "อ่านเร็วมาก แต่เจอคนดังแล้วพัง!",
                    "detail": "ทันทีที่ใครโพสต์ ระบบส่งโพสต์นั้นไปใส่ตู้ไปรษณีย์ (ไทม์ไลน์ใน RAM) ของผู้ติดตามทุกคนไว้ล่วงหน้า ──► อ่านเร็วมาก แต่ถ้าคนดังที่มีผู้ติดตาม 30 ล้านคนโพสต์ โพสต์เดียวต้องเขียนถึง 30 ล้านครั้ง!",
                },
                {
                    "name": "🏆 ทางออกสุดท้ายของ Twitter: วิธีลูกผสม (Hybrid)",
                    "tag": "แยกตามจำนวนผู้ติดตาม",
                    "detail": "คนทั่วไปใช้วิธีที่ 2 (ส่งใส่ไทม์ไลน์ผู้ติดตามล่วงหน้า) ส่วน 'คนดังที่มีผู้ติดตามหลายสิบล้านคน' ให้ใช้วิธีที่ 1 (ค่อยดึงมารวมตอนผู้ใช้เปิดอ่าน)!",
                },
            ],
        }

    if topic == "PERCENTILES_AMAZON":
        return {
            "title": "การวัดประสิทธิภาพด้วยเปอร์เซ็นไทล์ (p50, p95, p99, p99.9) & กรณีศึกษาของ Amazon — ตอนที่ 3 หน้า 3",
            "badge": "📈 ห้ามดูแค่ค่าเฉลี่ย (Average)! ต้องดูความหน่วงช่วงหาง (Tail Latency)",
            "cards": [
                {
                    "name": "ค่ามัธยฐาน (p50 / Median) vs ค่าเฉลี่ย",
                    "tag": "ตัวแทนของคนส่วนใหญ่",
                    "detail": "ถ้า p50 = 20 ms แปลว่าผู้ใช้ครึ่งหนึ่งได้คำตอบเร็วกว่า 20 ms และอีกครึ่งช้ากว่านี้ ไม่ถูกค่าสุดโต่งดึงให้เพี้ยนเหมือนค่าเฉลี่ย",
                },
                {
                    "name": "ความหน่วงช่วงหาง (p95, p99, p99.9 — Tail Latency)",
                    "tag": "ทำไม Amazon ถึงแคร์ p99.9?",
                    "detail": "ในหนังสือหน้า 3: Amazon พบว่าลูกค้าที่ได้คำตอบช้าที่สุด (กลุ่ม p99.9) มักเป็นลูกค้าที่มีประวัติการสั่งซื้อเยอะที่สุด ซึ่งก็คือลูกค้า VIP ที่มีค่ามากที่สุดของบริษัท!",
                },
                {
                    "name": "Tail Latency Amplification (ยิ่งเรียกหลายบริการยิ่งช้า)",
                    "tag": "ผลกระทบลูกโซ่",
                    "detail": "ถ้าการเปิดหน้าเว็บ 1 หน้าต้องเรียกบริการเบื้องหลัง 10 ตัว ขอแค่ตัวเดียวช้า ทั้งหน้าเว็บก็ช้าตามทันที ยิ่งเรียกหลายตัว โอกาสติดตัวช้าก็ยิ่งพุ่งสูงขึ้น!",
                },
            ],
        }

    # RELIABILITY_MAINTAINABILITY
    return {
        "title": "ความน่าเชื่อถือ (Reliability — Fault vs Failure) & ความง่ายในการดูแลรักษา (Maintainability) — ตอนที่ 2 & 4",
        "badge": "🐵 Netflix Chaos Monkey & 3 หลักการออกแบบระบบไม่ให้กลายเป็นก้อนโคลน",
        "cards": [
            {
                "name": "1. ความผิดพลาด (Fault) vs ระบบล่ม (Failure)",
                "tag": "สาเหตุอันดับ 1 คือมนุษย์ตั้งค่าผิด!",
                "detail": "ฮาร์ดแวร์มีโอกาสเสีย (ดิสก์ 10,000 ลูก เสียเฉลี่ยวันละลูก), ซอฟต์แวร์มีบั๊ก, และ 'มนุษย์ตั้งค่าผิด' คือสาเหตุอันดับต้นๆ! Netflix จึงสร้าง Chaos Monkey สุ่มปิดเครื่องจริงเพื่อทดสอบว่าระบบทนต่อความผิดพลาดได้จริงหรือไม่",
            },
            {
                "name": "2. ความเรียบง่าย (Simplicity & Abstraction)",
                "tag": "กำจัด Accidental Complexity",
                "detail": "ป้องกันไม่ให้โปรเจกต์กลายเป็นก้อนโคลนขนาดใหญ่ ด้วยการใช้ Abstraction (เช่น ภาษา SQL ที่ซ่อนความยุ่งยากของการจัดการดิสก์และล็อกไว้ข้างใต้)",
            },
            {
                "name": "3. ปฏิบัติการง่าย (Operability) & พร้อมเปลี่ยนแปลง (Evolvability)",
                "tag": "ดูแลระยะยาว",
                "detail": "ค่าใช้จ่ายส่วนใหญ่ของซอฟต์แวร์อยู่ที่การดูแลต่อเนื่อง ระบบจึงต้องเฝ้าระวังง่าย ย้อนกลับ (Rollback) ได้เร็ว และปรับเปลี่ยนตามธุรกิจใหม่ได้เสมอ",
            },
        ],
    }


@app.post("/api/ch6/{topic}")
def simulate_ch6(topic: str):
    if topic == "KEY_RANGE_VS_HASH":
        return {
            "title": "วิธีแบ่งข้อมูล (Partitioning): แบ่งตามช่วงคีย์ vs แบ่งตามค่าแฮช vs ทางสายกลางของ Cassandra — ตอนที่ 3–5 หน้า 1–3",
            "badge": "⚖️ เป้าหมายคือกระจายงานให้เท่ากัน และหลีกเลี่ยงจุดร้อน (Hot Spot)",
            "cards": [
                {
                    "name": "1. แบ่งตามช่วงของคีย์ (Key-Range — เหมือนสารานุกรม A–Z)",
                    "tag": "ค้นเป็นช่วงง่าย แต่ระวังคีย์เวลา!",
                    "detail": "ข้อดีคือคีย์เรียงกันอยู่แล้ว ค้นหาเป็นช่วงง่ายมาก แต่ถ้าใช้ 'เวลา/วันที่' เป็นคีย์ ข้อมูลของวันนี้ทั้งหมดจะพุ่งไปกองที่เครื่องเดียวจนกลายเป็นจุดร้อน (Hot Spot)!",
                },
                {
                    "name": "2. แบ่งตามค่าแฮช (Hash Partitioning — เช่น MD5)",
                    "tag": "กระจายเรียบกริบ แต่เสียการค้นเป็นช่วง",
                    "detail": "นำคีย์เข้าฟังก์ชันแฮชให้ได้ตัวเลขสุ่มก่อนแบ่งช่วง (ห้ามใช้ hashCode ของ Java เพราะเปลี่ยนตามโปรเซส!) ข้อมูลกระจายเท่ากันดี แต่คีย์ที่เคยติดกันจะกระจัดกระจายไปคนละเครื่อง",
                },
                {
                    "name": "3. ทางสายกลางของ Cassandra & แก้คีย์คนดังร้อนจัด (Salting)",
                    "tag": "Compound Key + เลขสุ่ม 2 หลัก",
                    "detail": "Cassandra ใช้คีย์ 2 ส่วน: ส่วนแรก (user_id) ใช้แฮชเลือกเครื่อง ส่วนที่สอง (เวลาโพสต์) ใช้เรียงลำดับในเครื่องนั้น! และถ้าเจอคีย์คนดังที่คนแห่คอมเมนต์ ให้เติมเลขสุ่ม 2 หลัก (00..99) ต่อท้ายคีย์เพื่อกระจายไป 100 ส่วน",
                },
            ],
        }

    if topic == "SECONDARY_INDEX_CARS":
        return {
            "title": "การแบ่งดัชนีรอง (Secondary Index): ตัวอย่างเว็บขายรถมือสอง ค้นหา 'รถสีแดง' — ตอนที่ 6 หน้า 3",
            "badge": "🚗 Local Index (Scatter/Gather) ปะทะ Global Index (Term-Partitioned)",
            "cards": [
                {
                    "name": "แบบที่ 1: ดัชนีประจำแต่ละส่วน (Local / Document-Partitioned Index)",
                    "tag": "✍️ เขียนง่าย (แก้เครื่องเดียว) แต่อ่านช้า!",
                    "detail": "แต่ละเครื่องเก็บดัชนีเฉพาะรถที่อยู่ในเครื่องตัวเอง ตอนเพิ่มรถคันใหม่แก้แค่เครื่องเดียวจบ แต่พอค้นหา 'รถสีแดงทั้งหมด' ต้องส่งคำถามไปถามทุกเครื่องพร้อมกัน (Scatter/Gather) ขอแค่เครื่องเดียวช้า คำตอบทั้งหมดก็ช้าตาม!",
                },
                {
                    "name": "แบบที่ 2: ดัชนีรวมที่แบ่งตามคำค้น (Global / Term-Partitioned Index)",
                    "tag": "🚀 อ่านเร็ว (ถามเครื่องเดียว) แต่เขียนหลายที่!",
                    "detail": "รวมดัชนีสีรถทั้งระบบมาแบ่งตามตัวอักษร เช่น สี A–R อยู่เครื่องแรก สี S–Z อยู่เครื่องสอง ตอนค้นหา 'รถสีแดง' ไปถามเครื่องเดียวรู้เรื่องทันที แต่ตอนเพิ่มรถใหม่ 1 คันที่มีทั้งสี ยี่ห้อ และรุ่น ต้องตามไปอัปเดตดัชนีหลายเครื่อง (มักทำแบบไม่รอ Asynchronous)",
                },
            ],
        }

    # REBALANCING_ROUTING
    return {
        "title": "การย้ายข้อมูลเมื่อเพิ่ม/ลดเครื่อง (Rebalancing) & การส่งคำขอไปให้ถูกเครื่อง (ZooKeeper vs Gossip) — ตอนที่ 7–8 หน้า 4–5",
        "badge": "🚫 กฎเหล็ก: ห้ามเอาค่าแฮชไปหารด้วยจำนวนเครื่อง (hash % N) เด็ดขาด!",
        "cards": [
            {
                "name": "❌ ทำไมห้ามใช้ hash % จำนวนเครื่อง (mod N)?",
                "tag": "ข้อมูลเกือบทั้งระบบต้องย้ายที่!",
                "detail": "ถ้ามี 10 เครื่อง ใช้ hash % 10 พอเพิ่มเป็น 11 เครื่อง (hash % 11) เศษของคีย์เกือบทุกตัวจะเปลี่ยนทันที ทำให้ข้อมูลเกือบทั้งหมดต้องย้ายเครื่องจนเครือข่ายพัง!",
            },
            {
                "name": "✅ 3 วิธี Rebalancing ที่ถูกต้องในระบบจริง",
                "tag": "ย้ายเฉพาะส่วนที่จำเป็น",
                "detail": "1. สร้างจำนวน Partition คงที่ไว้เยอะๆ ตั้งแต่ต้น (เช่น 10 เครื่อง แต่สร้าง 1,000 partitions — Elasticsearch/Riak) | 2. แบ่งยืดหยุ่นตามขนาดข้อมูล (เช่น HBase แบ่งครึ่งเมื่อครบ 10 GB) | 3. จำนวน Partition ตามจำนวนเครื่อง (Cassandra เครื่องละ 256 vnodes)",
            },
            {
                "name": "🧭 ส่งคำขอไปให้ถูกเครื่องได้อย่างไร (Service Discovery)?",
                "tag": "ZooKeeper vs Gossip Protocol",
                "detail": "HBase, Kafka และ SolrCloud ใช้บริการกลางอย่าง ZooKeeper คอยจดและแจ้งเตือนว่า Partition ไหนย้ายไปอยู่เครื่องไหน ส่วน Cassandra และ Riak ใช้ Gossip Protocol ให้เครื่องต่างๆ ซุบซิบบอกต่อข่าวกันเอง",
            },
        ],
    }


@app.post("/api/ch9/{topic}")
def simulate_ch9(topic: str):
    if topic == "QUEUE_VS_LOG_CDC":
        return {
            "title": "การส่งต่อสตรีม (Messaging Queue vs Event Log) & ความเชื่อมโยงกับฐานข้อมูล (CDC & Event Sourcing) — ตอนที่ 2–3 หน้า 1–2",
            "badge": "📜 ฐานข้อมูลคือสตรีมที่ถูกเก็บนิ่งๆ และสตรีมคือฐานข้อมูลที่กำลังเดินทางผ่านกาลเวลา!",
            "cards": [
                {
                    "name": "1. คิวข้อความ (RabbitMQ) vs บันทึกเหตุการณ์ (Kafka Event Log)",
                    "tag": "ลบเมื่อทำเสร็จ vs เก็บต่อท้ายอ่านซ้ำได้",
                    "detail": "Messaging Queue ส่งงานให้ผู้บริโภคทำทีละชิ้น พอทำเสร็จข้อความจะถูกลบทิ้ง ส่วน Event Log (Kafka) เขียนต่อท้ายไฟล์เรียงยาวเหมือนประวัติศาสตร์ ผู้บริโภคแต่ละรายจำตำแหน่ง (Offset) ของตัวเองและย้อนกลับมาอ่านซ้ำได้เสมอ!",
                },
                {
                    "name": "2. Change Data Capture (CDC) vs Event Sourcing",
                    "tag": "'ฝาก 1,000' + 'ฝาก 1,000' = 2,000",
                    "detail": "CDC ดักการเพิ่ม/ลบ/แก้ในฐานข้อมูลหลักส่งออกเป็นสตรีมให้อัปเดตคลังข้อมูลและระบบค้นหาแบบเรียลไทม์ ส่วน Event Sourcing เก็บทุกเหตุการณ์ดิบๆ (เช่น 'ฝาก 1,000' แล้ว 'ฝากอีก 1,000') แทนที่จะเก็บแค่สถานะยอดเงินปัจจุบัน ทำให้แกะรอยย้อนหลังได้ทุกวินาที",
                },
            ],
        }

    if topic == "EVENT_TIME_WATERMARK":
        return {
            "title": "ปัญหาเรื่อง 'เวลา' ในสตรีม: ซื้อของตอน 12:59 น. แต่เน็ตหลุดเพิ่งมาถึงระบบตอน 13:15 น.! — ตอนที่ 5 หน้า 2–3",
            "badge": "⏱️ Event Time vs Processing Time และเส้นแบ่งเวลาสมมติ (Watermarks)",
            "cards": [
                {
                    "name": "❌ ถ้าใช้เวลาเครื่องประมวลผล (Processing Time = 13:15 น.)",
                    "tag": "ยอดขายผิดชั่วโมง!",
                    "detail": "ลูกค้ากดซื้อสินค้าตอน 12:59 น. (Event Time) แต่สัญญาณอินเทอร์เน็ตหลุด ข้อมูลเพิ่งเดินทางมาถึงเซิร์ฟเวอร์ตอน 13:15 น. (Processing Time) ถ้านับตามนาฬิกาเซิร์ฟเวอร์ ยอดขายนี้จะถูกปัดไปเป็นของบ่ายโมง ซึ่งผิดความจริง!",
                },
                {
                    "name": "✅ ใช้เวลาที่เกิดเหตุการณ์จริง (Event Time) + Watermarks",
                    "tag": "จัดการข้อมูลหลงยุค (Stragglers)",
                    "detail": "ระบบสตรีมยุคใหม่ (เช่น Apache Flink) ใช้เส้นแบ่งเวลาสมมติที่เรียกว่า Watermarks เพื่อบอกระบบว่า 'มั่นใจว่าข้อมูลจนถึงเวลานี้เดินทางมาครบแล้ว เริ่มคำนวณผลลัพธ์รอบนี้ได้' และหากมีข้อมูลตกค้างมาช้ากว่านั้น (Stragglers) ก็สั่งอัปเดตแก้ไขยอดย้อนหลังได้",
                },
            ],
        }

    # STREAM_JOINS
    return {
        "title": "การเชื่อมข้อมูลสตรีม 3 รูปแบบ (Stream Joins) — ตอนที่ 6 หน้า 3",
        "badge": "👟 ค้นหาคำว่า 'รองเท้า' แล้วคลิกซื้อภายใน 1 นาทีต่อมา!",
        "cards": [
            {
                "name": "1. Stream-Table Join (สตรีมเชื่อมกับตารางนิ่ง)",
                "tag": "เติมข้อมูลรายละเอียด",
                "detail": "มีสตรีมการคลิกสินค้าวิ่งเข้ามา อยากรู้ว่าสินค้านั้นราคาเท่าไร ก็วิ่งไปดึงข้อมูลราคาจากฐานข้อมูล (หรือตารางที่ซิงก์เก็บไว้ในเครื่อง) มาแปะรวมกัน",
            },
            {
                "name": "2. Stream-Stream Join (Window Join — เชื่อมสองสตรีมในช่วงเวลา)",
                "tag": "จำสถานะชั่วคราวในหน้าต่างเวลา",
                "detail": "ตัวอย่างในหนังสือหน้า 3: ลูกค้าค้นหาคำว่า 'รองเท้า' บนเว็บ (สตรีมที่ 1) แล้วคลิกซื้อสินค้าภายใน 1 นาทีต่อมา (สตรีมที่ 2) ระบบต้องจำเหตุการณ์แรกไว้ชั่วคราวเพื่อรอจับคู่กับเหตุการณ์ที่สอง",
            },
            {
                "name": "3. Stream-Stream Join (Stateful Join — เชื่อมแบบไม่จำกัดเวลา)",
                "tag": "เก็บสถานะสะสมข้ามวัน",
                "detail": "เช่น การจับคู่การแสดงโฆษณากับยอดคลิกซื้อสินค้าที่อาจเกิดขึ้นตามมาในอีกหลายวันให้หลัง ระบบต้องเก็บสถานะสะสมไว้เรื่อยๆ",
            },
        ],
    }


@app.post("/api/ch10/{topic}")
def simulate_ch10(topic: str):
    if topic == "UNIX_TO_MAPREDUCE":
        return {
            "title": "จากปรัชญาเครื่องมือ Unix (Pipes) สู่การประมวลผลระดับเพตาไบต์ด้วย MapReduce & HDFS — ตอนที่ 2–3 หน้า 1–2",
            "badge": "🔧 cat | grep | sort | uniq ──► ขยายออกเป็น Mapper + Shuffle + Reducer บนเครื่องนับพัน!",
            "cards": [
                {
                    "name": "1. ปรัชญาจากเครื่องมือของ Unix (Unix Command-Line Tools)",
                    "tag": "เรียบง่ายแต่ทรงพลัง",
                    "detail": "• ทำหน้าที่อย่างเดียวแต่ทำให้ดีเลิศ\n• เชื่อมต่อกันด้วยท่อ (Pipe |) ส่งผลลัพธ์จากคำสั่งแรกเป็นอินพุตให้คำสั่งถัดไป\n• ข้อมูลต้นทางห้ามแก้ (Immutable Inputs) อ่านแล้วแปรรูปเป็นชุดใหม่เสมอ",
                },
                {
                    "name": "2. MapReduce และระบบแฟ้มข้อมูลกระจาย (HDFS)",
                    "tag": "จำลอง Unix Pipes บนเครื่องนับพัน",
                    "detail": "• Mapper: วิ่งอ่านข้อมูลดิบทีละระเบียน แล้วคัดแยกออกมาเป็นคู่คีย์-ค่า (Key-Value)\n• Shuffle (เบื้องหลัง): จัดกลุ่มและเรียงลำดับข้อมูลที่มี 'คีย์เดียวกัน' มาไว้ด้วยกัน\n• Reducer: รวบรวมข้อมูลคีย์เดียวกันมาคำนวณสรุปผล (เช่น รวมยอด หรือนับจำนวน) โดยเก็บไฟล์บน HDFS",
                },
            ],
        }

    # MAPREDUCE_VS_SPARK
    return {
        "title": "จุดอ่อนของ MapReduce และการปฏิวัติสู่ยุค Dataflow Engines (Apache Spark / Tez / Flink) — ตอนที่ 4–5 หน้า 2–3",
        "badge": "🚀 เลิกเขียนพักลงดิสก์ทุกขั้นตอน! ประมวลผลใน RAM ด้วยกราฟการไหลของข้อมูล (DAG)",
        "cards": [
            {
                "name": "🐢 จุดอ่อนของ MapReduce ดั้งเดิม",
                "tag": "ช้าเพราะเขียนลงดิสก์ทุกก้าว",
                "detail": "แม้จะทนทานต่อความพังมาก แต่ทุกขั้นตอนของการต่อ Job (เช่น Job A ส่งผลให้ Job B) ต้องบังคับเขียนไฟล์พักลงดิสก์ (Materialize Intermediate State) แล้วรออ่านขึ้นมาใหม่ ทำให้เสียเวลา I/O มหาศาล และต้องเขียนโค้ดยาวเหยียด",
            },
            {
                "name": "⚡ ยุคใหม่: Dataflow Engines (Apache Spark, Tez, Flink)",
                "tag": "In-Memory + DAG + Spark SQL",
                "detail": "• ประมวลผลในหน่วยความจำ (In-memory) ส่งต่อข้อมูลผ่าน RAM หรือเครือข่ายโดยตรง เร็วกว่าเดิมหลายเท่า\n• มองภาพรวมเป็นกราฟ (DAG - Directed Acyclic Graph) ปรับแต่งเส้นทางคำนวณอัตโนมัติ ไม่บังคับตายตัวว่าต้อง Map-แล้ว-Reduce เท่านั้น\n• มีภาษาที่เขียนง่ายขึ้นมาก เช่น Spark SQL",
            },
        ],
    }


@app.post("/api/ch11/{topic}")
def simulate_ch11(topic: str):
    if topic == "CHECKPOINTS_EXACTLY_ONCE":
        return {
            "title": "สตรีมมิ่งแบบปลายทางถึงปลายทาง (End-to-End): ทำอย่างไรให้ข้อมูลไม่หายและไม่ตัดเงินเบิ้ลแม้เครื่องแฮงก์! — ตอนที่ 2–4 หน้า 1–2",
            "badge": "🎯 End-to-End Exactly-Once Semantics (Checkpoints + Idempotence + Atomic 2PC)",
            "cards": [
                {
                    "name": "1. การรับมือเมื่อเครื่องประมวลผลแฮงก์กลางคัน (Checkpoints & Barriers)",
                    "tag": "ไม่ต้องกลับไปนับหนึ่งใหม่",
                    "detail": "ในสตรีมที่ข้อมูลไหลมาไม่มีวันจบ Apache Flink จะส่งสัญญาณคั่น (Barriers) แทรกเข้าไปในสตรีมเป็นระยะเพื่อบันทึกภาพสถานะ (Checkpoint) ลงดิสก์ ถ้าเครื่องพังกลางคัน ก็ดึงสถานะล่าสุดจาก Checkpoint มารันต่อจากตำแหน่งสตรีมนั้นทันที!",
                },
                {
                    "name": "2. การบันทึกผลลัพธ์ลงปลายทางไม่ให้เบิ้ล (Idempotence & Atomic Transactions)",
                    "tag": "ทำไมแอปค้างแล้วระบบไม่ตัดเงินซ้ำ",
                    "detail": "สมมติคำนวณยอดเสร็จแล้วกำลังส่งไปบันทึกฐานข้อมูลปลายทางแต่เครื่องพังพอดี พอส่งซ้ำรอบสอง:\n• วิธีที่ 1 (Idempotent): ใช้รหัสข้อความ (Message ID) กำกับ หรือสั่ง 'ตั้งยอดเป็นค่านี้' ถ้าส่งซ้ำฐานข้อมูลเห็น ID เดิมก็จะเมินเฉย ไม่บวกซ้ำ!\n• วิธีที่ 2 (Atomic Transaction / 2PC): ผูกการบันทึกผลลัพธ์ปลายทางเข้ากับการขยับตำแหน่งสตรีม (Offset) ให้สำเร็จพร้อมกันหรือพังพร้อมกันทั้งคู่",
                },
            ],
        }

    # LAMBDA_VS_UNIFIED
    return {
        "title": "จากสถาปัตยกรรมแยกสองทาง (Lambda Architecture) สู่การรวมโลกแบตช์และสตรีมเข้าด้วยกัน — ตอนที่ 5 หน้า 2",
        "badge": "🔄 ยุติความเหนื่อยยากในการเขียนและดูแลโค้ด 2 ชุดคู่ขนานกัน!",
        "cards": [
            {
                "name": "🏛️ อดีต: Lambda Architecture (แยกเดิน 2 เส้นทางพร้อมกัน)",
                "tag": "แม่นยำแต่ต้องดูแลโค้ด 2 ชุด!",
                "detail": "• Batch Layer: เอาข้อมูลทั้งหมดมาคำนวณย้อนหลังแบบแบตช์ (แม่นยำ แต่ช้า ได้ผลลัพธ์เป็นรอบๆ)\n• Speed Layer: เอาสตรีมมาคำนวณสดแบบเรียลไทม์ (เร็ว แต่มีโอกาสคลาดเคลื่อน)\n• Serving Layer: นำผลลัพธ์สองฝั่งมาผสมกันตอนแสดงผล (ข้อเสีย: วิศวกรต้องเขียนและดูแลโค้ด 2 ชุดตลอดเวลา เหนื่อยและซับซ้อนมาก!)",
            },
            {
                "name": "🏆 ปัจจุบัน: Unified Stream Processing (ทางเดินเดียวจบครบในตัว)",
                "tag": "Exactly-Once ทำให้รวมกันได้!",
                "detail": "เมื่อเครื่องมือสตรีมมิ่งยุคใหม่ (เช่น Apache Flink) มีกลไก Checkpoints และ Exactly-once ที่แม่นยำเทียบเท่าแบตช์แล้ว จึงสามารถรวบเหลือทางเดินเดียวที่ทั้งเร็วแบบเรียลไทม์และถูกต้อง 100% โดยไม่ต้องแยกสองระบบอีกต่อไป",
            },
        ],
    }


@app.post("/api/ch12/{topic}")
def simulate_ch12(topic: str):
    if topic == "UNBUNDLING_INTEGRATION":
        return {
            "title": "อวสานฐานข้อมูลครอบจักรวาล: การบูรณาการข้อมูล (Data Integration) & การแยกชิ้นส่วนฐานข้อมูลมาต่อเหมือนเลโก้ — ตอนที่ 2–3 หน้า 1–2",
            "badge": "🧱 Unbundling Databases: PostgreSQL (ข้อมูลหลัก) ──► Kafka CDC ──► Elasticsearch + Redis + Parquet",
            "cards": [
                {
                    "name": "1. การบูรณาการระบบข้อมูลด้วย Derived Data (ข้อมูลต่อยอด)",
                    "tag": "ไม่มีฐานข้อมูลเดี่ยวตัวไหนเก่งทุกเรื่อง!",
                    "detail": "ในระบบจริงเราต้องใช้หลายเครื่องมือประกอบกัน: ใช้ PostgreSQL เก็บข้อมูลหลัก (System of Record) แล้วส่งการเปลี่ยนแปลงผ่านสตรีม/CDC (Kafka) ไปอัปเดตระบบย่อยอื่นๆ อัตโนมัติ เช่น Elasticsearch (ค้นหาข้อความ), Redis (แคช), และคลังข้อมูล (Parquet) โดยไม่ให้ข้อมูลขัดแย้งกัน",
                },
                {
                    "name": "2. การแยกส่วนประกอบของฐานข้อมูล (Unbundling Databases)",
                    "tag": "ประกอบร่างเหมือนตัวต่อเลโก้",
                    "detail": "วิสัยทัศน์ของ Martin Kleppmann คือการแยกส่วนการจัดเก็บ ออกจากส่วนดัชนี และส่วนประมวลผล แล้วให้นักพัฒนานำชิ้นส่วนที่ดีที่สุดมาประกอบร่างกันเองผ่านมาตรฐานกลางอย่าง Kafka และไฟล์ Parquet ทำให้ระบบโปร่งใสและปรับแต่งได้ทุกชิ้นส่วน",
                },
            ],
        }

    # UNIFIED_AND_ETHICS
    return {
        "title": "การประมวลผลไร้รอยต่อ (Unified Processing) & ความน่าเชื่อถือ ความเป็นส่วนตัว และจริยธรรมข้อมูล — ตอนที่ 4–6 หน้า 2–3",
        "badge": "⚖️ ข้อมูลไม่ใช่แค่เรื่องเทคนิค แต่มันคือชีวิตของคนจริงๆ (Privacy, Right to be Forgotten & Ethics)",
        "cards": [
            {
                "name": "1. Unified Processing (เขียนโค้ดชุดเดียว รันได้ทั้ง Batch และ Stream)",
                "tag": "Apache Beam & Apache Flink",
                "detail": "มองข้อมูลแบตช์เป็นเพียง 'สตรีมที่มีจุดเริ่มต้นและจุดสิ้นสุดชัดเจน (Bounded Stream)' ส่วนสตรีมคือ 'สตรีมที่ไม่มีวันสิ้นสุด (Unbounded Stream)' ทำให้เขียนโค้ดชุดเดียวรันได้ทั้งสองโลกอย่างไร้รอยต่อ",
            },
            {
                "name": "2. ความเป็นส่วนตัว (สิทธิในการถูกลืม) และจริยธรรมของวิศวกรข้อมูล",
                "tag": "ความรับผิดชอบต่อสังคม",
                "detail": "• ความน่าเชื่อถือ: อย่าออกแบบระบบที่ผิดพลาดแล้วซ่อนปัญหาไว้ใต้พรม\n• ความเป็นส่วนตัว (Privacy & Right to be forgotten): ระบบต้องเปิดให้ลบข้อมูลของผู้ใช้ออกได้อย่างเกลี้ยงเกลาจริงๆ ไม่ใช่แค่ซ่อนไว้\n• จริยธรรม (Ethics): อัลกอริทึมมีผลต่อการอนุมัติสินเชื่อ การรับเข้าทำงาน และชีวิตผู้คน วิศวกรต้องระวังข้อมูลที่บิดเบือนหรืออคติเสมอ",
            },
        ],
    }


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
