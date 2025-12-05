import os
import sys
import ssl
import json
import asyncio
import random
import datetime
import certifi

if sys.platform.startswith("win"):
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    except Exception:
        pass

from aiomqtt import Client, MqttError

# === MQTT 設定===
HOST   = "ca6d193d786e4190b3e0399b919e4be7.s1.eu.hivemq.cloud"
PORT   = 8883
USER   = "supubandsub"
PASS   = "Su1216mq"
TOPIC  = "class/2025/lab1/stu1/data"
CID    = "sensor-stu1-001"
USERNO = "202501"

def make_tls_context() -> ssl.SSLContext:
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.load_verify_locations(cafile=certifi.where())
    ctx.check_hostname = True
    ctx.verify_mode = ssl.CERT_REQUIRED
    return ctx

# 初始模擬狀態：使用你前端顯示的身高、體重，其他初值在合理範圍內
state = {
    "height": 178.0,
    "weight": 64.0,
    "pulse": 78,        # 45~120
    "spo2": 96,         # 84~99
    "temperature": 36.5,# 35.0~39.0
    "bp_sys": 106,      # 70~139
    "bp_dia": 69        # 40~89
}

def clamp(v, lo, hi):
    return max(lo, min(hi, v))

def make_payload() -> dict:
    """根據 state 產生 payload（並更新 state），遵守你指定的變動規則。"""
    # 依規則微幅變動
    state["pulse"] = int(clamp(state["pulse"] + random.randint(-6, 6), 45, 120))
    state["spo2"]  = int(clamp(state["spo2"]  + random.randint(-2, 2), 84, 99))
    # temperature +/-0.5，保留一位小數
    t = state["temperature"] + (random.random() - 0.5)
    t = round(clamp(round(t*10)/10.0, 35.0, 39.0), 1)
    state["temperature"] = t
    state["bp_sys"] = int(clamp(state["bp_sys"] + random.randint(-10, 10), 70, 139))
    state["bp_dia"] = int(clamp(state["bp_dia"] + random.randint(-3, 3), 40, 89))

    height = state["height"]
    weight = state["weight"]
    bmi = round(weight / ((height / 100.0) ** 2), 1)

    payload = {
        "msgno": random.randint(100, 999),
        "device_id": CID,
        "userno": USERNO,
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        # 身高體重固定（若要模擬變化可改 state）
        "height": round(height, 1),
        "weight": round(weight, 1),
        "bmi": bmi,
        # 同時放兩組血壓欄位（舊名與新名都放，前端會抓 bp_sys/bp_dia）
        "blood_pressure_systolic": state["bp_sys"],
        "blood_pressure_diastolic": state["bp_dia"],
        "bp_sys": state["bp_sys"],
        "bp_dia": state["bp_dia"],
        # 脈搏/體溫/血氧
        "pulse": state["pulse"],
        "temperature": state["temperature"],
        "temp_c": state["temperature"],  # 另放一組可能被前端檢查的 key
        "spo2": state["spo2"]
    }
    return payload

async def publisher():
    tls_ctx = make_tls_context()

    try:
        async with Client(
            hostname=HOST,
            port=PORT,
            username=USER,
            password=PASS,
            identifier=CID,
            tls_context=tls_ctx,
        ) as client:
            print(f"Connected to MQTT broker {HOST}:{PORT} as {CID}")
            while True:
                payload = make_payload()
                text = json.dumps(payload, ensure_ascii=False)  # 將字典轉成 JSON 字串
                print("📤 Publish ->", text)
                # publish with QoS 1
                await client.publish(TOPIC, text, qos=1, retain=False)
                # 每 3 秒發一次（前端 / mock 皆以 3 秒更新）
                await asyncio.sleep(3)

    except MqttError as e:
        print(f"MQTT Error: {e}")
    except Exception as e:
        print(f"Error: {e}")

async def main():
    await publisher()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped by user")
