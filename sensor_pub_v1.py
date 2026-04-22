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

# === MQTT 設定 ===
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

# 初始模擬狀態：配合新版前端欄位
state = {
    "height": 178.0,
    "weight": 64.0,
    "heart_rate": 78,
    "steps": 4200,
    "active_minutes": 35,
    "sleep_hours": 7.2,
    "sleep_quality": 82,
    "sedentary_time": 25,
    "calories": 1680,
    "spo2": 97,
    "hrv": 48,
}

def clamp(v, lo, hi):
    return max(lo, min(hi, v))

def make_payload() -> dict:
    """根據 state 產生新版健康監測 payload。"""

    state["heart_rate"] = int(clamp(state["heart_rate"] + random.randint(-3, 3), 45, 120))

    # 步數：只能增加，有時不變
    if random.random() < 0.45:
        step_add = 0
    else:
        step_add = random.randint(5, 80)
    state["steps"] = int(clamp(state["steps"] + step_add, 0, 30000))

    # 運動時間：有活動時才增加
    if step_add >= 30:
        state["active_minutes"] = int(clamp(state["active_minutes"] + random.choice([0, 1]), 0, 300))

    # 睡眠資料：同一天固定，不一直變
    # sleep_hours / sleep_quality 不更新

    # 久坐：有走動就歸零，沒走動才增加
    if step_add >= 30:
        state["is_sedentary"] = False
        state["sedentary_time"] = 0
    else:
        if random.random() < 0.75:
            state["is_sedentary"] = True
            state["sedentary_time"] = int(clamp(state["sedentary_time"] + random.randint(3, 6), 0, 180))
        else:
            state["is_sedentary"] = False
            state["sedentary_time"] = 0

    # 卡路里慢慢增加
    state["calories"] = int(clamp(state["calories"] + random.randint(1, 8) + step_add // 25, 800, 4000))

    # 血氧 / HRV 小幅變動
    state["spo2"] = int(clamp(state["spo2"] + random.randint(-1, 1), 94, 99))
    state["hrv"] = int(clamp(state["hrv"] + random.randint(-2, 2), 10, 120))

    height = state["height"]
    weight = state["weight"]
    bmi = round(weight / ((height / 100.0) ** 2), 1)

    payload = {
        "msgno": random.randint(100, 999),
        "device_id": CID,
        "userno": USERNO,
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "height": round(height, 1),
        "weight": round(weight, 1),
        "bmi": bmi,
        "heart_rate": state["heart_rate"],
        "steps": state["steps"],
        "active_minutes": state["active_minutes"],
        "sleep_hours": state["sleep_hours"],
        "sleep_quality": state["sleep_quality"],
        "sedentary_time": state["sedentary_time"],
        "calories": state["calories"],
        "spo2": state["spo2"],
        "hrv": state["hrv"]
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
                text = json.dumps(payload, ensure_ascii=False)
                print("📤 Publish ->", text)
                await client.publish(TOPIC, text, qos=1, retain=False)
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