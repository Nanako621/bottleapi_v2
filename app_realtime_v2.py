# app_realtime_v2.py  （含 mock_publisher）
# 保留你的設計：1) aiomqtt 在 background asyncio loop 訂閱  2) gevent + Bottle 提供 WebSocket  3) queue 作橋接
# 新增：mock_publisher 每 3 秒推送與前端相容的指標（當 MQTT 不活躍時仍可看到即時資料）

import sys, ssl, json, asyncio, threading, queue, certifi, time, random
from bottle import Bottle, request, abort, response, static_file
from gevent import sleep as gsleep, spawn as gspawn
from gevent.pywsgi import WSGIServer
from geventwebsocket.handler import WebSocketHandler
from geventwebsocket import WebSocketError
from aiomqtt import Client

if sys.platform.startswith("win"):
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    except Exception:
        pass

# === MQTT 參數（請依你的環境調整） ===
HOST = "ca6d193d786e4190b3e0399b919e4be7.s1.eu.hivemq.cloud"
PORT = 8883
USER = "supubandsub"
PASS = "Su1216mq"
TOPIC = "class/2025/lab1/stu1/data"
CID  = "subscriber-stu1-001"

def make_tls():
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.load_verify_locations(certifi.where())
    ctx.check_hostname = True
    ctx.verify_mode = ssl.CERT_REQUIRED
    return ctx

# ---- 跨執行緒橋接（MQTT -> inbox -> broadcaster -> WebSocket clients） ----
inbox = queue.Queue(maxsize=1000)  # MQTT 丟進來（字串：json）
sockets = set()

# 用來紀錄最後一次 MQTT 訊息時間（秒），供 mock 決定是否需要更積極模擬
last_mqtt_ts = 0.0
last_mqtt_lock = threading.Lock()

def set_last_mqtt_ts():
    global last_mqtt_ts
    with last_mqtt_lock:
        last_mqtt_ts = time.time()

def get_last_mqtt_ts():
    with last_mqtt_lock:
        return last_mqtt_ts

import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def broadcaster():
    """從 inbox 取出字串，送到所有 WebSocket client；並打印 debug 日誌"""
    sent_counter = 0
    while True:
        try:
            msg = inbox.get(timeout=0.1)
        except queue.Empty:
            gsleep(0.05)
            continue

        # log message arrival
        logging.info(f"[broadcaster] pop inbox msg (len sockets={len(sockets)}) -> {msg[:120]!r}")

        dead = []
        for ws in list(sockets):
            try:
                ws.send(msg)
                sent_counter += 1
                logging.debug(f"[broadcaster] sent to ws {id(ws)} (total sent {sent_counter})")
            except Exception as e:
                logging.warning(f"[broadcaster] send failed to ws {id(ws)} -> {e}")
                dead.append(ws)
        for ws in dead:
            sockets.discard(ws)
            logging.info(f"[broadcaster] removed dead ws {id(ws)}; now sockets={len(sockets)}")


# 啟動 broadcaster（gevent）
gspawn(broadcaster)

# ---- aiomqtt + Thread 的 asyncio 事件迴圈----
async def mqtt_sub():
    """訂閱 MQTT 並把標準化 JSON 放到 inbox"""
    async with Client(
        hostname=HOST, port=PORT,
        username=USER, password=PASS,
        identifier=CID, tls_context=make_tls()
    ) as cli:
        await cli.subscribe(TOPIC, qos=1)
        async for m in cli.messages:
            # 解析 topic
            try:
                topic = m.topic if isinstance(m.topic, str) else m.topic.decode("utf-8", errors="replace")
            except Exception:
                topic = str(getattr(m, "topic", "<unknown>"))

            # 解析 payload
            try:
                raw = m.payload.decode("utf-8", errors="replace")
            except Exception:
                raw = getattr(m, "payload", b"").decode("utf-8", errors="replace") if hasattr(m, "payload") else ""

            # 嘗試把 payload 轉成 JSON 物件，失敗就保留字串
            try:
                parsed = json.loads(raw)
            except Exception:
                parsed = raw

            # 組成統一格式再放入 inbox（序列化成字串）
            out_obj = {"topic": topic, "payload": parsed}
            out_text = json.dumps(out_obj, ensure_ascii=False)
            try:
                inbox.put_nowait(out_text)
            except queue.Full:
                # queue 滿了，丟棄最舊（或採取其他策略）
                try:
                    _ = inbox.get_nowait()
                    inbox.put_nowait(out_text)
                except Exception:
                    pass

            # 更新最後一次 MQTT 時間
            set_last_mqtt_ts()

def run_mqtt():
    # 在 background thread 使用 asyncio.run 啟動 mqtt_sub
    try:
        asyncio.run(mqtt_sub())
    except Exception as e:
        print("mqtt loop 終止或發生例外:", e)

# 啟動 MQTT 執行緒（daemon）
threading.Thread(target=run_mqtt, daemon=True).start()

# ---- Mock publisher：當 MQTT 不活躍時每 3 秒放入模擬資料 ----
# 初始模擬狀態（與前端相容）
mock_state = {
    "pulse": 78,    # 45~120
    "spo2": 97,     # 84~99
    "temp": 36.5,   # 35.0~39.0
    "bp_sys": 118,  # 70~139
    "bp_dia": 76,   # 40~89
    "height": 178,
    "weight": 64
}

def clamp(v, lo, hi):
    return max(lo, min(hi, v))

def mock_publisher():
    """每 3 秒產生一筆模擬資料放入 inbox（格式跟 MQTT 相同：{topic, payload}）"""
    while True:
        # 若最近有 MQTT 訊息，也可以選擇繼續推 mock（讓前端能在無新 MQTT 時持續更新）
        # 或者改成：if time_since_last_mqtt < 2: sleep and continue  -> 我這裡選擇始終推送 mock（3s），真實 MQTT 會覆寫
        # 但為避免跟真實資料競爭太頻繁，我們可以檢查是否在非常短時間內收到 MQTT，若是，降低 mock 頻率
        last = get_last_mqtt_ts()
        now = time.time()
        # 如果最近 2 秒內有收到 MQTT，讓 mock 等待 3 秒再發（避免頻繁覆寫）
        if now - last < 2.0:
            time.sleep(3.0)
            continue

        # 按你的規則調整 random 變動
        # 脈搏 ±6，範圍 45~120
        mock_state["pulse"] = int(clamp(round(mock_state["pulse"] + random.randint(-6, 6)), 45, 120))
        # 血氧 ±2，範圍 84~99
        mock_state["spo2"] = int(clamp(round(mock_state["spo2"] + random.randint(-2, 2)), 84, 99))
        # 體溫 ±0.5，範圍 35.5~39.0（保持一位小數）
        mock_state["temp"] = round(clamp(round(mock_state["temp"]*10)/10 + (random.random() - 0.5), 35.5, 39.0), 1)
        # 收縮壓 ±10，範圍 70~139
        mock_state["bp_sys"] = int(clamp(round(mock_state["bp_sys"] + random.randint(-10, 10)), 70, 139))
        # 舒張壓 ±3，範圍 40~89
        mock_state["bp_dia"] = int(clamp(round(mock_state["bp_dia"] + random.randint(-3, 3)), 40, 89))

        payload = {
            "timestamp": int(time.time()),
            # 使用與前端一致的欄位名稱
            "pulse": mock_state["pulse"],
            "spo2": mock_state["spo2"],
            "temperature": mock_state["temp"],  # 前端會檢查 temperature 或 temp_c
            "bp_sys": mock_state["bp_sys"],
            "bp_dia": mock_state["bp_dia"],
            "height": mock_state["height"],
            "weight": mock_state["weight"]
        }
        obj = {"topic": "mock/data", "payload": payload}
        try:
            inbox.put_nowait(json.dumps(obj, ensure_ascii=False))
        except queue.Full:
            try:
                _ = inbox.get_nowait()
                inbox.put_nowait(json.dumps(obj, ensure_ascii=False))
            except Exception:
                pass

        # 等待 3 秒再產生下一筆
        time.sleep(3.0)

# 啟動 mock publisher 背景執行緒（daemon）
threading.Thread(target=mock_publisher, daemon=True).start()

# ---- Bottle + WebSocket ----
app = Bottle()

@app.get("/")
def index():
    return static_file('patient_info.html', root='.')

@app.get("/dashboard")
def dashboard():
    return static_file('realmedashboard.html', root='.')

@app.get("/input2")
def input2():
    return static_file('height_weight.html', root='.')



# 若你有 /static 目錄，下面可以提供靜態資源：
@app.route('/static/<filepath:path>')
def server_static(filepath):
    return static_file(filepath, root='./static')

# WebSocket endpoint (與前端相同路徑：/ws)
@app.route("/ws")
def ws():
    wsock = request.environ.get("wsgi.websocket")
    if not wsock:
        abort(400, "Expected WebSocket")

    sockets.add(wsock)
    logging.info(f"[ws] client connected: {id(wsock)} (total {len(sockets)})")
    try:
        while True:
            # non-blocking receive — we only keep the socket alive
            msg = wsock.receive()
            if msg is None:
                logging.info(f"[ws] client {id(wsock)} closed connection.")
                break
            # Optional: log if client sent something
            logging.debug(f"[ws] received from client {id(wsock)}: {msg}")
    except WebSocketError as e:
        logging.warning(f"[ws] WebSocketError for {id(wsock)}: {e}")
    except Exception as e:
        logging.exception(f"[ws] Exception for {id(wsock)}: {e}")
    finally:
        sockets.discard(wsock)
        logging.info(f"[ws] client removed: {id(wsock)} (total {len(sockets)})")


# 新增一個 API：當使用者按下右上按鈕會 POST 到這裡
@app.post("/api/action")
def api_action():
    try:
        payload = request.json
        if payload is None:
            payload = json.loads(request.body.read() or "{}")
    except Exception:
        payload = {}
    action = payload.get("action", "unknown")
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] Action received:", action, "payload:", payload)
    response.content_type = "application/json"
    return {"status":"ok","action":action,"received_at":ts}

if __name__ == "__main__":
    print("啟動 Web Server (0.0.0.0:8080)...")
    server = WSGIServer(("0.0.0.0", 8080), app, handler_class=WebSocketHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("Shutting down...")
