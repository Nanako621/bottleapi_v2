# app_realtime_v2.py  （含 mock_publisher）
# 保留你的設計：1) aiomqtt 在 background asyncio loop 訂閱  2) gevent + Bottle 提供 WebSocket  3) queue 作橋接
# 新增：mock_publisher 每 3 秒推送與前端相容的指標（當 MQTT 不活躍時仍可看到即時資料）

import sqlite3, sys, ssl, json, asyncio, threading, queue, certifi, time, random
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
# 初始模擬狀態（與新版前端相容）
mock_state = {
    "heart_rate": 78,
    "steps": 4200,
    "active_minutes": 35,
    "sleep_hours": 7.2,       # 同一天固定
    "sleep_quality": 82,      # 同一天固定
    "sedentary_time": 25,     # 單位：分鐘
    "calories": 1680,
    "spo2": 97,
    "hrv": 48,
    "height": 178,
    "weight": 64,
    "is_sedentary": True,     # 是否正在久坐
    "last_sedentary_tick": time.time()  # 上次久坐+1分鐘的時間
}

def clamp(v, lo, hi):
    return max(lo, min(hi, v))

def mock_publisher():
    """每 3 秒產生一筆模擬資料放入 inbox（格式跟 MQTT 相同：{topic, payload}）"""
    while True:
        last = get_last_mqtt_ts()
        now = time.time()

        # 最近 2 秒有 MQTT，避免 mock 搶著覆蓋
        if now - last < 2.0:
            time.sleep(3.0)
            continue

        # 1. 心率：小幅波動
        mock_state["heart_rate"] = int(clamp(
            mock_state["heart_rate"] + random.randint(-3, 3), 45, 120
        ))

        # 2. 隨機判斷現在是否仍在坐著
        # 大部分時間坐著，偶爾起身走動
        if random.random() < 0.75:
            mock_state["is_sedentary"] = True
        else:
            mock_state["is_sedentary"] = False

        step_add = 0

        # 3. 步數：只有不在久坐時才增加，而且每次只加 1~5 步
        if not mock_state["is_sedentary"]:
            step_add = random.randint(1, 5)
            mock_state["steps"] = int(clamp(
                mock_state["steps"] + step_add, 0, 30000
            ))

            # 有走動就代表久坐中斷，歸零重新算
            mock_state["sedentary_time"] = 0
            mock_state["last_sedentary_tick"] = now

            # 運動時間很少量增加
            if random.random() < 0.3:
                mock_state["active_minutes"] = int(clamp(
                    mock_state["active_minutes"] + 1, 0, 300
                ))

        else:
            # 4. 久坐時：步數不增加
            # 真正滿 60 秒才讓久坐時間 +1
            elapsed = now - mock_state["last_sedentary_tick"]
            if elapsed >= 60:
                add_minutes = int(elapsed // 60)
                mock_state["sedentary_time"] = int(clamp(
                    mock_state["sedentary_time"] + add_minutes, 0, 180
                ))
                mock_state["last_sedentary_tick"] += add_minutes * 60

        # 5. 睡眠資料：同一天固定，不更新
        # sleep_hours / sleep_quality 維持不動

        # 6. 卡路里：跟活動量微幅增加
        mock_state["calories"] = int(clamp(
            mock_state["calories"] + random.randint(0, 2) + step_add,
            800, 4000
        ))

        # 7. 血氧：小幅波動
        mock_state["spo2"] = int(clamp(
            mock_state["spo2"] + random.randint(-1, 1), 94, 99
        ))

        # 8. HRV：小幅波動
        mock_state["hrv"] = int(clamp(
            mock_state["hrv"] + random.randint(-2, 2), 10, 120
        ))

        payload = {
            "timestamp": int(now),
            "heart_rate": mock_state["heart_rate"],
            "steps": mock_state["steps"],
            "active_minutes": mock_state["active_minutes"],
            "sleep_hours": mock_state["sleep_hours"],
            "sleep_quality": mock_state["sleep_quality"],
            "sedentary_time": mock_state["sedentary_time"],
            "calories": mock_state["calories"],
            "spo2": mock_state["spo2"],
            "hrv": mock_state["hrv"],
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

        time.sleep(3.0)

# 啟動 mock publisher 背景執行緒（daemon）
threading.Thread(target=mock_publisher, daemon=True).start()

# ---- Bottle 路由設定 (請確保這段在 if __name__ == "__main__" 之前) ----
app = Bottle()

@app.get("/")
def index():
    return static_file('iot_intro.html', root='.')

# --- 新增：處理圖片與字體檔案的路由 ---
@app.route('/<filename:re:.*\\.(png|jpg|ttf)>')
def send_static_res(filename):
    return static_file(filename, root='.')

@app.get("/realmedashboard.html")
def dashboard():
    return static_file('realmedashboard.html', root='.')

@app.get("/patient_entry.html")
def patient_entry_page():
    return static_file('patient_entry.html', root='.')

# 登入驗證 API
@app.post('/api/login')
def do_login():
    data = request.json
    email = data.get('email')
    password = data.get('password')
    
    try:
        conn = sqlite3.connect('iot_platform.db')
        cur = conn.cursor()
        # 查詢是否有匹配的帳號密碼
        cur.execute("SELECT * FROM users WHERE email=? AND password=?", (email, password))
        user = cur.fetchone()
        
        if user:
            return {"status": "success", "message": "登入成功"}
        else:
            response.status = 401
            return {"status": "error", "message": "帳號或密碼錯誤"}
    except Exception as e:
        response.status = 500
        return {"status": "error", "message": str(e)}
    finally:
        conn.close()


# 註冊帳號 API
@app.post('/api/register')
def do_register():
    data = request.json
    email = data.get('email')
    password = data.get('password')
    
    try:
        conn = sqlite3.connect('iot_platform.db')
        cur = conn.cursor()
        cur.execute("INSERT INTO users (email, password) VALUES (?, ?)", (email, password))
        conn.commit()
        return {"status": "success", "message": "註冊成功"}
    except sqlite3.IntegrityError:
        # 當 email 重複時，會跳到這裡
        response.status = 400
        return {"status": "error", "message": "此 Email 已被註冊"}
    finally:
        conn.close()

# 登錄/更新 病人資料 API (記憶出生年月日版本)
@app.post('/api/patient_entry')
def add_patient():
    data = request.json
    email = data.get('email')  # 使用 email 作為主鍵
    
    if not email:
        response.status = 400
        return {"status": "error", "message": "缺少 Email 資訊"}

    try:
        conn = sqlite3.connect('iot_platform.db')
        cur = conn.cursor()
        
        # 檢查該 Email 是否已經存在於 patients 表格中
        cur.execute("SELECT email FROM patients WHERE email=?", (email,))
        exists = cur.fetchone()
        
        if exists:
            # 如果資料已存在，就更新 (UPDATE)
            # 欄位調整為：birth_year, birth_month, birth_day
            cur.execute("""
                UPDATE patients 
                SET name=?, birth_year=?, birth_month=?, birth_day=?, gender=?, height=?, weight=?
                WHERE email=?
            """, (
                data['name'], 
                data['birth_year'], 
                data['birth_month'], 
                data['birth_day'], 
                data['gender'], 
                data['height'], 
                data['weight'], 
                email
            ))
        else:
            # 如果不存在，就新增 (INSERT)
            cur.execute("""
                INSERT INTO patients (email, name, birth_year, birth_month, birth_day, gender, height, weight) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                email, 
                data['name'], 
                data['birth_year'], 
                data['birth_month'], 
                data['birth_day'], 
                data['gender'], 
                data['height'], 
                data['weight']
            ))
            
        conn.commit()
        return {"status": "success", "message": "資料已同步至雲端（已記錄出生年月日）"}
    except Exception as e:
        print(f"Database Error: {e}")
        response.status = 500
        return {"status": "error", "message": f"資料庫寫入失敗: {str(e)}"}
    finally:
        conn.close()
# 獲取個人資料 API (供進入頁面時顯示舊資料)
@app.get('/api/get_patient/<email>')
def get_patient(email):
    try:
        conn = sqlite3.connect('iot_platform.db')
        cur = conn.cursor()
        # 查詢所有我們需要的欄位
        cur.execute("""
            SELECT name, birth_year, birth_month, birth_day, gender, height, weight 
            FROM patients WHERE email=?
        """, (email,))
        row = cur.fetchone()
        
        if row:
            return {
                "status": "success", 
                "data": {
                    "name": row[0],
                    "birth_year": row[1],
                    "birth_month": row[2],
                    "birth_day": row[3],
                    "gender": row[4],
                    "height": row[5],
                    "weight": row[6]
                }
            }
        else:
            return {"status": "empty", "message": "尚無歷史資料"}
    except Exception as e:
        return {"status": "error", "message": str(e)}
    finally:
        conn.close()
        
                
if __name__ == "__main__":
    print("啟動 Web Server (0.0.0.0:8080)...")
    server = WSGIServer(("0.0.0.0", 8080), app, handler_class=WebSocketHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("Shutting down...")
        
        