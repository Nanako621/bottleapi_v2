# app_realtime_v2.py  （含 mock_publisher）
# 保留你的設計：1) aiomqtt 在 background asyncio loop 訂閱  2) gevent + Bottle 提供 WebSocket  3) queue 作橋接
# 新增：mock_publisher 每 3 秒推送與前端相容的指標（當 MQTT 不活躍時仍可看到即時資料）

import sqlite3, sys, ssl, json, asyncio, threading, queue, certifi, time, random, os
from bottle import Bottle, request, abort, response, static_file
from gevent import sleep as gsleep, spawn as gspawn
from gevent.pywsgi import WSGIServer
from geventwebsocket.handler import WebSocketHandler
from geventwebsocket import WebSocketError
from aiomqtt import Client
from statistics import mean
from datetime import datetime
from dotenv import load_dotenv
from google import genai

# 1. 確保精準讀取 .env 檔案
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(BASE_DIR, ".env")
load_dotenv(env_path)

# 2. 取得並檢查金鑰
api_key = os.getenv("GEMINI_API_KEY")

if not api_key:
    print("❌ 錯誤：找不到 GEMINI_API_KEY，請檢查 .env 檔案內容與位置！")
else:
    # 顯示金鑰前後幾碼，方便你確認是不是自己申請的那組
    print(f"✅ GEMINI_API_KEY 讀取成功！")
    print(f"目前使用的 Key: {api_key[:6]}......{api_key[-4:]}")

# 3. 初始化 Gemini Client (關鍵：加入 http_options 解決 404 問題)
try:
    gemini_client = genai.Client(
        api_key=api_key,
        http_options={'api_version': 'v1'} # 強制使用 v1 正式版介面，避免 v1beta 找不到模型
    )
    print("🚀 Gemini Client 初始化完成 (API Version: v1)")
except Exception as e:
    print(f"❌ Client 初始化失敗: {e}")

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
data_buffer = {}
buffer_lock = threading.Lock()
current_user_email = None  # 初始為 None，表示未登入


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

def flush_to_db(email):
    with buffer_lock:
        # 從緩存中取出該使用者的數據並清空
        data = data_buffer.pop(email, None)
    
    if data:
        try:
            # 計算各項數值 (均值或差值)
            avg_hr = sum(data['hr']) / len(data['hr']) if data['hr'] else 0
            avg_spo2 = sum(data['spo2']) / len(data['spo2']) if data['spo2'] else 0
            # 步數計算差值 (最後一筆減去第一筆)
            total_steps = data['steps'][-1] - data['steps'][0] if len(data['steps']) > 1 else 0
            
            avg_active = sum(data['active_min']) / len(data['active_min']) if data['active_min'] else 0
            avg_sleep_h = data['sleep_h'][-1] if data['sleep_h'] else 0  # 睡眠取最新狀態
            avg_sleep_q = data['sleep_q'][-1] if data['sleep_q'] else 0
            avg_sedentary = sum(data['sedentary']) / len(data['sedentary']) if data['sedentary'] else 0
            avg_cal = sum(data['cal']) / len(data['cal']) if data['cal'] else 0
            avg_hrv = sum(data['hrv']) / len(data['hrv']) if data['hrv'] else 0

            # 連接資料庫並寫入
            conn = sqlite3.connect('iot_platform.db')
            cur = conn.cursor()
            
            # 嚴格對齊你要求的 11 個項目順序
            sql = """
                INSERT INTO health_logs 
                (email, heart_rate, steps_delta, spo2, active_minutes, 
                 sleep_hours, sleep_quality, sedentary_time, calories, hrv, recorded_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            # 變數順序必須與上方 SQL 括號內的欄位順序完全對應
            params = (
                email,           # 1
                avg_hr,          # 2
                total_steps,     # 3
                avg_spo2,        # 4
                avg_active,      # 5
                avg_sleep_h,     # 6
                avg_sleep_q,     # 7
                avg_sedentary,   # 8
                avg_cal,         # 9
                avg_hrv,         # 10
                datetime.now()   # 11 (時間放在最後)
            )
            
            cur.execute(sql, params)
            conn.commit()
            conn.close()
            logging.info(f" [DB Export] 已成功存入 {email} 的 11 項彙整數據（含 9 項生理指標）。")
            
        except Exception as e:
            logging.error(f" [DB Export] 寫入失敗: {e}")
        
def maintenance_task():
    """每日凌晨執行：產生昨日摘要並清理 7 天前的舊 Log"""
    while True:
        # 1. 計算距離明天凌晨 00:01 還有多久
        now = time.localtime()
        # 計算秒數，讓它在每天凌晨執行
        seconds_until_midnight = (24 - now.tm_hour - 1) * 3600 + (60 - now.tm_min - 1) * 60 + (60 - now.tm_sec)
        logging.info(f" [System] 維護任務將在 {seconds_until_midnight} 秒後執行")
        
        # 等待到凌晨
        gsleep(seconds_until_midnight + 60) 
        
        logging.info(" [System] 開始執行每日維護：產生摘要與清理舊數據...")
        try:
            conn = sqlite3.connect('iot_platform.db')
            cursor = conn.cursor()
            
            # 1. 產生昨日摘要 (計算昨日平均值並寫入 daily_summaries)
            # 這裡會幫每位有資料的使用者計算一筆昨日總結
            cursor.execute('''
                INSERT OR IGNORE INTO daily_summaries 
                (email, summary_date, avg_heart_rate, max_heart_rate, total_steps, avg_spo2)
                SELECT 
                    email, 
                    date('now', '-1 day', 'localtime'),
                    AVG(heart_rate), 
                    MAX(heart_rate), 
                    SUM(steps_delta), 
                    AVG(spo2)
                FROM health_logs
                WHERE date(recorded_at) = date('now', '-1 day', 'localtime')
                GROUP BY email
            ''')
            
            # 2. 清理過期數據 (刪除超過 7 天的詳細 Log)
            cursor.execute("DELETE FROM health_logs WHERE recorded_at < date('now', '-7 days', 'localtime')")
            
            conn.commit()
            conn.close()
            logging.info(" [System] 每日維護完成：摘要已生成，舊資料已清理。")
        except Exception as e:
            logging.error(f" [System] 維護任務發生錯誤: {e}")
            
            
def broadcaster():
    """從 inbox 取出字串，送到所有 WebSocket client，並處理 5 分鐘數據彙整"""
    sent_counter = 0
    while True:
        try:
            msg = inbox.get(timeout=0.1)
        except queue.Empty:
            gsleep(0.05)
            continue

        # --- [邏輯優化] 只有登入後才處理數據存儲 ---
        if current_user_email is not None:
            try:
                raw_data = json.loads(msg)
                payload = raw_data.get("payload")
                if isinstance(payload, dict):
                    email = current_user_email  # 強制使用目前登入的帳號，防止數據誤植
                    
                    hr = payload.get("heart_rate")
                    spo2 = payload.get("spo2")
                    steps = payload.get("steps")

                    if hr is not None and spo2 is not None and steps is not None:
                        with buffer_lock:
                            # 確保初始化包含這 9 個列表
                            if email not in data_buffer:
                                data_buffer[email] = {
                                    'hr': [], 'spo2': [], 'steps': [], 
                                    'active_min': [], 'sleep_h': [], 'sleep_q': [],
                                    'sedentary': [], 'cal': [], 'hrv': [],
                                    'start_time': time.time()
                                }

                            # 確保 payload 提取正確 (假設 payload 是來自前端的字典)
                            p = raw_data.get("payload", {})
                            data_buffer[email]['hr'].append(p.get("heart_rate", 0))
                            data_buffer[email]['spo2'].append(p.get("spo2", 0))
                            data_buffer[email]['steps'].append(p.get("steps", 0))
                            data_buffer[email]['active_min'].append(p.get("active_minutes", 0))
                            data_buffer[email]['sleep_h'].append(p.get("sleep_hours", 0))
                            data_buffer[email]['sleep_q'].append(p.get("sleep_quality", 0))
                            data_buffer[email]['sedentary'].append(p.get("sedentary_time", 0))
                            data_buffer[email]['cal'].append(p.get("calories", 0))
                            data_buffer[email]['hrv'].append(p.get("hrv", 0))
                            
                            # 判斷是否累積滿 5 分鐘
                            if time.time() - data_buffer[email]['start_time'] >= 300:
                                threading.Thread(target=flush_to_db, args=(email,), daemon=True).start()
            except Exception as e:
                logging.debug(f"[buffer] 解析失敗: {e}")
        else:
            # 如果沒登入，我們什麼都不做（不進入 data_buffer），所以不會計時，也不會寫入 DB
            pass

        # 無論有沒有登入，廣播都要繼續（這樣首頁或展示畫面才會有即時跳動的數字）
        dead = []
        for ws in list(sockets):
            try:
                ws.send(msg)
                sent_counter += 1
            except Exception:
                dead.append(ws)
        for ws in dead:
            sockets.discard(ws)
            
# 啟動 broadcaster（gevent）
gspawn(broadcaster)
# 啟動每日維護任務
gspawn(maintenance_task)

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

@app.get("/iot_intro.html")
def intro():
    return static_file("iot_intro.html", root=".")

@app.get("/patient_entry.html")
def patient_entry_page():
    return static_file('patient_entry.html', root='.')

@app.post('/api/login')
def do_login():
    global current_user_email
    data = request.json
    email = data.get('email')
    password = data.get('password')
    
    try:
        conn = sqlite3.connect('iot_platform.db')
        cur = conn.cursor()
        cur.execute("SELECT * FROM users WHERE email=? AND password=?", (email, password))
        user = cur.fetchone()
        
        if user:
            current_user_email = email # 【關鍵】在此時才標記登入成功
            logging.info(f" [System] 使用者 {email} 登入成功，開始監控數據。")
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
        
@app.post('/api/contact')
def handle_contact():
    data = request.json
    name = data.get('name')
    email = data.get('email')
    message = data.get('message')

    if not name or not email or not message:
        return {"status": "error", "message": "所有欄位皆為必填"}

    try:
        conn = sqlite3.connect('iot_platform.db')
        cursor = conn.cursor()
        cursor.execute(
            'INSERT INTO contact_messages (name, email, message) VALUES (?, ?, ?)',
            (name, email, message)
        )
        conn.commit()
        conn.close()
        return {"status": "success", "message": "留言已送出，我們會儘快聯絡您！"}
    except Exception as e:
        return {"status": "error", "message": str(e)}
    
@app.post("/api/chatgpt")
def chatgpt_assistant():
    data = request.json or {}
    question = data.get("question", "").strip()

    if not question:
        response.status = 400
        return {"status": "error", "answer": "請輸入問題。"}

    system_prompt = """
你是大學生健康 IoT 平台的 AI 健康小幫手。
請使用繁體中文回答。
只能提供健康管理、衛教、生活習慣、睡眠、運動、久坐、心率、血氧等一般建議。
不能診斷疾病，不能開藥，不能取代醫師。
回答要簡短、清楚、適合大學生理解。
最後一定要提醒：若有明顯不適或症狀持續，請尋求醫療專業協助。
"""

    try:
        result = gemini_client.models.generate_content(
            model="gemini-2.5-flash-lite",
            contents=system_prompt + "\n\n使用者問題：" + question
        )

        return {
            "status": "success",
            "answer": result.text
        }

    except Exception as e:
        msg = str(e)

        if "quota" in msg.lower() or "429" in msg:
            answer = "目前 Gemini 額度暫時受限，先提供基礎健康建議：\n\n"

            if "睡" in question:
                answer += "睡眠品質不好可能與壓力、作息不固定、睡前使用手機或咖啡因攝取有關。建議固定睡眠時間、睡前 30 分鐘減少螢幕使用，並維持安靜舒適的睡眠環境。"
            elif "久坐" in question or "坐" in question:
                answer += "久坐時間過長可能增加腰背不適與代謝負擔。建議每 30–60 分鐘起身活動 3–5 分鐘，做簡單伸展或走動。"
            elif "心率" in question or "心跳" in question:
                answer += "一般靜息心率約 50–100 bpm。若長期偏高或伴隨胸悶、頭暈、喘等不適，建議尋求醫療評估。"
            elif "血氧" in question or "spo2" in question.lower():
                answer += "一般血氧常見約 95% 以上。若明顯低於平常，或合併喘、胸悶等症狀，應盡快尋求醫療協助。"
            else:
                answer += "建議維持規律作息、均衡飲食、適度運動，並避免長時間久坐。"

            answer += "\n\n以上建議僅供健康管理與衛教參考，若有明顯不適或症狀持續，請尋求醫療專業協助。"
            return {"status": "success", "answer": answer}

        response.status = 500
        return {
            "status": "error",
            "answer": "AI 服務暫時無法連線：" + msg
        }
        
        
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
    
    
@app.get("/ws")
def handle_websocket():
    ws = request.environ.get('wsgi.websocket')
    if not ws:
        abort(400, "Expected WebSocket request.")
    sockets.add(ws)
    logging.info(f" [WS] 新連線已建立: {id(ws)}")
    try:
        while True:
            msg = ws.receive()
            if msg is None: break
    except WebSocketError:
        pass
    finally:
        sockets.discard(ws)
        logging.info(f" [WS] 連線已中斷: {id(ws)}")
                
if __name__ == "__main__":
    print("啟動 Web Server (0.0.0.0:8080)...")
    server = WSGIServer(("0.0.0.0", 8080), app, handler_class=WebSocketHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("Shutting down...")
        
        