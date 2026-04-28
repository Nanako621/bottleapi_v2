import sqlite3

def init_db():
    conn = sqlite3.connect('iot_platform.db')
    cursor = conn.cursor()
    
    # 1. 使用者帳號表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT, 
            email TEXT UNIQUE NOT NULL, 
            password TEXT NOT NULL
        )
    ''')
    
    # 2. 個人資料表 
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS patients (
            email TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            birth_year INTEGER,
            birth_month INTEGER,
            birth_day INTEGER,
            gender TEXT,
            height REAL,
            weight REAL
        )
    ''')
    
    # 3. 聯絡我們資料表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS contact_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            message TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # === [新增] AI 與健康數據分析相關表格 ===

    # 4. 短期詳細紀錄表 (每 5 分鐘一筆，預計儲存 7 天)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS health_logs (
            log_id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL,
            heart_rate REAL, 
            steps_delta INTEGER,    -- 這段時間內增加的步數
            spo2 REAL,
            recorded_at DATETIME DEFAULT (datetime('now', 'localtime')),
            FOREIGN KEY (email) REFERENCES users(email)
        )
    ''')

    # 5. 每日統計摘要表 (AI 深度分析用)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS daily_summaries (
            summary_id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL,
            summary_date DATE NOT NULL,
            avg_heart_rate REAL,
            max_heart_rate INTEGER,
            total_steps INTEGER,
            avg_spo2 REAL,
            sleep_score INTEGER,    -- 預留給 AI 計算
            stress_level TEXT,      -- 預留給 AI 評估
            created_at DATETIME DEFAULT (datetime('now', 'localtime')),
            UNIQUE(email, summary_date),
            FOREIGN KEY (email) REFERENCES users(email)
        )
    ''')
    
    conn.commit()
    conn.close()
    print("資料庫檢查完成：健康數據紀錄表 (health_logs & daily_summaries) 已建立。")

if __name__ == '__main__':
    init_db()