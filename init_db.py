import sqlite3

def init_db():
    conn = sqlite3.connect('iot_platform.db')
    cursor = conn.cursor()
    
    # 使用者帳號表 (保持不變)
    cursor.execute('CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT UNIQUE NOT NULL, password TEXT NOT NULL)')
    
    # 刪除舊表並建立新結構
    cursor.execute('DROP TABLE IF EXISTS patients')
    cursor.execute('''
        CREATE TABLE patients (
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
    
    conn.commit()
    conn.close()
    print("資料庫已更新：現在會記憶出生年、月、日。")

if __name__ == '__main__':
    init_db()