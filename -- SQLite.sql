-- 1. 刪除舊有的表格與所有數據
DROP TABLE IF EXISTS health_logs;

-- 2. 依照你要求的順序重新建立表格 (時間在最後)
CREATE TABLE health_logs (
    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT,              -- 1. email
    heart_rate REAL,         -- 2. heart_rate
    steps_delta INTEGER,     -- 3. steps_delta
    spo2 REAL,               -- 4. spo2
    active_minutes REAL,     -- 5. active_minutes
    sleep_hours REAL,        -- 6. sleep_hours
    sleep_quality REAL,      -- 7. sleep_quality
    sedentary_time REAL,     -- 8. sedentary_time
    calories REAL,           -- 9. calories
    hrv REAL,                -- 10. hrv
    recorded_at DATETIME     -- 11. recorded_at (時間在最後)
);