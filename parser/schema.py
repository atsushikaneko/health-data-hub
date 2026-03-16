"""
Apple Health SQLite Schema & DB Initialization

設計方針:
- health_records: 全レコードを1テーブルに格納（汎用）
- sleep_sessions: 睡眠セッション復元結果（パーサー側で構築）
- sleep_stages: 睡眠ステージ詳細（deep/rem/core/awake）
- workouts: ワークアウト記録
- ecg_readings: ECG波形データ
- activity_summaries: 日次アクティビティリング
- import_log: インポート履歴管理（差分取り込み用）

冪等性: UNIQUE制約 + INSERT OR IGNORE で何度フルインポートしても重複しない
"""

import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'db', 'health.db')


def get_connection(db_path=None):
    path = db_path or DB_PATH
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path=None):
    conn = get_connection(db_path)
    c = conn.cursor()

    # ===== メインレコードテーブル =====
    c.execute("""
    CREATE TABLE IF NOT EXISTS health_records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        type TEXT NOT NULL,
        value REAL,
        unit TEXT,
        start_date TEXT NOT NULL,
        end_date TEXT,
        source_name TEXT,
        source_version TEXT,
        device TEXT,
        creation_date TEXT,
        UNIQUE(type, start_date, end_date, source_name)
    )
    """)

    # ===== 睡眠セッション（パーサーが復元） =====
    c.execute("""
    CREATE TABLE IF NOT EXISTS sleep_sessions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sleep_date TEXT NOT NULL,
        bedtime TEXT NOT NULL,
        waketime TEXT NOT NULL,
        duration_hours REAL NOT NULL,
        bedtime_hour REAL,
        waketime_hour REAL,
        UNIQUE(sleep_date, bedtime)
    )
    """)

    # ===== 睡眠ステージ =====
    c.execute("""
    CREATE TABLE IF NOT EXISTS sleep_stages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sleep_date TEXT NOT NULL,
        stage TEXT NOT NULL,
        start_date TEXT NOT NULL,
        end_date TEXT NOT NULL,
        duration_minutes REAL NOT NULL,
        UNIQUE(sleep_date, stage, start_date)
    )
    """)

    # ===== ワークアウト =====
    c.execute("""
    CREATE TABLE IF NOT EXISTS workouts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        workout_type TEXT NOT NULL,
        start_date TEXT NOT NULL,
        end_date TEXT NOT NULL,
        duration_minutes REAL,
        total_energy_burned REAL,
        total_distance REAL,
        source_name TEXT,
        UNIQUE(workout_type, start_date, end_date)
    )
    """)

    # ===== ECG =====
    c.execute("""
    CREATE TABLE IF NOT EXISTS ecg_readings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        start_date TEXT NOT NULL,
        classification TEXT,
        average_heart_rate REAL,
        sampling_frequency REAL,
        voltage_data TEXT,
        UNIQUE(start_date)
    )
    """)

    # ===== アクティビティサマリー =====
    c.execute("""
    CREATE TABLE IF NOT EXISTS activity_summaries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL,
        active_energy_burned REAL,
        active_energy_burned_goal REAL,
        exercise_time REAL,
        exercise_time_goal REAL,
        stand_hours REAL,
        stand_hours_goal REAL,
        UNIQUE(date)
    )
    """)

    # ===== インポートログ =====
    c.execute("""
    CREATE TABLE IF NOT EXISTS import_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        imported_at TEXT NOT NULL,
        filename TEXT,
        records_total INTEGER,
        records_inserted INTEGER,
        max_date TEXT
    )
    """)

    # ===== インデックス =====
    c.execute("CREATE INDEX IF NOT EXISTS idx_hr_type_start ON health_records(type, start_date)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_hr_start ON health_records(start_date)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_hr_type ON health_records(type)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ss_date ON sleep_sessions(sleep_date)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_stages_date ON sleep_stages(sleep_date)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_as_date ON activity_summaries(date)")

    conn.commit()
    conn.close()
    print(f"DB initialized: {db_path or DB_PATH}")


if __name__ == "__main__":
    init_db()
