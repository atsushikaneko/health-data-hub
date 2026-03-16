#!/usr/bin/env python3
"""
Apple Health MCP Server

Claude Desktopから自然言語でHealth DBにクエリできるMCPサーバー。

ツール一覧:
- query_health: SQLを直接実行（Claude が自分でSQL組む用）
- get_daily_summary: 指定日の全指標サマリー
- get_sleep: 指定期間の睡眠データ
- get_activity_trend: 指定期間の活動量トレンド
- get_record_types: 利用可能なレコードタイプ一覧
- compare_periods: 2期間の比較（before/after分析用）

Claude Desktop設定 (claude_desktop_config.json):
{
  "mcpServers": {
    "health-data-hub": {
      "command": "python3",
      "args": ["/path/to/health-data-hub/mcp_server/server.py"],
      "env": {
        "HEALTH_DB_PATH": "/path/to/health-data-hub/db/health.db"
      }
    }
  }
}
"""

import json
import os
import sqlite3
from mcp.server.fastmcp import FastMCP

DB_PATH = os.environ.get(
    "HEALTH_DB_PATH",
    os.path.join(os.path.dirname(__file__), "..", "db", "health.db")
)

mcp = FastMCP("apple-health")


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


@mcp.tool()
def query_health(sql: str) -> str:
    """
    Health DBに対してSQLを直接実行する。SELECT文のみ許可。

    テーブル一覧:
    - health_records (type, value, unit, start_date, end_date, source_name)
    - sleep_sessions (sleep_date, bedtime, waketime, duration_hours, bedtime_hour, waketime_hour)
    - sleep_stages (sleep_date, stage, start_date, end_date, duration_minutes)
    - workouts (workout_type, start_date, end_date, duration_minutes, total_energy_burned, total_distance)
    - activity_summaries (date, active_energy_burned, active_energy_burned_goal, exercise_time, exercise_time_goal, stand_hours, stand_hours_goal)

    主なレコードtype:
    - HKQuantityTypeIdentifierStepCount (歩数)
    - HKQuantityTypeIdentifierActiveEnergyBurned (アクティブカロリー)
    - HKQuantityTypeIdentifierHeartRate (心拍数)
    - HKQuantityTypeIdentifierRestingHeartRate (安静時心拍)
    - HKQuantityTypeIdentifierHeartRateVariabilitySDNN (HRV)
    - HKQuantityTypeIdentifierOxygenSaturation (SpO2)
    - HKQuantityTypeIdentifierAppleExerciseTime (運動時間)
    - HKQuantityTypeIdentifierTimeInDaylight (日光時間)
    - HKQuantityTypeIdentifierHeadphoneAudioExposure (ヘッドホン音量)
    - HKQuantityTypeIdentifierAppleSleepingWristTemperature (手首温度)
    - HKQuantityTypeIdentifierRespiratoryRate (呼吸数)
    - HKQuantityTypeIdentifierWalkingStepLength (歩幅)
    - HKQuantityTypeIdentifierSixMinuteWalkTestDistance (6分間歩行距離)
    - HKQuantityTypeIdentifierFlightsClimbed (階段)
    - HKCategoryTypeIdentifierAppleStandHour (スタンド)
    """
    if not sql.strip().upper().startswith("SELECT"):
        return "エラー: SELECT文のみ実行できます"
    if ";" in sql:
        return "エラー: 複数のSQL文は実行できません"
    try:
        conn = get_db()
        rows = conn.execute(sql).fetchall()
        conn.close()
        if not rows:
            return "結果が見つかりませんでした"
        result = [dict(r) for r in rows]
        return json.dumps(result, ensure_ascii=False, indent=2)
    except Exception as e:
        return f"エラー: {str(e)}"


@mcp.tool()
def get_daily_summary(date: str) -> str:
    """
    指定日の全指標サマリーを取得する。
    date: YYYY-MM-DD形式
    """
    conn = get_db()
    summary = {}

    # 歩数
    row = conn.execute("""
        SELECT SUM(value) as total FROM health_records
        WHERE type = 'HKQuantityTypeIdentifierStepCount'
        AND start_date LIKE ?
    """, (f"{date}%",)).fetchone()
    summary["steps"] = round(row["total"]) if row["total"] else 0

    # アクティブカロリー
    row = conn.execute("""
        SELECT SUM(value) as total FROM health_records
        WHERE type = 'HKQuantityTypeIdentifierActiveEnergyBurned'
        AND start_date LIKE ?
    """, (f"{date}%",)).fetchone()
    summary["active_calories"] = round(row["total"], 1) if row["total"] else 0

    # 安静時心拍
    row = conn.execute("""
        SELECT AVG(value) as avg_val FROM health_records
        WHERE type = 'HKQuantityTypeIdentifierRestingHeartRate'
        AND start_date LIKE ?
    """, (f"{date}%",)).fetchone()
    summary["resting_hr"] = round(row["avg_val"], 1) if row["avg_val"] else None

    # HRV
    row = conn.execute("""
        SELECT AVG(value) as avg_val FROM health_records
        WHERE type = 'HKQuantityTypeIdentifierHeartRateVariabilitySDNN'
        AND start_date LIKE ?
    """, (f"{date}%",)).fetchone()
    summary["hrv"] = round(row["avg_val"], 1) if row["avg_val"] else None

    # 日光時間
    row = conn.execute("""
        SELECT SUM(value) as total FROM health_records
        WHERE type = 'HKQuantityTypeIdentifierTimeInDaylight'
        AND start_date LIKE ?
    """, (f"{date}%",)).fetchone()
    summary["daylight_min"] = round(row["total"], 1) if row["total"] else 0

    # ヘッドホン音量
    row = conn.execute("""
        SELECT MAX(value) as max_val, AVG(value) as avg_val FROM health_records
        WHERE type = 'HKQuantityTypeIdentifierHeadphoneAudioExposure'
        AND start_date LIKE ?
    """, (f"{date}%",)).fetchone()
    summary["headphone_max_db"] = round(row["max_val"], 1) if row["max_val"] else None
    summary["headphone_avg_db"] = round(row["avg_val"], 1) if row["avg_val"] else None

    # 睡眠（その日の夜の睡眠）
    row = conn.execute("""
        SELECT duration_hours, bedtime_hour, waketime_hour
        FROM sleep_sessions WHERE sleep_date = ?
    """, (date,)).fetchone()
    if row:
        summary["sleep_hours"] = row["duration_hours"]
        summary["bedtime_hour"] = row["bedtime_hour"]
        summary["waketime_hour"] = row["waketime_hour"]

    # アクティビティリング
    row = conn.execute("""
        SELECT * FROM activity_summaries WHERE date = ?
    """, (date,)).fetchone()
    if row:
        summary["exercise_min"] = row["exercise_time"]
        summary["stand_hours"] = row["stand_hours"]
        cal = row["active_energy_burned"]
        cal_goal = row["active_energy_burned_goal"]
        if cal and cal_goal and cal_goal > 0:
            summary["move_ring_pct"] = round(cal / cal_goal * 100, 1)

    conn.close()
    return json.dumps(summary, ensure_ascii=False, indent=2)


@mcp.tool()
def get_sleep(start_date: str, end_date: str) -> str:
    """
    指定期間の睡眠データを取得する。
    start_date, end_date: YYYY-MM-DD形式
    """
    conn = get_db()
    rows = conn.execute("""
        SELECT
            s.sleep_date,
            s.duration_hours,
            s.bedtime_hour,
            s.waketime_hour,
            COALESCE(SUM(CASE WHEN st.stage='deep' THEN st.duration_minutes END), 0) as deep_min,
            COALESCE(SUM(CASE WHEN st.stage='rem' THEN st.duration_minutes END), 0) as rem_min,
            COALESCE(SUM(CASE WHEN st.stage='core' THEN st.duration_minutes END), 0) as core_min,
            COALESCE(SUM(CASE WHEN st.stage='awake' THEN st.duration_minutes END), 0) as awake_min
        FROM sleep_sessions s
        LEFT JOIN sleep_stages st ON s.sleep_date = st.sleep_date
        WHERE s.sleep_date BETWEEN ? AND ?
        GROUP BY s.sleep_date
        ORDER BY s.sleep_date
    """, (start_date, end_date)).fetchall()
    conn.close()

    if not rows:
        return "この期間の睡眠データが見つかりませんでした"
    return json.dumps([dict(r) for r in rows], ensure_ascii=False, indent=2)


@mcp.tool()
def get_activity_trend(start_date: str, end_date: str, interval: str = "daily") -> str:
    """
    指定期間の活動量トレンドを取得する。
    interval: "daily" or "weekly" or "monthly"
    """
    conn = get_db()

    if interval not in ("daily", "weekly", "monthly"):
        return "エラー: intervalは daily/weekly/monthly のいずれか"

    if interval == "weekly":
        group_expr = "strftime('%Y-W%W', start_date)"
    elif interval == "monthly":
        group_expr = "strftime('%Y-%m', start_date)"
    else:
        group_expr = "DATE(start_date)"

    rows = conn.execute(f"""
        SELECT
            {group_expr} as period,
            SUM(CASE WHEN type='HKQuantityTypeIdentifierStepCount' THEN value END) as steps,
            SUM(CASE WHEN type='HKQuantityTypeIdentifierActiveEnergyBurned' THEN value END) as active_cal,
            SUM(CASE WHEN type='HKQuantityTypeIdentifierAppleExerciseTime' THEN value END) as exercise_min,
            SUM(CASE WHEN type='HKQuantityTypeIdentifierTimeInDaylight' THEN value END) as daylight_min,
            AVG(CASE WHEN type='HKQuantityTypeIdentifierRestingHeartRate' THEN value END) as resting_hr,
            AVG(CASE WHEN type='HKQuantityTypeIdentifierHeartRateVariabilitySDNN' THEN value END) as hrv
        FROM health_records
        WHERE start_date BETWEEN ? AND ?
        GROUP BY {group_expr}
        ORDER BY period
    """, (start_date, end_date + " 23:59:59")).fetchall()
    conn.close()

    if not rows:
        return "この期間のデータが見つかりませんでした"

    result = []
    for r in rows:
        d = dict(r)
        for k, v in d.items():
            if isinstance(v, float):
                d[k] = round(v, 1)
        result.append(d)

    return json.dumps(result, ensure_ascii=False, indent=2)


@mcp.tool()
def get_record_types() -> str:
    """DBに存在するレコードタイプと件数の一覧を返す"""
    conn = get_db()
    rows = conn.execute("""
        SELECT type, COUNT(*) as count,
               MIN(start_date) as first_date,
               MAX(start_date) as last_date
        FROM health_records
        GROUP BY type
        ORDER BY count DESC
    """).fetchall()
    conn.close()
    return json.dumps([dict(r) for r in rows], ensure_ascii=False, indent=2)


@mcp.tool()
def compare_periods(period1_start: str, period1_end: str,
                    period2_start: str, period2_end: str) -> str:
    """
    2期間を比較する（例: 朝散歩before/after）。
    全主要指標の平均値を両期間で算出して比較。
    """
    conn = get_db()
    results = {}

    for label, start, end in [("period1", period1_start, period1_end),
                                ("period2", period2_start, period2_end)]:
        days = conn.execute(f"""
            SELECT COUNT(DISTINCT DATE(start_date)) as days
            FROM health_records
            WHERE start_date BETWEEN ? AND ?
        """, (start, end + " 23:59:59")).fetchone()["days"]

        metrics = conn.execute(f"""
            SELECT
                SUM(CASE WHEN type='HKQuantityTypeIdentifierStepCount' THEN value END) / {max(days,1)} as avg_steps,
                SUM(CASE WHEN type='HKQuantityTypeIdentifierActiveEnergyBurned' THEN value END) / {max(days,1)} as avg_cal,
                SUM(CASE WHEN type='HKQuantityTypeIdentifierAppleExerciseTime' THEN value END) / {max(days,1)} as avg_exercise_min,
                SUM(CASE WHEN type='HKQuantityTypeIdentifierTimeInDaylight' THEN value END) / {max(days,1)} as avg_daylight_min,
                AVG(CASE WHEN type='HKQuantityTypeIdentifierRestingHeartRate' THEN value END) as avg_rhr,
                AVG(CASE WHEN type='HKQuantityTypeIdentifierHeartRateVariabilitySDNN' THEN value END) as avg_hrv
            FROM health_records
            WHERE start_date BETWEEN ? AND ?
        """, (start, end + " 23:59:59")).fetchone()

        sleep = conn.execute("""
            SELECT AVG(duration_hours) as avg_sleep,
                   AVG(bedtime_hour) as avg_bedtime
            FROM sleep_sessions
            WHERE sleep_date BETWEEN ? AND ?
        """, (start, end)).fetchone()

        d = dict(metrics)
        d["days"] = days
        d["avg_sleep_hours"] = round(sleep["avg_sleep"], 2) if sleep["avg_sleep"] else None
        d["avg_bedtime_hour"] = round(sleep["avg_bedtime"], 2) if sleep["avg_bedtime"] else None

        for k, v in d.items():
            if isinstance(v, float):
                d[k] = round(v, 1)

        results[label] = d

    conn.close()
    return json.dumps(results, ensure_ascii=False, indent=2)


@mcp.tool()
def get_weather(start_date: str, end_date: str) -> str:
    """
    指定期間の天気データを取得する。
    start_date, end_date: YYYY-MM-DD形式

    weather_codeの意味:
    0=快晴, 1=晴れ, 2=曇り, 3=曇天,
    45,48=霧, 51-55=霧雨, 56-57=凍結霧雨,
    61-65=雨, 66-67=凍結雨, 71-75=雪,
    77=霧雪, 80-82=にわか雨, 85-86=にわか雪,
    95=雷雨, 96,99=雹を伴う雷雨
    """
    conn = get_db()
    rows = conn.execute("""
        SELECT * FROM weather_records
        WHERE date BETWEEN ? AND ?
        ORDER BY date
    """, (start_date, end_date)).fetchall()
    conn.close()

    if not rows:
        return "天気データが見つかりません。python3 parser/fetch_weather.py を実行してください"
    return json.dumps([dict(r) for r in rows], ensure_ascii=False, indent=2)


@mcp.tool()
def get_health_and_weather(start_date: str, end_date: str) -> str:
    """
    指定期間の健康データと天気データを統合して日次で返す。
    天気×睡眠、天気×活動量の分析に使う。
    """
    conn = get_db()
    rows = conn.execute("""
        SELECT
            w.date,
            w.temp_max, w.temp_min, w.temp_mean,
            w.precipitation, w.humidity_mean, w.pressure_mean,
            w.sunshine_hours, w.weather_code, w.wind_speed_max,
            s.duration_hours as sleep_hours,
            s.bedtime_hour,
            COALESCE(SUM(CASE WHEN st.stage='deep' THEN st.duration_minutes END), 0) as deep_min,
            COALESCE(SUM(CASE WHEN st.stage='awake' THEN st.duration_minutes END), 0) as awake_min,
            a.active_energy_burned as active_cal,
            a.exercise_time as exercise_min,
            a.stand_hours
        FROM weather_records w
        LEFT JOIN sleep_sessions s ON w.date = s.sleep_date
        LEFT JOIN sleep_stages st ON w.date = st.sleep_date
        LEFT JOIN activity_summaries a ON w.date = a.date
        WHERE w.date BETWEEN ? AND ?
        GROUP BY w.date
        ORDER BY w.date
    """, (start_date, end_date)).fetchall()
    conn.close()

    if not rows:
        return "データが見つかりません。この期間の健康データと天気データが両方存在することを確認してください"

    result = []
    for r in rows:
        d = dict(r)
        for k, v in d.items():
            if isinstance(v, float):
                d[k] = round(v, 2)
        result.append(d)

    return json.dumps(result, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    mcp.run()
