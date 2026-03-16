#!/usr/bin/env python3
"""
Weather Data Fetcher → SQLite

Open-Meteo API（無料・登録不要）から天気データを取得してSQLiteに投入。

Usage:
    # 過去1年分を取得
    python3 fetch_weather.py

    # 期間指定
    python3 fetch_weather.py --start 2025-07-01 --end 2026-03-16

    # DB指定
    python3 fetch_weather.py --db /path/to/health.db

設計:
- INSERT OR REPLACEで冪等（何度実行しても最新データで上書き）
- Apple Healthのデータ期間に合わせてデフォルトで過去1年取得
- デフォルト座標は変更可能（--lat / --lon オプション）
"""

import argparse
import json
import logging
import os
import sqlite3
from datetime import datetime, timedelta

# Open-MeteoはHTTPSアクセスが必要
# ネットワーク制限がある環境では事前にJSONを用意して--fileオプションで読み込み可
try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

# デフォルト: 東京駅
DEFAULT_LAT = 35.6812
DEFAULT_LON = 139.7671
DEFAULT_DB = os.path.join(os.path.dirname(__file__), '..', 'db', 'health.db')


def init_weather_table(conn):
    """天気テーブルを作成"""
    conn.execute("""
    CREATE TABLE IF NOT EXISTS weather_records (
        date TEXT PRIMARY KEY,
        temp_max REAL,
        temp_min REAL,
        temp_mean REAL,
        precipitation REAL,
        humidity_mean REAL,
        pressure_mean REAL,
        sunshine_hours REAL,
        daylight_duration REAL,
        weather_code INTEGER,
        wind_speed_max REAL,
        location_name TEXT DEFAULT 'Tokyo'
    )
    """)
    conn.execute("""
    CREATE INDEX IF NOT EXISTS idx_weather_date
    ON weather_records(date)
    """)
    conn.commit()


def fetch_from_api(lat, lon, start_date, end_date):
    """Open-Meteo APIから天気データを取得"""
    if not HAS_REQUESTS:
        print("ERROR: requests library not installed. Run: pip install requests")
        return None

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "daily": ",".join([
            "temperature_2m_max",
            "temperature_2m_min",
            "temperature_2m_mean",
            "precipitation_sum",
            "relative_humidity_2m_mean",
            "surface_pressure_mean",
            "sunshine_duration",
            "daylight_duration",
            "weather_code",
            "wind_speed_10m_max",
        ]),
        "timezone": "Asia/Tokyo",
    }

    print(f"Fetching weather data: {start_date} to {end_date}")
    print(f"Location: lat={lat}, lon={lon}")

    response = requests.get(url, params=params)
    if response.status_code != 200:
        print(f"ERROR: API returned status {response.status_code}")
        print(response.text)
        return None

    return response.json()


def fetch_recent_from_forecast_api(lat, lon, start_date, end_date):
    """
    直近のデータはarchive APIにない場合があるので
    forecast APIから取得する
    """
    if not HAS_REQUESTS:
        return None

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "daily": ",".join([
            "temperature_2m_max",
            "temperature_2m_min",
            "temperature_2m_mean",
            "precipitation_sum",
            "relative_humidity_2m_mean",
            "surface_pressure_mean",
            "sunshine_duration",
            "daylight_duration",
            "weather_code",
            "wind_speed_10m_max",
        ]),
        "timezone": "Asia/Tokyo",
        "past_days": 31,
    }

    print(f"Fetching recent weather data from forecast API...")
    response = requests.get(url, params=params)
    if response.status_code != 200:
        print(f"WARNING: Forecast API returned status {response.status_code}")
        return None

    return response.json()


def insert_weather_data(conn, data):
    """APIレスポンスをSQLiteに投入"""
    if not data or "daily" not in data:
        print("No daily data found in response")
        return 0

    daily = data["daily"]
    dates = daily.get("time", [])
    count = 0

    for i, date in enumerate(dates):
        sunshine_raw = daily.get("sunshine_duration", [None])[i]
        sunshine_hours = round(sunshine_raw / 3600, 2) if sunshine_raw else None

        daylight_raw = daily.get("daylight_duration", [None])[i]
        daylight_hours = round(daylight_raw / 3600, 2) if daylight_raw else None

        try:
            conn.execute("""
                INSERT OR REPLACE INTO weather_records
                (date, temp_max, temp_min, temp_mean, precipitation,
                 humidity_mean, pressure_mean, sunshine_hours,
                 daylight_duration, weather_code, wind_speed_max)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                date,
                daily.get("temperature_2m_max", [None])[i],
                daily.get("temperature_2m_min", [None])[i],
                daily.get("temperature_2m_mean", [None])[i],
                daily.get("precipitation_sum", [None])[i],
                daily.get("relative_humidity_2m_mean", [None])[i],
                daily.get("surface_pressure_mean", [None])[i],
                sunshine_hours,
                daylight_hours,
                daily.get("weather_code", [None])[i],
                daily.get("wind_speed_10m_max", [None])[i],
            ))
            count += 1
        except sqlite3.Error as e:
            logging.warning(f"SQLite error inserting {date}: {e}")

    conn.commit()
    return count


def main():
    parser = argparse.ArgumentParser(description="Fetch weather data → SQLite")
    parser.add_argument("--start", help="Start date (YYYY-MM-DD)",
                        default=(datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d"))
    parser.add_argument("--end", help="End date (YYYY-MM-DD)",
                        default=datetime.now().strftime("%Y-%m-%d"))
    parser.add_argument("--db", help="Path to SQLite DB", default=DEFAULT_DB)
    parser.add_argument("--lat", type=float, default=DEFAULT_LAT, help="Latitude")
    parser.add_argument("--lon", type=float, default=DEFAULT_LON, help="Longitude")
    parser.add_argument("--file", help="Load from JSON file instead of API")
    parser.add_argument("--location", default="Tokyo", help="Location name for records")
    args = parser.parse_args()

    # DB接続
    os.makedirs(os.path.dirname(args.db), exist_ok=True)
    conn = sqlite3.connect(args.db)
    init_weather_table(conn)

    total_inserted = 0

    if args.file:
        # ファイルから読み込み
        with open(args.file) as f:
            data = json.load(f)
        total_inserted = insert_weather_data(conn, data)
    else:
        # Archive APIから過去データ取得
        # Open-Meteoのarchive APIは直近5日くらいのデータがない場合があるので
        # 5日前までをarchiveから、直近をforecastから取得
        five_days_ago = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")

        if args.start < five_days_ago:
            archive_end = min(args.end, five_days_ago)
            data = fetch_from_api(args.lat, args.lon, args.start, archive_end)
            if data:
                count = insert_weather_data(conn, data)
                total_inserted += count
                print(f"  Archive: {count} days inserted")

        # Forecast API で直近データ補完
        recent_data = fetch_recent_from_forecast_api(args.lat, args.lon, five_days_ago, args.end)
        if recent_data:
            count = insert_weather_data(conn, recent_data)
            total_inserted += count
            print(f"  Recent: {count} days inserted")

    conn.close()
    print(f"\n=== Weather Import Complete ===")
    print(f"  Total days inserted/updated: {total_inserted}")
    print(f"  DB: {args.db}")


if __name__ == "__main__":
    main()
