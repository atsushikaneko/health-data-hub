#!/usr/bin/env python3
"""
Apple Health XML → SQLite Parser

Usage:
    python xml_to_sqlite.py /path/to/export.xml [--db /path/to/health.db]

Features:
- iterparseで380MB+のXMLもメモリ効率的に処理
- INSERT OR IGNOREで冪等（何度実行しても重複しない）
- 睡眠セッション復元（2h以内のギャップを結合）
- バッチINSERTで高速化
"""

import xml.etree.ElementTree as ET
import sqlite3
import argparse
import logging
import os
import sys
from datetime import datetime, timedelta
from schema import get_connection, init_db

BATCH_SIZE = 5000


def parse_datetime(s):
    """Apple Health形式の日時文字列をパース"""
    if not s:
        return None
    try:
        return datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return None


def parse_float(s):
    """安全なfloat変換"""
    if not s:
        return None
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def sleep_date_for(dt):
    """就寝時刻から睡眠日を算出（12時以前は前日扱い）"""
    if dt.hour < 12:
        return (dt - timedelta(days=1)).strftime("%Y-%m-%d")
    return dt.strftime("%Y-%m-%d")


def import_xml(xml_path, db_path=None):
    """メインのインポート処理"""
    if not os.path.exists(xml_path):
        print(f"ERROR: File not found: {xml_path}")
        sys.exit(1)

    print(f"Parsing: {xml_path}")
    print(f"DB: {db_path or 'default'}")

    # DB初期化
    init_db(db_path)
    conn = get_connection(db_path)
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA temp_store=MEMORY")
    c = conn.cursor()

    # カウンター
    counts = {
        "records_total": 0,
        "records_inserted": 0,
        "sleep_segments": 0,
        "workouts": 0,
        "ecg": 0,
        "activity_summaries": 0,
    }

    # バッチバッファ
    record_batch = []
    sleep_raw_segments = []
    sleep_stage_batch = []

    # iterparseでストリーミング処理
    context = ET.iterparse(xml_path, events=("end",))

    for event, elem in context:
        # ===== Record =====
        if elem.tag == "Record":
            counts["records_total"] += 1

            rtype = elem.get("type", "")
            value = parse_float(elem.get("value"))
            unit = elem.get("unit")
            start = elem.get("startDate", "")
            end = elem.get("endDate", "")
            source = elem.get("sourceName", "")
            source_ver = elem.get("sourceVersion", "")
            device = elem.get("device", "")
            creation = elem.get("creationDate", "")

            # --- 睡眠データは特別処理 ---
            if rtype == "HKCategoryTypeIdentifierSleepAnalysis":
                val_str = elem.get("value", "")
                start_dt = parse_datetime(start)
                end_dt = parse_datetime(end)

                if start_dt and end_dt:
                    dur_min = (end_dt - start_dt).total_seconds() / 60
                    sd = sleep_date_for(start_dt)

                    if "Asleep" in val_str or "InBed" in val_str:
                        sleep_raw_segments.append((start_dt, end_dt))

                    # ステージ
                    stage = None
                    if "Deep" in val_str:
                        stage = "deep"
                    elif "REM" in val_str:
                        stage = "rem"
                    elif "Core" in val_str:
                        stage = "core"
                    elif "Awake" in val_str:
                        stage = "awake"

                    if stage:
                        sleep_stage_batch.append((
                            sd, stage, start[:19], end[:19], dur_min
                        ))

                # カテゴリ値を数値化（InBed=0, Asleep系=1）
                if "Asleep" in (elem.get("value", "") or ""):
                    value = 1
                elif "InBed" in (elem.get("value", "") or ""):
                    value = 0

            # --- ECGデータ ---
            elif rtype == "HKQuantityTypeIdentifierElectrodermalActivity" or \
                 "Electrocardiogram" in rtype:
                pass  # ECGは別処理（下のInstantaneousBeatsPerMinuteで拾うか、別途）

            # 汎用レコード投入
            record_batch.append((
                rtype, value, unit, start[:19] if start else "",
                end[:19] if end else "", source, source_ver, device,
                creation[:19] if creation else ""
            ))

            if len(record_batch) >= BATCH_SIZE:
                inserted = _flush_records(c, record_batch)
                counts["records_inserted"] += inserted
                record_batch.clear()

            if counts["records_total"] % 100000 == 0:
                print(f"  ... {counts['records_total']:,} records processed")

        # ===== Workout =====
        elif elem.tag == "Workout":
            wtype = elem.get("workoutActivityType", "")
            start = elem.get("startDate", "")
            end = elem.get("endDate", "")
            dur = parse_float(elem.get("duration"))
            energy = parse_float(elem.get("totalEnergyBurned"))
            dist = parse_float(elem.get("totalDistance"))
            source = elem.get("sourceName", "")

            try:
                c.execute("""
                    INSERT OR IGNORE INTO workouts
                    (workout_type, start_date, end_date, duration_minutes,
                     total_energy_burned, total_distance, source_name)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (wtype, start[:19], end[:19], dur, energy, dist, source))
                counts["workouts"] += 1
            except sqlite3.Error as e:
                logging.warning(f"SQLite error: {e}")

        # ===== ActivitySummary =====
        elif elem.tag == "ActivitySummary":
            date_comp = elem.get("dateComponents", "")
            if date_comp:
                try:
                    c.execute("""
                        INSERT OR IGNORE INTO activity_summaries
                        (date, active_energy_burned, active_energy_burned_goal,
                         exercise_time, exercise_time_goal,
                         stand_hours, stand_hours_goal)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                        date_comp,
                        parse_float(elem.get("activeEnergyBurned")),
                        parse_float(elem.get("activeEnergyBurnedGoal")),
                        parse_float(elem.get("appleExerciseTime")),
                        parse_float(elem.get("appleExerciseTimeGoal")),
                        parse_float(elem.get("appleStandHours")),
                        parse_float(elem.get("appleStandHoursGoal")),
                    ))
                    counts["activity_summaries"] += 1
                except sqlite3.Error as e:
                    logging.warning(f"SQLite error: {e}")

        elem.clear()

    # 残りのバッチをフラッシュ
    if record_batch:
        inserted = _flush_records(c, record_batch)
        counts["records_inserted"] += inserted

    # 睡眠ステージ投入
    if sleep_stage_batch:
        _flush_sleep_stages(c, sleep_stage_batch)

    # ===== 睡眠セッション復元 =====
    print("Reconstructing sleep sessions...")
    _reconstruct_sleep_sessions(c, sleep_raw_segments)

    # インポートログ
    c.execute("""
        INSERT INTO import_log (imported_at, filename, records_total, records_inserted, max_date)
        VALUES (?, ?, ?, ?, ?)
    """, (
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        os.path.basename(xml_path),
        counts["records_total"],
        counts["records_inserted"],
        c.execute("SELECT MAX(start_date) FROM health_records").fetchone()[0]
    ))

    conn.commit()
    conn.close()

    print(f"\n=== Import Complete ===")
    print(f"  Total records parsed: {counts['records_total']:,}")
    print(f"  Records inserted: {counts['records_inserted']:,}")
    print(f"  Workouts: {counts['workouts']}")
    print(f"  Activity summaries: {counts['activity_summaries']}")
    print(f"  Sleep segments: {len(sleep_raw_segments)}")


def _flush_records(cursor, batch):
    """バッチでレコードを投入"""
    cursor.executemany("""
        INSERT OR IGNORE INTO health_records
        (type, value, unit, start_date, end_date,
         source_name, source_version, device, creation_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, batch)
    return cursor.rowcount


def _flush_sleep_stages(cursor, batch):
    """睡眠ステージをバッチ投入"""
    cursor.executemany("""
        INSERT OR IGNORE INTO sleep_stages
        (sleep_date, stage, start_date, end_date, duration_minutes)
        VALUES (?, ?, ?, ?, ?)
    """, batch)


def _reconstruct_sleep_sessions(cursor, raw_segments):
    """
    生の睡眠セグメントからセッションを復元
    2時間以内のギャップは同一セッションとして結合
    """
    if not raw_segments:
        return

    raw_segments.sort(key=lambda x: x[0])
    sessions = []
    current = None

    for start, end in raw_segments:
        if current is None:
            current = {"start": start, "end": end}
        elif (start - current["end"]).total_seconds() < 7200:  # 2h gap
            current["end"] = max(current["end"], end)
        else:
            sessions.append(current)
            current = {"start": start, "end": end}

    if current:
        sessions.append(current)

    # セッションをDBに投入
    for s in sessions:
        dur = (s["end"] - s["start"]).total_seconds() / 3600
        if 3 <= dur <= 14:
            bt = s["start"]
            sd = sleep_date_for(bt)

            bt_hour = bt.hour + bt.minute / 60
            if bt_hour < 12:
                bt_hour += 24

            wt_hour = s["end"].hour + s["end"].minute / 60

            try:
                cursor.execute("""
                    INSERT OR IGNORE INTO sleep_sessions
                    (sleep_date, bedtime, waketime, duration_hours,
                     bedtime_hour, waketime_hour)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    sd,
                    bt.strftime("%Y-%m-%d %H:%M:%S"),
                    s["end"].strftime("%Y-%m-%d %H:%M:%S"),
                    round(dur, 2),
                    round(bt_hour, 2),
                    round(wt_hour, 2),
                ))
            except sqlite3.Error as e:
                logging.warning(f"SQLite error: {e}")

    print(f"  Sleep sessions reconstructed: {len(sessions)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Apple Health XML → SQLite")
    parser.add_argument("xml_path", help="Path to export.xml")
    parser.add_argument("--db", help="Path to SQLite DB", default=None)
    args = parser.parse_args()
    import_xml(args.xml_path, args.db)
