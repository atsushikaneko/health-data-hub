#!/usr/bin/env python3
"""健診結果をDBに投入するスクリプト"""
import os
import sys

sys.path.append(os.path.dirname(__file__))
from schema import get_connection, init_db


def import_checkup(db_path=None):
    init_db(db_path)
    conn = get_connection(db_path)
    c = conn.cursor()

    # === 2026/4/16 協会けんぽ健診 ===
    data_kenpo = [
        ("2026-04-16", "協会けんぽ", "身長", 179.1, None, "cm", None, None, None),
        ("2026-04-16", "協会けんぽ", "体重", 73.8, None, "kg", None, None, None),
        ("2026-04-16", "協会けんぽ", "BMI", 23.0, None, None, None, None, None),
        ("2026-04-16", "協会けんぽ", "腹囲", 78.0, None, "cm", None, None, None),
        ("2026-04-16", "協会けんぽ", "視力_右", 0.8, None, None, None, None, None),
        ("2026-04-16", "協会けんぽ", "視力_左", 0.3, None, None, None, None, None),
        ("2026-04-16", "協会けんぽ", "血圧_最高", 124.0, None, "mmHg", None, 129.0, "A"),
        ("2026-04-16", "協会けんぽ", "血圧_最低", 68.0, None, "mmHg", None, 84.0, "A"),
        ("2026-04-16", "協会けんぽ", "AST_GOT", 22.0, None, "U/L", None, 30.0, "A"),
        ("2026-04-16", "協会けんぽ", "ALT_GPT", 26.0, None, "U/L", None, 30.0, "A"),
        ("2026-04-16", "協会けんぽ", "γ-GTP", 16.0, None, "U/L", None, 50.0, "A"),
        ("2026-04-16", "協会けんぽ", "ALP_IFCC", 53.0, None, "U/L", 38.0, 113.0, "A"),
        ("2026-04-16", "協会けんぽ", "総コレステロール", 200.0, None, "mg/dL", 130.0, 219.0, "A"),
        ("2026-04-16", "協会けんぽ", "中性脂肪", 134.0, None, "mg/dL", 30.0, 149.0, "A"),
        ("2026-04-16", "協会けんぽ", "HDL", 46.0, None, "mg/dL", 40.0, None, "A"),
        ("2026-04-16", "協会けんぽ", "LDL", 126.0, None, "mg/dL", 60.0, 119.0, "B"),
        ("2026-04-16", "協会けんぽ", "白血球数", 4.5, None, "×10³/μL", 3.1, 8.4, "A"),
        ("2026-04-16", "協会けんぽ", "赤血球数", 504.0, None, "×10⁴/μL", 400.0, 539.0, "A"),
        ("2026-04-16", "協会けんぽ", "ヘモグロビン", 15.9, None, "g/dL", 13.1, 16.3, "A"),
        ("2026-04-16", "協会けんぽ", "ヘマトクリット", 48.2, None, "%", 38.5, 48.9, "A"),
        ("2026-04-16", "協会けんぽ", "空腹時血糖", 89.0, None, "mg/dL", 70.0, 99.0, "A"),
        ("2026-04-16", "協会けんぽ", "尿糖", None, "(-)", None, None, None, "A"),
        ("2026-04-16", "協会けんぽ", "尿蛋白", None, "(-)", None, None, None, "A"),
        ("2026-04-16", "協会けんぽ", "尿潜血", None, "(-)", None, None, None, "A"),
        ("2026-04-16", "協会けんぽ", "尿酸", 5.1, None, "mg/dL", 2.1, 7.0, "A"),
        ("2026-04-16", "協会けんぽ", "クレアチニン", 1.21, None, "mg/dL", None, 1.00, "C"),
        ("2026-04-16", "協会けんぽ", "eGFR", 55.9, None, "mL/min/1.73m²", 60.0, None, "C"),
        ("2026-04-16", "協会けんぽ", "便潜血1", None, "(-)", None, None, None, "A"),
        ("2026-04-16", "協会けんぽ", "便潜血2", None, "(-)", None, None, None, "A"),
        ("2026-04-16", "協会けんぽ", "胸部X線", None, "異常なし", None, None, None, "A"),
        ("2026-04-16", "協会けんぽ", "胃部内視鏡", None, "表層性胃炎", None, None, None, "B"),
        ("2026-04-16", "協会けんぽ", "腹部超音波", None, "肝のう胞7mm単数", None, None, None, "B"),
        ("2026-04-16", "協会けんぽ", "ピロリ測定値", 2.0, None, None, None, None, None),
        ("2026-04-16", "協会けんぽ", "聴力_1000Hz", None, "正常", None, None, None, "A"),
        ("2026-04-16", "協会けんぽ", "聴力_4000Hz", None, "正常", None, None, None, "A"),
    ]

    # === 2026/4/24 北習志野えんどう内科（糖尿病内科） ===
    data_endo = [
        ("2026-04-24", "北習志野えんどう内科", "総蛋白", 7.1, None, "g/dL", 6.5, 8.3, None),
        ("2026-04-24", "北習志野えんどう内科", "アルブミン", 4.3, None, "g/dL", 3.8, 5.3, None),
        ("2026-04-24", "北習志野えんどう内科", "A/G比", 1.5, None, None, 1.1, 2.3, None),
        ("2026-04-24", "北習志野えんどう内科", "LDL", 146.0, None, "mg/dL", 70.0, 139.0, None),
        ("2026-04-24", "北習志野えんどう内科", "HDL", 51.0, None, "mg/dL", 40.0, 77.0, None),
        ("2026-04-24", "北習志野えんどう内科", "中性脂肪", 82.0, None, "mg/dL", 30.0, 149.0, None),
        ("2026-04-24", "北習志野えんどう内科", "AST_GOT", 25.0, None, "U/L", 8.0, 38.0, None),
        ("2026-04-24", "北習志野えんどう内科", "ALT_GPT", 36.0, None, "U/L", 4.0, 43.0, None),
        ("2026-04-24", "北習志野えんどう内科", "γ-GTP", 17.0, None, "U/L", None, 86.0, None),
        ("2026-04-24", "北習志野えんどう内科", "グルコース", 95.0, None, "mg/dL", 60.0, 109.0, None),
        ("2026-04-24", "北習志野えんどう内科", "HbA1c", 5.4, None, "%", 4.6, 6.2, None),
        ("2026-04-24", "北習志野えんどう内科", "尿素窒素", 15.2, None, "mg/dL", 8.0, 20.0, None),
        ("2026-04-24", "北習志野えんどう内科", "クレアチニン", 1.08, None, "mg/dL", 0.61, 1.04, None),
        ("2026-04-24", "北習志野えんどう内科", "eGFR", 63.8, None, "mL/min/1.73m²", 60.0, None, None),
        ("2026-04-24", "北習志野えんどう内科", "尿酸", 5.4, None, "mg/dL", 3.6, 7.0, None),
        ("2026-04-24", "北習志野えんどう内科", "白血球数", 4.3, None, "×10³/μL", 3.9, 9.8, None),
        ("2026-04-24", "北習志野えんどう内科", "赤血球数", 498.0, None, "×10⁴/μL", 427.0, 570.0, None),
        ("2026-04-24", "北習志野えんどう内科", "ヘモグロビン", 15.6, None, "g/dL", 13.5, 17.6, None),
        ("2026-04-24", "北習志野えんどう内科", "ヘマトクリット", 47.3, None, "%", 39.8, 51.8, None),
        ("2026-04-24", "北習志野えんどう内科", "血小板数", 17.8, None, "×10⁴/μL", 13.1, 36.2, None),
    ]

    all_data = data_kenpo + data_endo

    before = c.execute("SELECT COUNT(*) FROM checkup_results").fetchone()[0]
    c.executemany("""
        INSERT OR IGNORE INTO checkup_results
        (date, source, item_name, value, value_text, unit, reference_min, reference_max, grade)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, all_data)
    conn.commit()
    after = c.execute("SELECT COUNT(*) FROM checkup_results").fetchone()[0]
    print(f"Imported {len(all_data)} checkup records ({after - before} new, {after} total)")
    conn.close()


if __name__ == "__main__":
    db_path = os.environ.get("HEALTH_DB_PATH")
    import_checkup(db_path)
