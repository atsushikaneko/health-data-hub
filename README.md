# Health Data Hub

Apple Health等のヘルスデータをSQLiteに集約し、Claude DesktopからMCP経由でクエリできるようにするツール。

## 構成

```
health-data-hub/
├── parser/
│   ├── schema.py           # SQLiteスキーマ定義
│   ├── xml_to_sqlite.py    # XMLパーサー（冪等・差分対応）
│   └── requirements.txt
├── tmp/                    # エクスポートXML置き場（.gitignore、中身はコミットしない）
├── db/
│   └── health.db           # SQLite（.gitignore対象）
├── mcp_server/
│   ├── server.py           # MCP Server
│   └── requirements.txt
└── .gitignore
```

## セットアップ

### 前提条件

- Python 3.10+
- pip

```bash
pip install -r requirements.txt
```

### 1. データ投入

iPhoneの「ヘルスケア」→ プロフィール → 「すべてのヘルスケアデータを書き出す」→ MacにAirDrop

```bash
cp ~/Downloads/apple_health_export/export.xml ./tmp/export.xml
python3 parser/xml_to_sqlite.py tmp/export.xml
```

### 2. 天気データ投入（オプション）

Open-Meteo APIから天気データを取得してDBに格納します（無料・登録不要）。

```bash
python3 parser/fetch_weather.py
```

期間や地域を指定する場合：

```bash
python3 parser/fetch_weather.py --start 2025-07-01 --end 2026-03-16 --lat 35.6762 --lon 139.6503 --location Tokyo
```

### 3. Claude Desktop連携

`claude_desktop_config.json` に追加:

```json
{
  "mcpServers": {
    "health-data-hub": {
      "command": "python3",
      "args": ["/absolute/path/to/health-data-hub/mcp_server/server.py"],
      "env": {
        "HEALTH_DB_PATH": "/absolute/path/to/health-data-hub/db/health.db"
      }
    }
  }
}
```

Claude Desktopを再起動すれば使えます。

## 使い方（Claude Desktopから）

### iPhone単体でもできること
- 「今週の歩数の推移を見せて」
- 「先月と今月の歩行速度を比較して」
- 「直近1週間で一番歩いた日は？」

### Apple Watchがあるとできること
- 「昨日の睡眠どうだった？」
- 「2月と3月の日光を浴びた時間を比較して」
- 「朝散歩始める前と後で活動量を比較して」

### クロス分析（複数データソースの組み合わせ）
- 「手首温度と中途覚醒時間に相関はある？」（Apple Watch × Apple Watch）
- 「気圧が低い日は歩数が減ってる？」（Open-Meteo × Apple Health）
- 「雨の日と晴れの日で睡眠の質に違いはある？」（Open-Meteo × Apple Watch）

## テーブル構成

| テーブル           | 内容                                     |
| ------------------ | ---------------------------------------- |
| health_records     | 全レコード（歩数、心拍、SpO2等）89万件+  |
| sleep_sessions     | 復元された睡眠セッション                 |
| sleep_stages       | 睡眠ステージ（deep/rem/core/awake）      |
| workouts           | ワークアウト記録                         |
| activity_summaries | 日次アクティビティリング                 |
| ecg_readings       | ECG波形データ                            |
| import_log         | インポート履歴                           |
| weather_records    | 天気データ（気温/降水量/気圧/日照/湿度） |

## MCPツール一覧

| ツール                 | 用途                             |
| ---------------------- | -------------------------------- |
| query_health           | 生SQL実行                        |
| get_daily_summary      | 指定日のサマリー                 |
| get_sleep              | 睡眠データ取得                   |
| get_activity_trend     | 活動量トレンド（日次/週次/月次） |
| get_record_types       | レコードタイプ一覧               |
| compare_periods        | 2期間比較（before/after分析）    |
| get_weather            | 天気データ取得                   |
| get_health_and_weather | 健康×天気の統合データ            |

## 設計思想

- **冪等性**: UNIQUE制約 + INSERT OR IGNOREで何度フルインポートしても重複しない
- **差分対応**: 毎回フルエクスポートを投入してOK（新しいレコードだけ追加される）
- **メモリ効率**: iterparseで380MB+のXMLをストリーミング処理
- **プライバシー**: データは完全にローカル。外部サーバーに送信しない
