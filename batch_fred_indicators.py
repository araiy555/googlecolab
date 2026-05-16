#!/usr/bin/env python3
"""
Finnhub経済指標カレンダー収集システム（GitHub Actions用）
Finnhub APIから経済指標カレンダー（発表日時・予想・結果・前回値）を取得しS3に保存する

参考: market_indicators_collector.py と同じ構造・スタイルで実装

S3保存先:
  m-s3storage/finnhub-calendar/YYYY-MM/calendar-YYYY-MM-DD.json  # 日別アーカイブ
  m-s3storage/finnhub-calendar/latest.json                        # 常に最新を上書き
  m-s3storage/finnhub-calendar/historical/all-events.json         # 全期間データ（初回のみ）

取得データのイメージ（1件）:
  {
    "date":     "2024-11-01",
    "time":     "2024-11-01T12:30:00",
    "country":  "US",
    "event":    "Non-Farm Payrolls",
    "impact":   "high",
    "previous": 254000,
    "estimate": 220000,
    "actual":   12000
  }
"""

import boto3
import json
import os
import time
from datetime import datetime, timezone, timedelta
import requests

# Finnhub API
FINNHUB_URL = "https://finnhub.io/api/v1/calendar/economic"
FINNHUB_KEY = "d83utn1r01qkm5c9l6s0d83utn1r01qkm5c9l6sg"

# 対象国（ドル円・日本株に影響する主要国）
TARGET_COUNTRIES = {"US", "JP", "EU", "GB", "CN", "AU", "CA"}

# レートリミット対策（無料プランは60回/分）
SLEEP_BETWEEN_REQUESTS = 2


class FinnhubCalendarCollector:
    def __init__(self):
        """Finnhub経済指標カレンダー収集システム初期化"""
        print("Finnhub経済指標カレンダー収集システム初期化中...")

        self.bucket_name = "m-s3storage"
        self.s3_prefix   = "finnhub-calendar/"

        # 取得期間（過去5年分）
        self.start_date = (datetime.now() - timedelta(days=365 * 5)).strftime('%Y-%m-%d')
        self.end_date   = datetime.now().strftime('%Y-%m-%d')

        self.s3 = self._init_s3_client()
        print("初期化完了")

    def _init_s3_client(self):
        """S3クライアント初期化"""
        try:
            os.environ['AWS_ACCESS_KEY_ID']     = os.getenv('AWS_ACCESS_KEY_ID')
            os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv('AWS_SECRET_ACCESS_KEY')
            return boto3.client('s3', region_name="ap-northeast-1")
        except Exception as e:
            print(f"S3初期化失敗: {e}")
            return None

    def fetch_calendar(self, from_date, to_date):
        """Finnhub APIから指定期間の経済指標カレンダーを取得する"""
        print(f"  {from_date} ～ {to_date} 取得中...")
        try:
            params = {
                "from":  from_date,
                "to":    to_date,
                "token": FINNHUB_KEY,
            }
            res = requests.get(FINNHUB_URL, params=params, timeout=30)
            res.raise_for_status()

            events = res.json().get("economicCalendar", [])

            # 対象国フィルタ・正規化
            records = []
            for e in events:
                if e.get("country", "") not in TARGET_COUNTRIES:
                    continue
                records.append({
                    "date":     e.get("date", "")[:10],
                    "time":     e.get("time", ""),
                    "country":  e.get("country", ""),
                    "event":    e.get("event", ""),
                    "impact":   e.get("impact", ""),
                    "previous": e.get("prev"),
                    "estimate": e.get("estimate"),
                    "actual":   e.get("actual"),
                    "unit":     e.get("unit", ""),
                })

            print(f"  ✓ {from_date} ～ {to_date}: {len(records)}件取得")
            return records

        except requests.exceptions.HTTPError as e:
            print(f"  ✗ HTTPエラー: {e}")
            return []
        except requests.exceptions.Timeout:
            print(f"  ✗ タイムアウト: {from_date} ～ {to_date}")
            return []
        except Exception as e:
            print(f"  ✗ 取得失敗: {e}")
            return []

    def save_to_s3(self, filename, data):
        """S3保存"""
        if not self.s3:
            print("S3クライアントが利用できません")
            return False
        try:
            key = f"{self.s3_prefix}{filename}"
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=json.dumps(data, ensure_ascii=False, indent=2).encode('utf-8'),
                ContentType='application/json',
            )
            print(f"✓ S3保存完了: {key}")
            return True
        except Exception as e:
            print(f"✗ S3保存エラー: {e}")
            return False

    def run_collection(self, mode="daily"):
        """
        データ収集実行
        mode: "historical" → 過去5年分一括取得（初回のみ）
              "daily"      → 直近7日分取得（毎日のcron用）
        """
        print("=" * 60)
        print(f"Finnhub経済指標カレンダー収集開始（mode={mode}）")
        print("=" * 60)

        today_str = datetime.now().strftime('%Y-%m-%d')
        ym        = today_str[:7]

        all_events = []

        if mode == "historical":
            # 月単位で分割して過去5年分を取得
            current    = datetime.strptime(self.start_date, '%Y-%m-%d')
            end        = datetime.strptime(self.end_date,   '%Y-%m-%d')

            while current < end:
                from_date = current.strftime('%Y-%m-%d')

                if current.month == 12:
                    next_month = current.replace(year=current.year + 1, month=1, day=1)
                else:
                    next_month = current.replace(month=current.month + 1, day=1)

                to_date = min(next_month - timedelta(days=1), end).strftime('%Y-%m-%d')

                records = self.fetch_calendar(from_date, to_date)
                all_events.extend(records)

                current = next_month
                time.sleep(SLEEP_BETWEEN_REQUESTS)

        else:
            # 直近7日分のみ取得
            from_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
            all_events = self.fetch_calendar(from_date, today_str)

        if not all_events:
            print("✗ イベントが0件のため保存をスキップします")
            return []

        # 重要度別件数
        high_count   = sum(1 for e in all_events if e["impact"] == "high")
        medium_count = sum(1 for e in all_events if e["impact"] == "medium")
        result_count = sum(1 for e in all_events if e["actual"] is not None)

        complete_data = {
            "collected_at": datetime.now(timezone.utc).isoformat(),
            "date":         today_str,
            "mode":         mode,
            "source":       "Finnhub",
            "count":        len(all_events),
            "events":       all_events,
        }

        # 日別アーカイブ保存
        self.save_to_s3(f"{ym}/calendar-{today_str}.json", complete_data)

        # latest.json（常に上書き）
        self.save_to_s3("latest.json", complete_data)

        # 過去5年分一括取得の場合はhistoricalにも保存
        if mode == "historical":
            self.save_to_s3("historical/all-events.json", complete_data)

        print("\n収集完了サマリー")
        print("=" * 60)
        print(f"  合計:     {len(all_events)}件")
        print(f"  高重要度: {high_count}件")
        print(f"  中重要度: {medium_count}件")
        print(f"  結果あり: {result_count}件")
        print("=" * 60)

        return all_events


def notify_slack(status, message):
    """Slackに通知を送る"""
    slack_webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not slack_webhook_url:
        print("警告: SLACK_WEBHOOK_URLが設定されていません")
        return

    color = "good" if status == "success" else "danger"
    emoji = "✅" if status == "success" else "❌"

    payload = {
        "attachments": [{
            "color": color,
            "title": f"{emoji} Finnhub経済指標カレンダー収集",
            "text":  message,
            "footer": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }]
    }

    try:
        requests.post(slack_webhook_url, json=payload)
    except Exception as e:
        print(f"Slack通知エラー: {e}")


def main():
    """
    メイン実行
    初回: python batch_finnhub_calendar.py historical
    毎日: python batch_finnhub_calendar.py
    """
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else "daily"

    try:
        collector = FinnhubCalendarCollector()
        events    = collector.run_collection(mode=mode)

        s3_path = f"s3://{collector.bucket_name}/{collector.s3_prefix}"
        print(f"\n処理完了")
        print(f"データ保存先: {s3_path}")

        if events:
            notify_slack("success",
                f"Finnhub経済指標カレンダー収集が完了しました\n"
                f"モード: {mode} / 件数: {len(events)}件\n"
                f"データ保存先: {s3_path}"
            )
        else:
            notify_slack("failure", "Finnhub経済指標カレンダーの取得に失敗しました（0件）")

    except Exception as e:
        print(f"エラーが発生しました: {e}")
        notify_slack("failure", f"エラーが発生しました: {str(e)}")


if __name__ == "__main__":
    main()
