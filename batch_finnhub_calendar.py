#!/usr/bin/env python3
"""
Finnhub経済指標カレンダー収集システム（GitHub Actions用）
Finnhub APIから経済指標カレンダー（発表日時・予想・結果・前回値）を取得しS3に保存する

参考: market_indicators_collector.py と同じ構造・スタイルで実装

S3保存先:
  m-s3storage/finnhub-calendar/YYYY-MM/calendar-YYYY-MM-DD.json  # 日別アーカイブ
  m-s3storage/finnhub-calendar/latest.json                        # 常に最新を上書き
  m-s3storage/finnhub-calendar/historical/all-events.json         # 全期間累積データ

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
import sys
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

    def _load_existing_events(self):
        """S3のhistorical/all-events.jsonから既存データを読み込む"""
        try:
            key = f"{self.s3_prefix}historical/all-events.json"
            res = self.s3.get_object(Bucket=self.bucket_name, Key=key)
            data = json.loads(res['Body'].read().decode('utf-8'))
            events = data.get("events", [])
            print(f"✓ 既存データ読み込み完了: {len(events)}件")
            return events
        except Exception:
            print("既存データなし（初回実行）")
            return []

    def _make_event_key(self, event):
        """イベントの重複チェック用キーを生成する（time + country + event名）"""
        return f"{event['time']}_{event['country']}_{event['event']}"

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

            # 対象国フィルタ・high impactのみ・正規化
            records = []
            for e in events:
                if e.get("country", "") not in TARGET_COUNTRIES:
                    continue
                if e.get("impact", "") != "high":
                    continue

                # dateが空の場合はtimeから補完
                date_str = e.get("date", "")[:10]
                if not date_str:
                    time_str = e.get("time", "")
                    date_str = time_str[:10] if time_str else ""

                records.append({
                    "date":     date_str,
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
              "daily"      → 直近7日分取得し重複なしで累積追記
        """
        print("=" * 60)
        print(f"Finnhub経済指標カレンダー収集開始（mode={mode}）")
        print("=" * 60)

        today_str = datetime.now().strftime('%Y-%m-%d')
        ym        = today_str[:7]

        new_events = []

        if mode == "historical":
            # 月単位で分割して過去5年分を取得
            current = datetime.strptime(self.start_date, '%Y-%m-%d')
            end     = datetime.strptime(self.end_date,   '%Y-%m-%d')

            while current < end:
                from_date = current.strftime('%Y-%m-%d')

                if current.month == 12:
                    next_month = current.replace(year=current.year + 1, month=1, day=1)
                else:
                    next_month = current.replace(month=current.month + 1, day=1)

                to_date = min(next_month - timedelta(days=1), end).strftime('%Y-%m-%d')

                records = self.fetch_calendar(from_date, to_date)
                new_events.extend(records)

                current = next_month
                time.sleep(SLEEP_BETWEEN_REQUESTS)

        else:
            # 直近7日分を取得
            from_date  = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
            new_events = self.fetch_calendar(from_date, today_str)

        if not new_events:
            print("✗ 新規イベントが0件のため保存をスキップします")
            return []

        # 既存データを読み込んで重複チェック
        existing_events = self._load_existing_events()
        existing_keys   = {self._make_event_key(e) for e in existing_events}

        # 新規のみ追加
        added_events = [e for e in new_events if self._make_event_key(e) not in existing_keys]
        all_events   = existing_events + added_events

        # 日付順にソート
        all_events.sort(key=lambda x: x.get("time", ""))

        print(f"\n新規追加: {len(added_events)}件 / 累積合計: {len(all_events)}件")

        result_count = sum(1 for e in all_events if e["actual"] is not None)

        complete_data = {
            "collected_at": datetime.now(timezone.utc).isoformat(),
            "date":         today_str,
            "mode":         mode,
            "source":       "Finnhub",
            "count":        len(all_events),
            "events":       all_events,
        }

        # 日別アーカイブ保存（その日取得した新規分のみ）
        daily_data = {
            "collected_at": datetime.now(timezone.utc).isoformat(),
            "date":         today_str,
            "mode":         mode,
            "source":       "Finnhub",
            "count":        len(added_events),
            "events":       added_events,
        }
        self.save_to_s3(f"{ym}/calendar-{today_str}.json", daily_data)

        # latest.json（直近7日分）
        self.save_to_s3("latest.json", daily_data)

        # historical/all-events.json（全期間累積）
        self.save_to_s3("historical/all-events.json", complete_data)

        print("\n収集完了サマリー")
        print("=" * 60)
        print(f"  新規追加:   {len(added_events)}件")
        print(f"  累積合計:   {len(all_events)}件")
        print(f"  結果あり:   {result_count}件")
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
                f"モード: {mode} / 累積: {len(events)}件\n"
                f"データ保存先: {s3_path}"
            )
        else:
            notify_slack("failure", "Finnhub経済指標カレンダーの取得に失敗しました（0件）")

    except Exception as e:
        print(f"エラーが発生しました: {e}")
        notify_slack("failure", f"エラーが発生しました: {str(e)}")


if __name__ == "__main__":
    main()
