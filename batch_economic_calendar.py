#!/usr/bin/env python3
"""
経済指標カレンダー収集システム（GitHub Actions用）
みんかぶFXから経済指標の発表予定・予想値・結果・前回変動幅を取得しS3に保存する

参考: market_indicators_collector.py と同じ構造・スタイルで実装

【みんかぶFXを使う理由】
- 指標名・国名が日本語
- 予想・結果・前回値が揃っている
- 前回発表時のドル円変動幅（pips）が取得できる

S3保存先:
  m-s3storage/economic-calendar/YYYY-MM/calendar-YYYY-MM-DD.json  # 日別アーカイブ
  m-s3storage/economic-calendar/latest.json                        # 常に最新を上書き

取得データのイメージ（1件）:
  {
    "date":          "2025-05-13",
    "time":          "21:30",
    "country":       "米国",
    "event":         "非農業部門雇用者数",
    "importance":    "high",
    "previous_pips": 45.0,
    "previous":      "228K",
    "forecast":      "243K",
    "actual":        "256K"     # 発表前はnull
  }
"""

import boto3
import json
import os
import re
from datetime import datetime, timezone
import requests
from bs4 import BeautifulSoup

# みんかぶFX 経済指標カレンダーURL
MINKABU_URL = "https://fx.minkabu.jp/indicators"

# HTTPリクエストヘッダー
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://fx.minkabu.jp/",
}


class EconomicCalendarCollector:
    def __init__(self):
        """経済指標カレンダー収集システム初期化"""
        print("経済指標カレンダー収集システム初期化中...")

        self.bucket_name = "m-s3storage"
        self.s3_prefix = "economic-calendar/"

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

    def _fetch_html(self):
        """みんかぶFXのHTMLを取得する"""
        try:
            res = requests.get(MINKABU_URL, headers=HEADERS, timeout=30)
            res.raise_for_status()
            res.encoding = 'utf-8'
            print(f"✓ HTML取得完了: status={res.status_code}, size={len(res.text):,}bytes")
            return res.text
        except requests.exceptions.HTTPError as e:
            raise RuntimeError(f"HTTPエラー: {e}")
        except requests.exceptions.Timeout:
            raise RuntimeError("タイムアウト（30秒）")
        except Exception as e:
            raise RuntimeError(f"HTML取得失敗: {e}")

    def _clean_text(self, text):
        """テキストをクリーニング（空文字の場合はNoneを返す）"""
        if not text:
            return None
        cleaned = text.strip().replace("\xa0", "").replace("\u3000", "").replace(" ", "")
        return cleaned if cleaned else None

    def _extract_pips(self, text):
        """前回変動幅テキストからpips数値を取り出す（例: '45pips' → 45.0）"""
        if not text:
            return None
        match = re.search(r'(\d+(?:\.\d+)?)\s*pips?', text, re.IGNORECASE)
        if match:
            return float(match.group(1))
        return None

    def _parse_importance(self, row_element):
        """行要素から重要度（high/medium/low）を取得する"""
        # 方法1: high/medium/low系のクラスを探す
        for level in ['high', 'medium', 'low']:
            if row_element.find(class_=re.compile(level, re.IGNORECASE)):
                return level

        # 方法2: imgのalt属性から取得
        img = row_element.find('img', alt=re.compile(r'high|medium|low', re.IGNORECASE))
        if img:
            alt = img.get('alt', '').lower()
            for level in ['high', 'medium', 'low']:
                if level in alt:
                    return level

        # 方法3: ★の数で判定（★3=high, ★2=medium, ★1=low）
        star_text = ''.join(row_element.find_all(string=re.compile(r'★')))
        count = star_text.count('★')
        if count >= 3:
            return 'high'
        elif count == 2:
            return 'medium'
        elif count == 1:
            return 'low'

        return None

    def fetch_calendar(self):
        """
        みんかぶFXをスクレイピングして経済指標リストを返す

        みんかぶの経済指標テーブル列順:
          発表時間 | 経済指標名 | 前回変動幅(USD/JPY pips) | 前回値 | 予想値 | 結果
        """
        print("経済指標カレンダー取得中...")
        html = self._fetch_html()

        soup = BeautifulSoup(html, "html.parser")
        tables = soup.find_all('table')

        if not tables:
            print("✗ テーブルが見つかりません（HTML構造変更の可能性あり）")
            return []

        events = []
        current_date = None
        current_year = datetime.now().year

        for table in tables:
            for row in table.find_all('tr'):
                row_text = row.get_text()

                # 日付行の検出（例: "05月13日" "2025年05月13日"）
                date_match = re.search(r'(\d{4})?年?(\d{1,2})月(\d{1,2})日', row_text)
                if date_match and len(row.find_all('td')) <= 2:
                    year  = int(date_match.group(1)) if date_match.group(1) else current_year
                    month = int(date_match.group(2))
                    day   = int(date_match.group(3))
                    try:
                        current_date = datetime(year, month, day).strftime('%Y-%m-%d')
                    except ValueError:
                        pass
                    continue

                # データ行の取得
                cells = row.find_all('td')
                if len(cells) < 4:
                    continue

                # 列0: 発表時刻（HH:MM形式でない行はスキップ）
                time_text = self._clean_text(cells[0].get_text())
                if not time_text or not re.match(r'\d{1,2}:\d{2}', time_text):
                    continue

                # 列1: 国名・指標名
                event_cell  = cells[1]
                country_tag = event_cell.find(class_=re.compile(r'country|nation|flag', re.IGNORECASE))
                country     = self._clean_text(country_tag.get_text()) if country_tag else None
                event_name  = self._clean_text(event_cell.get_text())
                if not event_name:
                    continue

                # 列2: 前回変動幅（pips）
                pips_text     = self._clean_text(cells[2].get_text()) if len(cells) > 2 else None
                previous_pips = self._extract_pips(pips_text)

                # 列3: 前回値
                previous = self._clean_text(cells[3].get_text()) if len(cells) > 3 else None

                # 列4: 予想値
                forecast = self._clean_text(cells[4].get_text()) if len(cells) > 4 else None

                # 列5: 結果
                actual = self._clean_text(cells[5].get_text()) if len(cells) > 5 else None

                # 重要度
                importance = self._parse_importance(row)

                events.append({
                    "date":          current_date,
                    "time":          time_text,
                    "country":       country,
                    "event":         event_name,
                    "importance":    importance,
                    "previous_pips": previous_pips,
                    "previous":      previous,
                    "forecast":      forecast,
                    "actual":        actual,
                })

        print(f"✓ パース完了: {len(events)}件")
        return events

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

    def run_collection(self):
        """データ収集実行"""
        print("=" * 60)
        print(f"経済指標カレンダー収集開始")
        print("=" * 60)

        today_str = datetime.now().strftime('%Y-%m-%d')
        ym        = today_str[:7]  # "2025-05"

        events = self.fetch_calendar()

        if not events:
            print("✗ イベントが0件のため保存をスキップします")
            return []

        # 重要度別件数
        high_count   = sum(1 for e in events if e["importance"] == "high")
        medium_count = sum(1 for e in events if e["importance"] == "medium")
        low_count    = sum(1 for e in events if e["importance"] == "low")
        result_count = sum(1 for e in events if e["actual"] is not None)

        complete_data = {
            "collected_at": datetime.now(timezone.utc).isoformat(),
            "date":         today_str,
            "source":       "みんかぶFX",
            "count":        len(events),
            "events":       events,
        }

        # 日別アーカイブ保存
        self.save_to_s3(f"{ym}/calendar-{today_str}.json", complete_data)

        # latest.json（常に上書き）
        self.save_to_s3("latest.json", complete_data)

        print("\n収集完了サマリー")
        print("=" * 60)
        print(f"合計: {len(events)}件")
        print(f"  🔴 高重要度: {high_count}件")
        print(f"  🟡 中重要度: {medium_count}件")
        print(f"  ⚪ 低重要度: {low_count}件")
        print(f"  ✅ 結果あり: {result_count}件")
        print("=" * 60)

        return events


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
            "title": f"{emoji} 経済指標カレンダー収集",
            "text":  message,
            "footer": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }]
    }

    try:
        requests.post(slack_webhook_url, json=payload)
    except Exception as e:
        print(f"Slack通知エラー: {e}")


def main():
    """メイン実行"""
    try:
        collector = EconomicCalendarCollector()
        events = collector.run_collection()

        s3_path = f"s3://{collector.bucket_name}/{collector.s3_prefix}"
        print(f"\n処理完了")
        print(f"データ保存先: {s3_path}")

        if events:
            result_count = sum(1 for e in events if e["actual"] is not None)
            notify_slack("success",
                f"経済指標カレンダー収集が完了しました\n"
                f"件数: {len(events)}件 / 結果取得済み: {result_count}件\n"
                f"データ保存先: {s3_path}"
            )
        else:
            notify_slack("failure", "経済指標カレンダーの取得に失敗しました（0件）")

    except Exception as e:
        print(f"エラーが発生しました: {e}")
        notify_slack("failure", f"エラーが発生しました: {str(e)}")


if __name__ == "__main__":
    main()
