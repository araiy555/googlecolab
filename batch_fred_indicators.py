#!/usr/bin/env python3
"""
FRED経済指標データ収集システム（GitHub Actions用）
FRED APIから主要経済指標を取得しS3に保存する

参考: market_indicators_collector.py と同じ構造・スタイルで実装

S3保存先:
  m-s3storage/fred-indicators/YYYY-MM/fred-YYYY-MM-DD.json  # 日別アーカイブ
  m-s3storage/fred-indicators/latest.json                    # 常に最新を上書き

取得指標:
  米国: FFレート, 10年債, 2年債, CPI, コアCPI, PPI,
        非農業部門雇用者数, 失業率, 平均時給, GDP成長率,
        小売売上高, 消費者信頼感
  日本: 政策金利, 10年債, CPI, GDP成長率
  為替: ドル円, ユーロドル
  リスク: VIX
"""

import boto3
import json
import os
from datetime import datetime, timezone, timedelta
import requests

# FRED API ベースURL
FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"


class FredIndicatorsCollector:
    def __init__(self):
        """FRED経済指標収集システム初期化"""
        print("FRED経済指標収集システム初期化中...")

        self.bucket_name = "m-s3storage"
        self.s3_prefix = "fred-indicators/"
        self.api_key = os.getenv("FRED_API_KEY", "91935bcccf126bf926f4a17787036841")


        # 取得対象指標: {指標名: FRED系列ID}
        self.indicators = {
            # 米国金利
            "米国FFレート":       "FEDFUNDS",
            "米国10年債利回り":   "GS10",
            "米国2年債利回り":    "GS2",
            # 米国インフレ
            "米国CPI":            "CPIAUCSL",
            "米国コアCPI":        "CPILFESL",
            "米国PPI":            "PPIACO",
            # 米国雇用
            "米国非農業部門雇用者数": "PAYEMS",
            "米国失業率":         "UNRATE",
            "米国平均時給":       "CES0500000003",
            # 米国景気
            "米国GDP成長率":      "A191RL1Q225SBEA",
            "米国小売売上高":     "RSAFS",
            "米国消費者信頼感":   "UMCSENT",
            # 日本
            "日本政策金利":       "IRSTCB01JPM156N",
            "日本10年債利回り":   "IRLTLT01JPM156N",
            "日本CPI":            "JPNCPIALLMINMEI",
            "日本GDP成長率":      "JPNRGDPEXP",
            # 為替
            "ドル円":             "DEXJPUS",
            "ユーロドル":         "DEXUSEU",
            # リスク
            "VIX":                "VIXCLS",
        }

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

    def fetch_series(self, name, series_id):
        """FRED APIから1系列のデータを取得する"""
        print(f"  {name} ({series_id}) 取得中...")
        try:
            params = {
                "series_id":        series_id,
                "api_key":          self.api_key,
                "file_type":        "json",
                "observation_start": self.start_date,
                "observation_end":   self.end_date,
                "sort_order":       "asc",
            }
            res = requests.get(FRED_BASE_URL, params=params, timeout=30)
            res.raise_for_status()

            data = res.json()
            observations = data.get("observations", [])

            # 欠損値（"."）を除外してリスト化
            records = []
            for obs in observations:
                if obs["value"] != ".":
                    records.append({
                        "date":  obs["date"],
                        "value": float(obs["value"]),
                    })

            print(f"  ✓ {name}: {len(records)}件取得")
            return records

        except requests.exceptions.HTTPError as e:
            print(f"  ✗ {name} HTTPエラー: {e}")
            return []
        except requests.exceptions.Timeout:
            print(f"  ✗ {name} タイムアウト")
            return []
        except Exception as e:
            print(f"  ✗ {name} 取得失敗: {e}")
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

    def run_collection(self):
        """データ収集実行"""
        print("=" * 60)
        print(f"FRED経済指標収集開始（{self.start_date} ～ {self.end_date}）")
        print("=" * 60)

        today_str = datetime.now().strftime('%Y-%m-%d')
        ym        = today_str[:7]

        results      = {}
        success_count = 0
        fail_count    = 0

        for name, series_id in self.indicators.items():
            records = self.fetch_series(name, series_id)
            if records:
                results[name] = records
                success_count += 1
            else:
                results[name] = []
                fail_count += 1

        # 各指標の最新値をまとめる
        latest_values = {}
        for name, records in results.items():
            if records:
                latest_values[name] = records[-1]

        complete_data = {
            "collected_at":  datetime.now(timezone.utc).isoformat(),
            "date":          today_str,
            "source":        "FRED API",
            "period":        {"start": self.start_date, "end": self.end_date},
            "latest_values": latest_values,
            "data":          results,
        }

        # 日別アーカイブ保存
        self.save_to_s3(f"{ym}/fred-{today_str}.json", complete_data)

        # latest.json（常に上書き）
        self.save_to_s3("latest.json", complete_data)

        print("\n収集完了サマリー")
        print("=" * 60)
        for name, records in results.items():
            status = f"{len(records)}件" if records else "失敗"
            print(f"  {name}: {status}")
        print(f"\n成功: {success_count}件 / 失敗: {fail_count}件")
        print("=" * 60)

        return complete_data, success_count, fail_count


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
            "title": f"{emoji} FRED経済指標収集",
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
        collector = FredIndicatorsCollector()
        complete_data, success_count, fail_count = collector.run_collection()

        s3_path = f"s3://{collector.bucket_name}/{collector.s3_prefix}"
        print(f"\n処理完了")
        print(f"データ保存先: {s3_path}")

        if fail_count == 0:
            notify_slack("success",
                f"FRED経済指標収集が完了しました\n"
                f"指標数: {success_count}件\n"
                f"データ保存先: {s3_path}"
            )
        else:
            notify_slack("failure",
                f"FRED経済指標収集が一部失敗しました\n"
                f"成功: {success_count}件 / 失敗: {fail_count}件"
            )

    except Exception as e:
        print(f"エラーが発生しました: {e}")
        notify_slack("failure", f"エラーが発生しました: {str(e)}")


if __name__ == "__main__":
    main()
