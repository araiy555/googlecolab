#!/usr/bin/env python3
"""
FX OHLCVデータ収集システム（GitHub Actions用）
ドル円・ユーロ円・ユーロドルの全時間軸OHLCVを取得しS3に保存する

参考: market_indicators_collector.py と同じ構造・スタイルで実装

S3保存先:
  m-s3storage/fx-data/usdjpy/1min/usdjpy-1min-YYYY-MM-DD.json
  m-s3storage/fx-data/usdjpy/5min/usdjpy-5min-YYYY-MM-DD.json
  m-s3storage/fx-data/usdjpy/15min/usdjpy-15min-YYYY-MM-DD.json
  m-s3storage/fx-data/usdjpy/1hour/usdjpy-1hour-YYYY-MM-DD.json
  m-s3storage/fx-data/usdjpy/daily/usdjpy-daily-YYYY-MM-DD.json
  ※ eurjpy / eurusd も同様の構造

yfinance取得制約:
  1m  → 直近7日分のみ
  5m  → 直近60日分のみ
  15m → 直近60日分のみ
  1h  → 直近730日分のみ
  1d  → 制限なし（5年分取得）
"""

import boto3
import json
import os
import time
from datetime import datetime, timedelta, timezone
import yfinance as yf
import requests

# リクエスト間のスリープ秒数（レートリミット対策）
SLEEP_BETWEEN_REQUESTS = 5   # 時間軸ごとの待機
SLEEP_BETWEEN_PAIRS    = 10  # ペアごとの待機
MAX_RETRY              = 3   # 失敗時のリトライ回数
RETRY_WAIT             = 30  # リトライ前の待機秒数


class FxOhlcvCollector:
    def __init__(self):
        """FX OHLCVデータ収集システム初期化"""
        print("FX OHLCVデータ収集システム初期化中...")

        self.bucket_name = "m-s3storage"
        self.s3_prefix = "fx-data/"

        # 取得対象FXペア: {フォルダ名: yfinanceティッカー}
        self.fx_pairs = {
            "usdjpy": "USDJPY=X",
            "eurjpy": "EURJPY=X",
            "eurusd": "EURUSD=X",
        }

        # 時間軸設定
        self.timeframes = [
            {"name": "1min",  "interval": "1m",  "period": "7d"},
            {"name": "5min",  "interval": "5m",  "period": "60d"},
            {"name": "15min", "interval": "15m", "period": "60d"},
            {"name": "1hour", "interval": "1h",  "period": "730d"},
            {"name": "daily", "interval": "1d",  "period": "5y"},
        ]

        # 時間軸ごとのS3保持日数
        self.retention_days = {
            "1min":  10,
            "5min":  90,
            "15min": 90,
            "1hour": 400,
            "daily": 9999,  # 日足は削除しない
        }

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

    def _fetch_ohlcv(self, ticker_symbol, interval, period):
        """
        yfinanceでOHLCVデータを取得してリストで返す
        レートリミット時はMAX_RETRYまでリトライする
        """
        for attempt in range(1, MAX_RETRY + 1):
            try:
                ticker = yf.Ticker(ticker_symbol)
                df = ticker.history(period=period, interval=interval)

                if df.empty:
                    print(f"  ⚠️  データなし: {ticker_symbol} {interval}")
                    return []

                records = []
                for ts, row in df.iterrows():
                    def safe_round(val, digits=4):
                        return round(float(val), digits) if val == val else None

                    records.append({
                        "datetime": ts.isoformat(),
                        "open":     safe_round(row["Open"]),
                        "high":     safe_round(row["High"]),
                        "low":      safe_round(row["Low"]),
                        "close":    safe_round(row["Close"]),
                        "volume":   int(row["Volume"]) if row["Volume"] == row["Volume"] else 0,
                    })

                print(f"  ✓ 取得完了: {len(records)}件 ({interval})")
                return records

            except Exception as e:
                err_str = str(e)
                if "Too Many Requests" in err_str or "Rate limit" in err_str.lower():
                    if attempt < MAX_RETRY:
                        print(f"  ⚠️  レートリミット（{attempt}回目）: {RETRY_WAIT}秒待機してリトライ...")
                        time.sleep(RETRY_WAIT)
                        continue
                    else:
                        print(f"  ✗ レートリミット: リトライ上限（{MAX_RETRY}回）に達しました")
                        return []
                else:
                    print(f"  ✗ 取得失敗 {ticker_symbol} {interval}: {e}")
                    return []

        return []

    def fetch_pair(self, pair_name, ticker_symbol):
        """1FXペアの全時間軸データを取得する"""
        print(f"\n{pair_name.upper()} ({ticker_symbol}) データ取得中...")
        results = {}

        for idx, tf in enumerate(self.timeframes):
            # 時間軸間のスリープ（初回はスキップ）
            if idx > 0:
                time.sleep(SLEEP_BETWEEN_REQUESTS)

            tf_name = tf["name"]
            print(f"  ⏱️  {tf_name}")
            records = self._fetch_ohlcv(ticker_symbol, tf["interval"], tf["period"])
            results[tf_name] = records

        return results

    def save_to_s3(self, pair_name, tf_name, records, today_str):
        """OHLCVデータをS3にJSON形式で保存する"""
        if not self.s3:
            print("S3クライアントが利用できません")
            return False

        key = f"{self.s3_prefix}{pair_name}/{tf_name}/{pair_name}-{tf_name}-{today_str}.json"

        payload = {
            "pair":         pair_name.upper(),
            "timeframe":    tf_name,
            "date":         today_str,
            "collected_at": datetime.now(timezone.utc).isoformat(),
            "count":        len(records),
            "ohlcv":        records,
        }

        try:
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=json.dumps(payload, ensure_ascii=False, indent=2).encode('utf-8'),
                ContentType='application/json',
            )
            print(f"  ✓ S3保存完了: {key}")
            return True
        except Exception as e:
            print(f"  ✗ S3保存エラー: {e}")
            return False

    def cleanup_old_files(self, pair_name, tf_name, retention_days):
        """指定日数より古いS3ファイルを削除する"""
        if retention_days >= 9999:
            return 0

        prefix = f"{self.s3_prefix}{pair_name}/{tf_name}/"
        cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)

        try:
            res = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
            if "Contents" not in res:
                return 0

            deleted = 0
            for obj in res["Contents"]:
                if obj["LastModified"] < cutoff:
                    self.s3.delete_object(Bucket=self.bucket_name, Key=obj["Key"])
                    deleted += 1

            if deleted > 0:
                print(f"  🗑️  古いファイル削除: {deleted}件 ({prefix})")
            return deleted

        except Exception as e:
            print(f"  ⚠️  削除処理失敗: {e}")
            return 0

    def run_collection(self):
        """データ収集実行"""
        print("=" * 60)
        print(f"FX OHLCVデータ収集開始")
        print("=" * 60)

        today_str = datetime.now().strftime('%Y-%m-%d')
        success_count = 0
        fail_count = 0

        for pair_idx, (pair_name, ticker_symbol) in enumerate(self.fx_pairs.items()):
            # ペア間のスリープ（初回はスキップ）
            if pair_idx > 0:
                print(f"\n  ⏳ 次のペアまで{SLEEP_BETWEEN_PAIRS}秒待機...")
                time.sleep(SLEEP_BETWEEN_PAIRS)

            # 全時間軸データ取得
            results = self.fetch_pair(pair_name, ticker_symbol)

            # S3保存 & 古いファイル削除
            for tf_name, records in results.items():
                if records:
                    ok = self.save_to_s3(pair_name, tf_name, records, today_str)
                    if ok:
                        success_count += 1
                    else:
                        fail_count += 1
                else:
                    fail_count += 1

                self.cleanup_old_files(pair_name, tf_name, self.retention_days[tf_name])

        print("\n収集完了サマリー")
        print("=" * 60)
        for pair_name in self.fx_pairs:
            print(f"{pair_name.upper()}: {len(self.timeframes)}時間軸")
        print(f"成功: {success_count}件 / 失敗: {fail_count}件")
        print("=" * 60)

        return success_count, fail_count


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
            "title": f"{emoji} FX OHLCVデータ収集",
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
        collector = FxOhlcvCollector()
        success_count, fail_count = collector.run_collection()

        s3_path = f"s3://{collector.bucket_name}/{collector.s3_prefix}"
        print(f"\n処理完了")
        print(f"データ保存先: {s3_path}")

        if fail_count == 0:
            notify_slack("success",
                f"FX OHLCVデータ収集が完了しました\n"
                f"ペア: {3}種 / 時間軸: {5}種 / 成功: {success_count}件\n"
                f"データ保存先: {s3_path}"
            )
        else:
            notify_slack("failure",
                f"FX OHLCVデータ収集が一部失敗しました\n"
                f"成功: {success_count}件 / 失敗: {fail_count}件"
            )

    except Exception as e:
        print(f"エラーが発生しました: {e}")
        notify_slack("failure", f"エラーが発生しました: {str(e)}")


if __name__ == "__main__":
    main()
