
#!/usr/bin/env python3
"""
JPX株価データ日次更新システム
毎日前日分のデータを取得・更新（登録済み銘柄のみ）
"""

import yfinance as yf
import pandas as pd
import io
import time
import os
from datetime import datetime, timedelta
import warnings
import logging
import boto3

warnings.filterwarnings('ignore')

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

class JPXDailyUpdater:
    """JPX株価データ日次更新 - 前日分のみ更新"""

    def __init__(self):
        self.s3_client = None
        self.registered_symbols = set()
        
        self.config = {
            "request_delay": 2.0,
        }

        print("JPX株価データ日次更新システム")
        print(f"設定: 待機{self.config['request_delay']}秒")

    def setup_s3(self):
        """S3接続（環境変数から認証情報を取得）"""
        try:
            aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
            aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')

            if aws_access_key and aws_secret_key:
                self.s3_client = boto3.client(
                    's3',
                    aws_access_key_id=aws_access_key,
                    aws_secret_access_key=aws_secret_key,
                    region_name="ap-northeast-1"
                )
            else:
                self.s3_client = boto3.client('s3', region_name="ap-northeast-1")

            # 接続テスト
            self.s3_client.head_bucket(Bucket="m-s3storage")
            print("S3接続成功: s3://m-s3storage/japan-stocks-5years-chart/")
            return True

        except Exception as e:
            print(f"S3接続失敗: {e}")
            return False

    def load_registered_symbols(self):
        """登録済みの銘柄をS3から確認"""
        if not self.s3_client:
            return

        try:
            print("登録済み銘柄を確認中...")

            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(
                Bucket="m-s3storage",
                Prefix="japan-stocks-5years-chart/stocks/"
            )

            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        # ファイル名から銘柄コードを抽出 (例: 1332.csv)
                        filename = obj['Key'].split('/')[-1]
                        if filename.endswith('.csv') and len(filename) == 9:  # XXXX.csv
                            symbol = filename.replace('.csv', '') + '.T'
                            self.registered_symbols.add(symbol)

            print(f"登録済み銘柄: {len(self.registered_symbols)}件")

        except Exception as e:
            print(f"登録済み銘柄確認エラー: {e}")

    def load_existing_data(self, symbol):
        """S3から既存データを取得"""
        if not self.s3_client:
            return None

        try:
            clean_symbol = symbol.replace('.T', '')
            s3_key = f"japan-stocks-5years-chart/stocks/{clean_symbol}.csv"

            response = self.s3_client.get_object(Bucket="m-s3storage", Key=s3_key)
            df = pd.read_csv(
                io.BytesIO(response['Body'].read()),
                index_col=0,
                parse_dates=True
            )
            return df

        except Exception as e:
            return None

    def get_yesterday_data(self, symbol):
        """前日分のデータのみ取得"""
        try:
            time.sleep(self.config["request_delay"])

            # 前日の日付を取得
            yesterday = datetime.now() - timedelta(days=1)
            
            ticker = yf.Ticker(symbol)
            # 前日を含む3日分取得（市場休場対応）
            start_date = yesterday - timedelta(days=2)
            end_date = yesterday + timedelta(days=1)
            
            data = ticker.history(
                start=start_date.strftime('%Y-%m-%d'),
                end=end_date.strftime('%Y-%m-%d')
            )

            if data.empty:
                return None

            # VWAP計算
            typical_price = (data['High'] + data['Low'] + data['Close']) / 3
            data['VWAP'] = (typical_price * data['Volume']).cumsum() / data['Volume'].cumsum()

            # 企業情報取得
            try:
                info = ticker.info
                shares_outstanding = info.get('sharesOutstanding', None)

                if shares_outstanding:
                    data['時価総額'] = data['Close'] * shares_outstanding
                else:
                    current_market_cap = info.get('marketCap', None)
                    if current_market_cap:
                        current_price = data['Close'].iloc[-1]
                        estimated_shares = current_market_cap / current_price
                        data['時価総額'] = data['Close'] * estimated_shares
                    else:
                        data['時価総額'] = None

            except:
                data['時価総額'] = None

            # 配当データ取得
            try:
                dividends = ticker.dividends
                data['配当金額'] = 0.0
                data['配当日'] = ''

                for div_date, div_amount in dividends.items():
                    if div_date in data.index:
                        data.loc[div_date, '配当金額'] = div_amount
                        data.loc[div_date, '配当日'] = div_date.strftime('%Y-%m-%d')

            except:
                data['配当金額'] = 0.0
                data['配当日'] = ''

            return data

        except Exception as e:
            if "Too Many Requests" not in str(e) and "Rate limited" not in str(e):
                print(f"{symbol} エラー: {e}")
            return None

    def merge_and_save(self, symbol, new_data):
        """既存データと新データをマージしてS3に保存"""
        if new_data is None or new_data.empty:
            return False

        try:
            clean_symbol = symbol.replace('.T', '')
            
            # 既存データを取得
            existing_data = self.load_existing_data(symbol)

            if existing_data is not None:
                # 既存データと新データをマージ
                combined_data = existing_data.copy()
                for date, row in new_data.iterrows():
                    combined_data.loc[date] = row

                # 日付順にソート
                combined_data = combined_data.sort_index()
                final_data = combined_data
            else:
                final_data = new_data.sort_index()

            # 日本語列名でリネーム
            jp_data = final_data.rename(columns={
                'Open': '始値',
                'High': '高値',
                'Low': '安値',
                'Close': '終値',
                'Volume': '出来高',
                'VWAP': 'VWAP'
            })

            # CSV作成
            csv_buffer = io.StringIO()
            jp_data.to_csv(csv_buffer, encoding='utf-8-sig')

            # S3アップロード
            s3_key = f"japan-stocks-5years-chart/stocks/{clean_symbol}.csv"

            self.s3_client.put_object(
                Bucket="m-s3storage",
                Key=s3_key,
                Body=csv_buffer.getvalue().encode('utf-8'),
                ContentType='text/csv'
            )

            return True

        except Exception as e:
            print(f"マージ・保存エラー ({symbol}): {e}")
            return False

    def collect_daily_update(self):
        """日次更新実行"""
        if not self.registered_symbols:
            print("登録済み銘柄なし")
            return False

        total = len(self.registered_symbols)
        print(f"\n日次更新開始: {total} 銘柄")
        print(f"対象: 前日分のデータ")

        success_count = 0
        failed_count = 0
        skipped_count = 0

        for i, symbol in enumerate(sorted(self.registered_symbols), 1):
            try:
                if (i - 1) % 50 == 0:
                    print(f"\n[{i}/{total}] 処理中...")

                # 前日分データ取得
                new_data = self.get_yesterday_data(symbol)

                if new_data is not None and not new_data.empty:
                    # データをマージしてS3に保存
                    if self.merge_and_save(symbol, new_data):
                        success_count += 1
                        print(f"  {symbol}: ✓")
                    else:
                        failed_count += 1
                        print(f"  {symbol}: ✗")
                else:
                    skipped_count += 1
                    print(f"  {symbol}: (データなし)")

                # 進捗表示（50銘柄ごと）
                if i % 50 == 0:
                    print(f"  進捗: {i}/{total} - 成功:{success_count} 失敗:{failed_count} スキップ:{skipped_count}")

            except Exception as e:
                print(f"エラー ({symbol}): {e}")
                failed_count += 1

        # 最終結果
        print(f"\n日次更新完了:")
        print(f"  成功: {success_count} 銘柄")
        print(f"  失敗: {failed_count} 銘柄")
        print(f"  スキップ: {skipped_count} 銘柄")
        print(f"  成功率: {success_count/(total)*100:.1f}%")

        return True

def main():
    """メイン実行関数"""
    print("JPX株価データ日次更新システム")
    print("=" * 70)

    updater = JPXDailyUpdater()

    # Step 1: S3接続
    if not updater.setup_s3():
        print("S3接続失敗")
        return

    # Step 2: 登録済み銘柄確認
    updater.load_registered_symbols()

    if not updater.registered_symbols:
        print("登録済み銘柄がありません")
        print("初回用スクリプト (collect_jpx_stocks_initial.py) を実行してください")
        return

    # Step 3: 日次更新実行
    print("\n日次更新を実行します")
    start_time = time.time()
    updater.collect_daily_update()
    elapsed = time.time() - start_time

    print(f"\n処理完了: {elapsed:.1f}秒")
    print(f"S3保存先: s3://m-s3storage/japan-stocks-5years-chart/stocks/")

if __name__ == "__main__":
    main()
