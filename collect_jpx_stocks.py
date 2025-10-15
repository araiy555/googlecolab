# JPX全銘柄5年分株価データ収集システム - 配当日・動的時価総額対応版

import yfinance as yf
import pandas as pd
import numpy as np
import requests
import io
import time
import os
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from tqdm import tqdm
from datetime import datetime, timedelta
import warnings
import logging
from pathlib import Path
import boto3

warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

class JPXStockCollector:
    """JPX株価データ収集 - レート制限対策・配当日・動的時価総額対応"""

    def __init__(self):
        self.jpx_symbols = []
        self.stock_data = {}
        self.failed_symbols = []
        self.lock = threading.Lock()
        self.s3_client = None

        self.data_dir = Path("jpx_stock_data")
        self.data_dir.mkdir(exist_ok=True)

        self.config = {
            "max_workers": 3,
            "request_delay": 2.0,
            "chunk_size": 300,
            "chunk_delay": 120
        }

        print("JPX株価収集システム - 配当日・動的時価総額対応版")
        print(f"設定: 並列{self.config['max_workers']}, 待機{self.config['request_delay']}秒, チャンク{self.config['chunk_size']}")

    def setup_s3(self):
        """S3接続"""
        try:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                region_name="ap-northeast-1"
            )
            self.s3_client.head_bucket(Bucket="m-s3storage")
            print("S3接続成功: s3://m-s3storage/japan-stocks-5years-chart/")
            return True
        except Exception as e:
            print(f"S3接続失敗: {e}")
            return False

    def get_jpx_symbols(self):
        """JPX銘柄取得 - Excelデータのみ使用"""
        print("JPX銘柄リスト取得中...")

        jpx_data = self._download_jpx_data()
        if jpx_data is None:
            raise Exception("JPX Excelファイル取得失敗")

        print(f"Excel取得成功: {len(jpx_data)} 行")

        symbols = self._extract_symbols(jpx_data)
        if not symbols or len(symbols) < 100:
            raise Exception(f"銘柄抽出失敗: {len(symbols) if symbols else 0} 銘柄のみ抽出")

        print(f"JPX公式データから {len(symbols)} 銘柄抽出成功")
        return symbols

    def _download_jpx_data(self):
        """JPX data_j.xls取得"""
        url = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"

        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Referer': 'https://www.jpx.co.jp/markets/statistics-equities/misc/01.html'
            }

            response = requests.get(url, headers=headers, timeout=60)

            if response.status_code == 200 and len(response.content) > 100000:
                content = response.content

                if b'<html' not in content[:500].lower():
                    try:
                        df = pd.read_excel(io.BytesIO(content), engine='xlrd', dtype=str)
                        if len(df) > 1000:
                            print(f"JPX Excel解析成功: {len(df)} 行")
                            return df
                    except:
                        try:
                            df = pd.read_excel(io.BytesIO(content), engine='calamine', dtype=str)
                            if len(df) > 1000:
                                print(f"JPX Excel解析成功: {len(df)} 行")
                                return df
                        except:
                            pass

            return None

        except:
            return None

    def _extract_symbols(self, df):
        """銘柄コード抽出 - 英字銘柄対応"""
        print(f"銘柄コード抽出開始: {df.shape}")
        print(f"列名: {list(df.columns)}")
        print("データサンプル:")
        print(df.head(3))

        symbols = set()
        import re

        for col_idx, col in enumerate(df.columns):
            col_symbols = []

            for value in df[col].dropna():
                value_str = str(value).strip()

                # パターン1: 4桁数値のみ
                if value_str.isdigit() and len(value_str) == 4:
                    code = int(value_str)
                    if 1000 <= code <= 9999:
                        col_symbols.append(f"{value_str}.T")
                        symbols.add(f"{value_str}.T")

                # パターン2: 数字+英字（130A, 1475BXなど）
                elif re.match(r'^\d{3,4}[A-Z]{1,2}$', value_str):
                    col_symbols.append(f"{value_str}.T")
                    symbols.add(f"{value_str}.T")

                # パターン3: 小数点付き
                elif '.' in value_str:
                    parts = value_str.split('.')
                    if len(parts) >= 2:
                        code_part = parts[0]
                        
                        # 数値のみ
                        if code_part.isdigit() and len(code_part) == 4:
                            code = int(code_part)
                            if 1000 <= code <= 9999:
                                col_symbols.append(f"{code_part}.T")
                                symbols.add(f"{code_part}.T")
                        
                        # 数字+英字
                        elif re.match(r'^\d{3,4}[A-Z]{1,2}$', code_part):
                            col_symbols.append(f"{code_part}.T")
                            symbols.add(f"{code_part}.T")

                # パターン4: 正規表現で4桁数値抽出
                matches = re.findall(r'\b(\d{4})\b', value_str)
                for match in matches:
                    code = int(match)
                    if 1000 <= code <= 9999:
                        col_symbols.append(f"{match}.T")
                        symbols.add(f"{match}.T")

            if col_symbols:
                print(f"列 {col_idx} '{col}': {len(col_symbols)} 銘柄抽出")
                if len(col_symbols) > 10:
                    print(f"  例: {col_symbols[:5]}")

        result = sorted(list(symbols))
        
        # 英字銘柄の集計表示
        alphabetic = [s for s in result if re.search(r'[A-Z]', s.replace('.T', ''))]
        numeric_only = [s for s in result if not re.search(r'[A-Z]', s.replace('.T', ''))]
        
        print(f"\n最終抽出結果: 合計 {len(result)} 銘柄")
        print(f"  数値のみ: {len(numeric_only)} 銘柄")
        if alphabetic:
            print(f"  英字付き: {len(alphabetic)} 銘柄")
            print(f"  英字例: {alphabetic[:10]}")

        if result:
            print(f"抽出例: {result[:10]}")

        return result

    def get_stock_data_safe(self, symbol):
        """安全な株価データ取得（配当日・動的時価総額対応）"""
        try:
            time.sleep(self.config["request_delay"])

            ticker = yf.Ticker(symbol)
            data = ticker.history(period="5y")

            if data.empty or len(data) < 100:
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

                company_info = {
                    'name': info.get('longName', info.get('shortName', 'N/A')),
                    'sector': info.get('sector', 'N/A'),
                    'market_cap': info.get('marketCap', None),
                    'shares_outstanding': shares_outstanding
                }
            except:
                data['時価総額'] = None
                company_info = {
                    'name': 'N/A',
                    'sector': 'N/A',
                    'market_cap': None,
                    'shares_outstanding': None
                }

            # 配当データ取得
            try:
                dividends = ticker.dividends
                five_years_ago = data.index[0]
                recent_dividends = dividends[dividends.index >= five_years_ago]

                data['配当金額'] = 0.0
                data['配当日'] = ''

                for div_date, div_amount in recent_dividends.items():
                    closest_date = data.index[data.index.get_indexer([div_date], method='nearest')[0]]
                    data.loc[closest_date, '配当金額'] = div_amount
                    data.loc[closest_date, '配当日'] = div_date.strftime('%Y-%m-%d')

            except Exception as div_error:
                data['配当金額'] = 0.0
                data['配当日'] = ''

            return {
                'price_data': data,
                'company_info': company_info
            }

        except Exception as e:
            if "Too Many Requests" not in str(e) and "Rate limited" not in str(e):
                print(f"{symbol} エラー: {e}")
            return None

    def upload_chunk_to_s3(self, chunk_symbols):
        """チャンクをS3アップロード"""
        if not self.s3_client:
            return 0

        uploaded = 0

        for symbol in chunk_symbols:
            if symbol in self.stock_data:
                try:
                    clean_symbol = symbol.replace('.T', '')
                    price_data = self.stock_data[symbol]['price_data']

                    jp_data = price_data.rename(columns={
                        'Open': '始値',
                        'High': '高値',
                        'Low': '安値',
                        'Close': '終値',
                        'Volume': '出来高',
                        'VWAP': 'VWAP'
                    })

                    csv_buffer = io.StringIO()
                    jp_data.to_csv(csv_buffer, encoding='utf-8-sig')

                    s3_key = f"japan-stocks-5years-chart/stocks/{clean_symbol}.csv"

                    self.s3_client.put_object(
                        Bucket="m-s3storage",
                        Key=s3_key,
                        Body=csv_buffer.getvalue().encode('utf-8'),
                        ContentType='text/csv'
                    )

                    uploaded += 1

                except Exception as e:
                    pass

        return uploaded

    def collect_all_stocks(self):
        """全銘柄収集"""
        if not self.jpx_symbols:
            print("銘柄リストなし")
            return False

        total = len(self.jpx_symbols)
        print(f"データ収集開始: {total} 銘柄")
        print(f"レート制限対策: {self.config['request_delay']}秒間隔, {self.config['max_workers']}並列")

        chunk_size = self.config["chunk_size"]
        chunks = [self.jpx_symbols[i:i+chunk_size] for i in range(0, len(self.jpx_symbols), chunk_size)]

        success_count = 0
        total_uploaded = 0

        for chunk_idx, chunk in enumerate(chunks):
            print(f"\nチャンク {chunk_idx + 1}/{len(chunks)} 処理中 ({len(chunk)} 銘柄)")

            chunk_success = 0
            chunk_start_time = time.time()

            with tqdm(total=len(chunk), desc=f"チャンク{chunk_idx + 1}") as pbar:
                with ThreadPoolExecutor(max_workers=self.config["max_workers"]) as executor:
                    futures = {executor.submit(self.get_stock_data_safe, symbol): symbol for symbol in chunk}

                    for future in as_completed(futures):
                        symbol = futures[future]
                        result = future.result()

                        if result:
                            with self.lock:
                                self.stock_data[symbol] = result
                                success_count += 1
                                chunk_success += 1
                        else:
                            with self.lock:
                                self.failed_symbols.append(symbol)

                        pbar.update(1)
                        pbar.set_postfix({
                            'チャンク成功': chunk_success,
                            '総成功': success_count,
                            '成功率': f"{success_count/(success_count + len(self.failed_symbols))*100:.1f}%"
                        })

            chunk_time = time.time() - chunk_start_time

            if self.s3_client and chunk_success > 0:
                uploaded = self.upload_chunk_to_s3(chunk)
                total_uploaded += uploaded
                print(f"S3アップロード完了: {uploaded} ファイル (累計: {total_uploaded})")

            remaining_chunks = len(chunks) - chunk_idx - 1
            estimated_remaining = remaining_chunks * (chunk_time + self.config["chunk_delay"]) / 60
            print(f"チャンク{chunk_idx + 1}完了: {chunk_success}/{len(chunk)} 成功")
            print(f"残り推定時間: {estimated_remaining:.1f} 分")

            if chunk_idx < len(chunks) - 1:
                print(f"休憩中... {self.config['chunk_delay']} 秒")
                time.sleep(self.config["chunk_delay"])

        print(f"\n収集完了:")
        print(f"  成功: {success_count} 銘柄")
        print(f"  失敗: {len(self.failed_symbols)} 銘柄")
        print(f"  成功率: {success_count/(success_count + len(self.failed_symbols))*100:.1f}%")
        print(f"  S3アップロード: {total_uploaded} ファイル")

        if self.s3_client and self.stock_data:
            self._save_summary_to_s3()

        return True

    def _save_summary_to_s3(self):
        """サマリーをS3保存"""
        try:
            summary_data = []

            for symbol, data_info in self.stock_data.items():
                price_data = data_info['price_data']
                company_info = data_info['company_info']

                first_price = price_data['Close'].iloc[0]
                latest_price = price_data['Close'].iloc[-1]
                total_return = (latest_price / first_price - 1) * 100

                dividend_count = len(price_data[price_data['配当金額'] > 0])
                total_dividends = price_data['配当金額'].sum()

                summary_data.append({
                    '銘柄コード': symbol.replace('.T', ''),
                    '会社名': company_info.get('name', 'N/A'),
                    'セクター': company_info.get('sector', 'N/A'),
                    '期間開始': str(price_data.index[0].date()),
                    '期間終了': str(price_data.index[-1].date()),
                    'データ日数': len(price_data),
                    '開始価格': round(first_price, 2) if first_price else None,
                    '最新価格': round(latest_price, 2) if latest_price else None,
                    '5年変化率(%)': round(total_return, 2) if total_return else None,
                    '平均出来高': int(price_data['Volume'].mean()) if not price_data['Volume'].isna().all() else None,
                    '配当回数': dividend_count,
                    '総配当額': round(total_dividends, 2) if total_dividends > 0 else 0,
                    '最新時価総額': price_data['時価総額'].iloc[-1] if price_data['時価総額'].iloc[-1] else None
                })

            summary_df = pd.DataFrame(summary_data)
            summary_csv = summary_df.to_csv(index=False, encoding='utf-8-sig')

            self.s3_client.put_object(
                Bucket="m-s3storage",
                Key="japan-stocks-5years-chart/summary.csv",
                Body=summary_csv.encode('utf-8'),
                ContentType='text/csv'
            )

            print("サマリー保存完了: summary.csv")

            total_stocks = len(summary_df)
            positive_returns = len(summary_df[summary_df['5年変化率(%)'] > 0])
            dividend_stocks = len(summary_df[summary_df['配当回数'] > 0])

            print(f"\n最終統計:")
            print(f"  総銘柄数: {total_stocks}")
            print(f"  プラスリターン: {positive_returns}/{total_stocks} ({positive_returns/total_stocks*100:.1f}%)")
            print(f"  配当支払い銘柄: {dividend_stocks}/{total_stocks} ({dividend_stocks/total_stocks*100:.1f}%)")
            if len(summary_df) > 0:
                print(f"  平均5年リターン: {summary_df['5年変化率(%)'].mean():.2f}%")
                print(f"  平均配当回数: {summary_df['配当回数'].mean():.1f}回")

        except Exception as e:
            print(f"サマリー保存エラー: {e}")


def run_safe_collection():
    """レート制限対策版実行"""
    print("JPX全銘柄5年分株価データ収集 - 配当日・動的時価総額対応版")
    print("=" * 70)

    collector = JPXStockCollector()

    symbols = collector.get_jpx_symbols()
    if not symbols:
        print("銘柄取得失敗")
        return None

    collector.jpx_symbols = symbols
    print(f"対象銘柄: {len(symbols)} 件")

    s3_ok = collector.setup_s3()
    if not s3_ok:
        print("S3接続失敗")
        return None

    estimated_time = (len(symbols) / collector.config["chunk_size"]) * (
        collector.config["chunk_size"] * collector.config["request_delay"] / collector.config["max_workers"] +
        collector.config["chunk_delay"]
    ) / 60

    print(f"\n実行確認:")
    print(f"  対象銘柄数: {len(symbols):,}")
    print(f"  推定処理時間: {estimated_time:.1f} 分")

    start_time = time.time()
    success = collector.collect_all_stocks()
    elapsed = time.time() - start_time

    print(f"\n処理完了: {elapsed/60:.1f} 分")
    print(f"S3保存先: s3://m-s3storage/japan-stocks-5years-chart/stocks/")

    return collector


if __name__ == "__main__":
    try:
        run_safe_collection()
        status = "✅ 成功"
        color = "good"
    except Exception as e:
        status = f"❌ 失敗: {str(e)}"
        color = "danger"
    
    slack_webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    
    if slack_webhook_url:
        message = {
            "attachments": [{
                "color": color,
                "title": "データ収集完了",
                "text": status,
                "footer": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }]
        }
        requests.post(slack_webhook_url, json=message)
    else:
        print("警告: SLACK_WEBHOOK_URLが設定されていません")
