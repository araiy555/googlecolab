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
from tqdm.notebook import tqdm
from datetime import datetime, timedelta
import warnings
import logging
from pathlib import Path
import boto3

warnings.filterwarnings('ignore')

# ログ設定（エラーメッセージ抑制）
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

        # 保存ディレクトリ
        self.data_dir = Path("jpx_stock_data")
        self.data_dir.mkdir(exist_ok=True)

        # レート制限対策設定
        self.config = {
            "max_workers": 3,        # 3並列に制限
            "request_delay": 2.0,    # 2秒待機
            "chunk_size": 50,        # 50銘柄ずつ
            "chunk_delay": 120       # チャンク間2分休憩
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

            # 接続テスト
            self.s3_client.head_bucket(Bucket="m-s3storage")
            print("S3接続成功: s3://m-s3storage/japan-stocks-5years-chart/")
            return True

        except Exception as e:
            print(f"S3接続失敗: {e}")
            return False

    def get_jpx_symbols(self):
        """JPX銘柄取得 - Excelデータのみ使用"""
        print("JPX銘柄リスト取得中...")

        # JPX公式データ取得
        jpx_data = self._download_jpx_data()
        if jpx_data is None:
            raise Exception("JPX Excelファイル取得失敗")

        print(f"Excel取得成功: {len(jpx_data)} 行")

        # 銘柄抽出
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

                # HTMLでないことを確認
                if b'<html' not in content[:500].lower():
                    try:
                        # xlrd使用
                        df = pd.read_excel(io.BytesIO(content), engine='xlrd', dtype=str)
                        if len(df) > 1000:
                            print(f"JPX Excel解析成功: {len(df)} 行")
                            return df
                    except:
                        try:
                            # calamine使用
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
        """銘柄コード抽出 - デバッグ強化"""
        print(f"銘柄コード抽出開始: {df.shape}")
        print(f"列名: {list(df.columns)}")
        print("データサンプル:")
        print(df.head(3))

        symbols = set()
        import re

        # 全ての列をチェック
        for col_idx, col in enumerate(df.columns):
            col_symbols = []

            for value in df[col].dropna():
                value_str = str(value).strip()

                # パターン1: 4桁数値
                if value_str.isdigit() and len(value_str) == 4:
                    code = int(value_str)
                    if 1000 <= code <= 9999:
                        col_symbols.append(f"{value_str}.T")
                        symbols.add(f"{value_str}.T")

                # パターン2: 小数点付き
                elif '.' in value_str:
                    parts = value_str.split('.')
                    if len(parts) >= 2 and parts[0].isdigit() and len(parts[0]) == 4:
                        code = int(parts[0])
                        if 1000 <= code <= 9999:
                            col_symbols.append(f"{parts[0]}.T")
                            symbols.add(f"{parts[0]}.T")

                # パターン3: 正規表現
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
        print(f"最終抽出結果: {len(result)} 銘柄")

        if result:
            print(f"抽出例: {result[:10]}")

        return result

    def get_stock_data_safe(self, symbol):
        """安全な株価データ取得（配当日・動的時価総額対応）"""
        try:
            # 長い待機でレート制限回避
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

                # 時価総額計算（株価 × 発行済株式数）
                if shares_outstanding:
                    # 正確な時価総額計算
                    data['時価総額'] = data['Close'] * shares_outstanding
                else:
                    # フォールバック: 現在時価総額から逆算
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
                # 5年分に絞る
                five_years_ago = data.index[0]
                recent_dividends = dividends[dividends.index >= five_years_ago]

                # 配当関連列を初期化
                data['配当金額'] = 0.0
                data['配当日'] = ''  # 配当日の文字列

                for div_date, div_amount in recent_dividends.items():
                    # 配当日に最も近い取引日を見つける
                    closest_date = data.index[data.index.get_indexer([div_date], method='nearest')[0]]
                    data.loc[closest_date, '配当金額'] = div_amount
                    # 実際の配当日を記録
                    data.loc[closest_date, '配当日'] = div_date.strftime('%Y-%m-%d')

            except Exception as div_error:
                # 配当データ取得失敗時は空で埋める
                data['配当金額'] = 0.0
                data['配当日'] = ''

            return {
                'price_data': data,
                'company_info': company_info
            }

        except Exception as e:
            # レート制限エラーは表示しない
            if "Too Many Requests" not in str(e) and "Rate limited" not in str(e):
                print(f"{symbol} エラー: {e}")
            return None

    def upload_chunk_to_s3(self, chunk_symbols):
        """チャンクをS3アップロード - 配当日・時価総額対応"""
        if not self.s3_client:
            return 0

        uploaded = 0

        for symbol in chunk_symbols:
            if symbol in self.stock_data:
                try:
                    clean_symbol = symbol.replace('.T', '')
                    price_data = self.stock_data[symbol]['price_data']

                    # 日本語列名でリネーム
                    jp_data = price_data.rename(columns={
                        'Open': '始値',
                        'High': '高値',
                        'Low': '安値',
                        'Close': '終値',
                        'Volume': '出来高',
                        'VWAP': 'VWAP'
                        # '時価総額', '配当金額', '配当日' はそのまま
                    })

                    # CSV作成
                    csv_buffer = io.StringIO()
                    jp_data.to_csv(csv_buffer, encoding='utf-8-sig')

                    # S3アップロード - シンプル名
                    s3_key = f"japan-stocks-5years-chart/stocks/{clean_symbol}.csv"

                    self.s3_client.put_object(
                        Bucket="m-s3storage",
                        Key=s3_key,
                        Body=csv_buffer.getvalue().encode('utf-8'),
                        ContentType='text/csv'
                    )

                    uploaded += 1

                except Exception as e:
                    # S3エラーも抑制
                    pass

        return uploaded

    def collect_all_stocks(self):
        """全銘柄収集 - レート制限対策"""
        if not self.jpx_symbols:
            print("銘柄リストなし")
            return False

        total = len(self.jpx_symbols)
        print(f"データ収集開始: {total} 銘柄")
        print(f"レート制限対策: {self.config['request_delay']}秒間隔, {self.config['max_workers']}並列")

        # チャンク分割
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

            # チャンク完了後の処理
            chunk_time = time.time() - chunk_start_time

            # S3アップロード
            if self.s3_client and chunk_success > 0:
                uploaded = self.upload_chunk_to_s3(chunk)
                total_uploaded += uploaded
                print(f"S3アップロード完了: {uploaded} ファイル (累計: {total_uploaded})")

            # 進捗表示
            remaining_chunks = len(chunks) - chunk_idx - 1
            estimated_remaining = remaining_chunks * (chunk_time + self.config["chunk_delay"]) / 60
            print(f"チャンク{chunk_idx + 1}完了: {chunk_success}/{len(chunk)} 成功")
            print(f"残り推定時間: {estimated_remaining:.1f} 分")

            # チャンク間休憩（レート制限回避）
            if chunk_idx < len(chunks) - 1:
                print(f"休憩中... {self.config['chunk_delay']} 秒")
                time.sleep(self.config["chunk_delay"])

        # 最終結果
        print(f"\n収集完了:")
        print(f"  成功: {success_count} 銘柄")
        print(f"  失敗: {len(self.failed_symbols)} 銘柄")
        print(f"  成功率: {success_count/(success_count + len(self.failed_symbols))*100:.1f}%")
        print(f"  S3アップロード: {total_uploaded} ファイル")

        # 最終サマリー保存
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

                # 配当統計
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

            # S3保存 - 固定名
            self.s3_client.put_object(
                Bucket="m-s3storage",
                Key="japan-stocks-5years-chart/summary.csv",
                Body=summary_csv.encode('utf-8'),
                ContentType='text/csv'
            )

            print("サマリー保存完了: summary.csv")

            # 基本統計表示
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

# ===== 実行関数 =====

def run_safe_collection():
    """レート制限対策版実行"""
    print("JPX全銘柄5年分株価データ収集 - 配当日・動的時価総額対応版")
    print("=" * 70)

    collector = JPXStockCollector()

    # Step 1: 銘柄取得
    symbols = collector.get_jpx_symbols()
    if not symbols:
        print("銘柄取得失敗")
        return None

    collector.jpx_symbols = symbols
    print(f"対象銘柄: {len(symbols)} 件")

    # Step 2: S3接続
    s3_ok = collector.setup_s3()
    if not s3_ok:
        print("S3接続失敗")
        return None

    # Step 3: 実行確認
    estimated_time = (len(symbols) / collector.config["chunk_size"]) * (
        collector.config["chunk_size"] * collector.config["request_delay"] / collector.config["max_workers"] +
        collector.config["chunk_delay"]
    ) / 60

    print(f"\n実行確認:")
    print(f"  対象銘柄数: {len(symbols):,}")
    print(f"  推定処理時間: {estimated_time:.1f} 分")
    print(f"  CSV列構成: 始値,高値,安値,終値,出来高,VWAP,時価総額,配当金額,配当日")
    print(f"  ファイル名形式: 1332.csv (シンプル・上書き)")
    print(f"  S3保存: チャンクごとリアルタイム")
    print(f"  レート制限対策: 有効")

    confirm = input("\n実行しますか？ (y/N): ").strip().lower()
    if confirm != 'y':
        print("実行中止")
        return None

    # Step 4: データ収集実行
    start_time = time.time()
    success = collector.collect_all_stocks()
    elapsed = time.time() - start_time

    print(f"\n処理完了: {elapsed/60:.1f} 分")
    print(f"S3保存先: s3://m-s3storage/japan-stocks-5years-chart/stocks/")
    print(f"ファイル名例: 1332.csv, 7203.csv, summary.csv")

    return collector

def test_one_stock():
    """1銘柄テスト（配当日・時価総額確認）"""
    print("1銘柄テスト実行")

    collector = JPXStockCollector()

    # S3接続
    if not collector.setup_s3():
        return False

    # テスト銘柄
    test_symbol = "7203.T"  # トヨタ
    print(f"テスト対象: {test_symbol}")

    result = collector.get_stock_data_safe(test_symbol)

    if result:
        collector.stock_data[test_symbol] = result

        # S3アップロード
        uploaded = collector.upload_chunk_to_s3([test_symbol])

        price_data = result['price_data']
        company_info = result['company_info']

        # 配当日確認
        dividend_days = price_data[price_data['配当金額'] > 0]
        total_dividends = price_data['配当金額'].sum()

        print(f"成功: {company_info['name']}")
        print(f"期間: {price_data.index[0].date()} - {price_data.index[-1].date()}")
        print(f"最新価格: ¥{price_data['Close'].iloc[-1]:,.0f}")

        # 時価総額表示
        if price_data['時価総額'].iloc[-1]:
            market_cap_billion = price_data['時価総額'].iloc[-1] / 1000000000000
            print(f"最新時価総額: ¥{market_cap_billion:.2f}兆円")
        else:
            print("時価総額: N/A")

        # 配当情報表示
        print(f"配当回数: {len(dividend_days)} 回")
        print(f"総配当額: ¥{total_dividends:.2f}")
        if len(dividend_days) > 0:
            print(f"配当履歴:")
            for idx, row in dividend_days.iterrows():
                trading_date = idx.strftime('%Y-%m-%d')
                dividend_date = row['配当日']
                print(f"  取引日:{trading_date} → 配当日:{dividend_date} → ¥{row['配当金額']:.2f}")

        print(f"S3保存: {'成功' if uploaded > 0 else '失敗'}")
        print(f"ファイル名: 7203.csv")

        return True
    else:
        print("テスト失敗")
        return False

def quick_sample(sample_size=100):
    """クイックサンプル実行"""
    print(f"クイックサンプル実行: {sample_size} 銘柄")

    collector = JPXStockCollector()

    symbols = collector.get_jpx_symbols()
    if symbols:
        # サンプル抽出
        import random
        sample_symbols = random.sample(symbols, min(sample_size, len(symbols)))
        collector.jpx_symbols = sample_symbols

        print(f"サンプル銘柄: {len(sample_symbols)} 件")

        if collector.setup_s3():
            collector.collect_all_stocks()
            return collector

    return None

def install_required_libraries():
    """必要ライブラリインストール"""
    import subprocess
    import sys

    libraries = [
        "yfinance>=0.2.18",
        "pandas>=1.5.0",
        "numpy>=1.21.0",
        "requests>=2.28.0",
        "tqdm>=4.64.0",
        "xlrd>=2.0.1",
        "python-calamine>=0.1.0",
        "openpyxl>=3.0.0",
        "boto3>=1.26.0"
    ]

    print("必要ライブラリインストール中...")

    for lib in libraries:
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", lib])
            print(f"✓ {lib}")
        except Exception as e:
            print(f"✗ {lib}: {e}")

# ===== メイン実行 =====

if __name__ == "__main__":
　　run_safe_collection()
