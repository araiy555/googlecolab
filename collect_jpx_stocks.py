# JPX全銘柄5年分株価データ収集システム - 配当日・動的時価総額・市場区分対応版

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
        self.symbol_metadata = {}  # 🆕 JPXメタデータ保存用
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
        """銘柄コード抽出 - 英字銘柄対応 + 市場区分・業種取得"""
        print(f"銘柄コード抽出開始: {df.shape}")
        print(f"列名: {list(df.columns)}")
        print("データサンプル:")
        print(df.head(3))

        symbols = {}  # 🆕 setからdictに変更
        import re

        # 🆕 列名を探す
        code_col = None
        name_col = None
        market_col = None
        sector_33_col = None
        sector_17_col = None
        size_col = None

        for col in df.columns:
            col_str = str(col)
            if 'コード' in col_str and '業種' not in col_str and '規模' not in col_str:
                if code_col is None:
                    code_col = col
            elif '銘柄名' in col_str:
                name_col = col
            elif '市場' in col_str or 'Market' in col_str:
                market_col = col
            elif '33業種区分' in col_str:
                sector_33_col = col
            elif '17業種区分' in col_str:
                sector_17_col = col
            elif '規模区分' in col_str:
                size_col = col

        print(f"\n🔍 検出された列:")
        print(f"  コード列: {code_col}")
        print(f"  銘柄名列: {name_col}")
        print(f"  市場区分列: {market_col}")
        print(f"  33業種区分列: {sector_33_col}")
        print(f"  17業種区分列: {sector_17_col}")
        print(f"  規模区分列: {size_col}")

        # データ抽出
        for idx, row in df.iterrows():
            try:
                # 銘柄コード取得
                if code_col is None:
                    continue
                    
                code_value = str(row[code_col]).strip()
                if not code_value or code_value == 'nan':
                    continue

                # 4桁数値のみ
                if code_value.isdigit() and len(code_value) == 4:
                    code = int(code_value)
                    if 1000 <= code <= 9999:
                        symbol = f"{code_value}.T"
                        
                        # 🆕 追加情報を取得
                        symbols[symbol] = {
                            'code': code_value,
                            'name': str(row[name_col]).strip() if name_col and pd.notna(row[name_col]) else 'N/A',
                            'market': str(row[market_col]).strip() if market_col and pd.notna(row[market_col]) else 'N/A',
                            'sector_33': str(row[sector_33_col]).strip() if sector_33_col and pd.notna(row[sector_33_col]) else 'N/A',
                            'sector_17': str(row[sector_17_col]).strip() if sector_17_col and pd.notna(row[sector_17_col]) else 'N/A',
                            'size': str(row[size_col]).strip() if size_col and pd.notna(row[size_col]) else 'N/A',
                        }
                
                # 数字+英字（130A, 1475BXなど）
                elif re.match(r'^\d{3,4}[A-Z]{1,2}$', code_value):
                    symbol = f"{code_value}.T"
                    symbols[symbol] = {
                        'code': code_value,
                        'name': str(row[name_col]).strip() if name_col and pd.notna(row[name_col]) else 'N/A',
                        'market': str(row[market_col]).strip() if market_col and pd.notna(row[market_col]) else 'N/A',
                        'sector_33': str(row[sector_33_col]).strip() if sector_33_col and pd.notna(row[sector_33_col]) else 'N/A',
                        'sector_17': str(row[sector_17_col]).strip() if sector_17_col and pd.notna(row[sector_17_col]) else 'N/A',
                        'size': str(row[size_col]).strip() if size_col and pd.notna(row[size_col]) else 'N/A',
                    }

            except Exception as e:
                continue

        result = sorted(list(symbols.keys()))
        
        # 統計表示
        alphabetic = [s for s in result if re.search(r'[A-Z]', s.replace('.T', ''))]
        numeric_only = [s for s in result if not re.search(r'[A-Z]', s.replace('.T', ''))]
        
        # 🆕 市場区分別の統計
        markets = {}
        for symbol, info in symbols.items():
            market = info['market']
            if market not in markets:
                markets[market] = 0
            markets[market] += 1
        
        print(f"\n最終抽出結果: 合計 {len(result)} 銘柄")
        print(f"  数値のみ: {len(numeric_only)} 銘柄")
        if alphabetic:
            print(f"  英字付き: {len(alphabetic)} 銘柄")
        
        print(f"\n📊 市場区分別:")
        for market, count in sorted(markets.items()):
            print(f"  {market}: {count}銘柄")

        if result:
            print(f"\n抽出例（詳細）:")
            for symbol in result[:5]:
                info = symbols[symbol]
                print(f"  {symbol}: {info['name']} | {info['market']} | {info['sector_17']}")

        # 🆕 symbolsをインスタンス変数に保存
        self.symbol_metadata = symbols

        return result

    def get_stock_data_safe(self, symbol):
        """安全な株価データ取得（配当日・動的時価総額・追加指標対応）"""
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

                # 🆕 52週高値・安値を計算
                if len(data) >= 252:
                    week_52_high = data['High'].tail(252).max()
                    week_52_low = data['Low'].tail(252).min()
                else:
                    week_52_high = None
                    week_52_low = None

                company_info = {
                    'name': info.get('longName', info.get('shortName', 'N/A')),
                    'sector': info.get('sector', 'N/A'),
                    'industry': info.get('industry', 'N/A'),  # 🆕 業種詳細
                    'market_cap': info.get('marketCap', None),
                    'shares_outstanding': shares_outstanding,
                    
                    # 🆕 バリュエーション指標
                    'trailing_pe': info.get('trailingPE', None),  # PER
                    'price_to_book': info.get('priceToBook', None),  # PBR
                    'beta': info.get('beta', None),  # ベータ値
                    'dividend_yield': info.get('dividendYield', None),  # 配当利回り
                    
                    # 🆕 52週高値・安値
                    'week_52_high': week_52_high,
                    'week_52_low': week_52_low,
                }
            except:
                data['時価総額'] = None
                company_info = {
                    'name': 'N/A',
                    'sector': 'N/A',
                    'industry': 'N/A',
                    'market_cap': None,
                    'shares_outstanding': None,
                    'trailing_pe': None,
                    'price_to_book': None,
                    'beta': None,
                    'dividend_yield': None,
                    'week_52_high': None,
                    'week_52_low': None,
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
        """サマリーをS3保存 - 統計指標・市場区分追加版"""
        try:
            summary_data = []

            for symbol, data_info in self.stock_data.items():
                price_data = data_info['price_data']
                company_info = data_info['company_info']
                
                # 🆕 JPXメタデータ取得
                jpx_meta = self.symbol_metadata.get(symbol, {})

                # 基本情報
                first_price = price_data['Close'].iloc[0]
                latest_price = price_data['Close'].iloc[-1]
                total_return = (latest_price / first_price - 1) * 100

                # 配当情報
                dividend_count = len(price_data[price_data['配当金額'] > 0])
                total_dividends = price_data['配当金額'].sum()

                # 🆕 統計指標計算
                try:
                    returns = price_data['Close'].pct_change().dropna()
                    returns = returns.replace([np.inf, -np.inf], np.nan).dropna()
                    
                    if len(returns) > 50:
                        # ボラティリティ（年率）
                        volatility = returns.std() * np.sqrt(252)
                        
                        # 年率リターン
                        annual_return = returns.mean() * 252
                        
                        # シャープレシオ
                        sharpe_ratio = annual_return / volatility if volatility > 0 else 0
                        
                        # 最大ドローダウン
                        cumulative = (1 + returns).cumprod()
                        running_max = cumulative.expanding().max()
                        drawdown = (cumulative - running_max) / running_max
                        max_drawdown = drawdown.min()
                        
                        # 平均売買代金
                        trading_values = price_data['Close'] * price_data['Volume']
                        avg_trading_value = trading_values.mean()
                    else:
                        volatility = None
                        annual_return = None
                        sharpe_ratio = None
                        max_drawdown = None
                        avg_trading_value = None
                        
                except Exception as e:
                    volatility = None
                    annual_return = None
                    sharpe_ratio = None
                    max_drawdown = None
                    avg_trading_value = None

                # 🆕 配当利回り計算
                if company_info.get('dividend_yield') is None and total_dividends > 0:
                    annual_dividend = total_dividends / 5
                    calculated_yield = (annual_dividend / latest_price) if latest_price > 0 else 0
                else:
                    calculated_yield = company_info.get('dividend_yield')

                summary_data.append({
                    '銘柄コード': symbol.replace('.T', ''),
                    '会社名': jpx_meta.get('name', company_info.get('name', 'N/A')),
                    
                    # 🆕 JPX情報
                    '市場区分': jpx_meta.get('market', 'N/A'),
                    '33業種区分': jpx_meta.get('sector_33', 'N/A'),
                    '17業種区分': jpx_meta.get('sector_17', 'N/A'),
                    '規模区分': jpx_meta.get('size', 'N/A'),
                    
                    # yfinance情報
                    'セクター（YF）': company_info.get('sector', 'N/A'),
                    '業種（YF）': company_info.get('industry', 'N/A'),
                    
                    # 期間情報
                    '期間開始': str(price_data.index[0].date()),
                    '期間終了': str(price_data.index[-1].date()),
                    'データ日数': len(price_data),
                    
                    # 価格情報
                    '開始価格': round(first_price, 2) if first_price else None,
                    '最新価格': round(latest_price, 2) if latest_price else None,
                    '52週高値': round(company_info.get('week_52_high'), 2) if company_info.get('week_52_high') else None,
                    '52週安値': round(company_info.get('week_52_low'), 2) if company_info.get('week_52_low') else None,
                    
                    # リターン指標
                    '5年変化率(%)': round(total_return, 2) if total_return else None,
                    '年率リターン(%)': round(annual_return * 100, 2) if annual_return is not None and not np.isnan(annual_return) else None,
                    
                    # リスク指標
                    'ボラティリティ(%)': round(volatility * 100, 2) if volatility is not None and not np.isnan(volatility) else None,
                    'シャープレシオ': round(sharpe_ratio, 2) if sharpe_ratio is not None and not np.isnan(sharpe_ratio) else None,
                    '最大DD(%)': round(max_drawdown * 100, 2) if max_drawdown is not None and not np.isnan(max_drawdown) else None,
                    
                    # バリュエーション
                    'PER': round(company_info.get('trailing_pe'), 2) if company_info.get('trailing_pe') else None,
                    'PBR': round(company_info.get('price_to_book'), 2) if company_info.get('price_to_book') else None,
                    'ベータ': round(company_info.get('beta'), 2) if company_info.get('beta') else None,
                    
                    # 流動性指標
                    '平均出来高': int(price_data['Volume'].mean()) if not price_data['Volume'].isna().all() else None,
                    '平均売買代金': int(avg_trading_value) if avg_trading_value is not None and not np.isnan(avg_trading_value) else None,
                    
                    # 配当情報
                    '配当回数': dividend_count,
                    '総配当額': round(total_dividends, 2) if total_dividends > 0 else 0,
                    '配当利回り(%)': round(calculated_yield * 100, 2) if calculated_yield and not np.isnan(calculated_yield) else None,
                    
                    # 企業情報
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

            # 統計表示
            total_stocks = len(summary_df)
            positive_returns = len(summary_df[summary_df['5年変化率(%)'] > 0])
            dividend_stocks = len(summary_df[summary_df['配当回数'] > 0])

            print(f"\n最終統計:")
            print(f"  総銘柄数: {total_stocks}")
            print(f"  プラスリターン: {positive_returns}/{total_stocks} ({positive_returns/total_stocks*100:.1f}%)")
            print(f"  配当支払い銘柄: {dividend_stocks}/{total_stocks} ({dividend_stocks/total_stocks*100:.1f}%)")
            
            if len(summary_df) > 0:
                print(f"  平均5年リターン: {summary_df['5年変化率(%)'].mean():.2f}%")
                print(f"  平均年率リターン: {summary_df['年率リターン(%)'].mean():.2f}%")
                print(f"  平均ボラティリティ: {summary_df['ボラティリティ(%)'].mean():.2f}%")
                print(f"  平均シャープレシオ: {summary_df['シャープレシオ'].mean():.2f}")
                print(f"  平均配当回数: {summary_df['配当回数'].mean():.1f}回")

        except Exception as e:
            print(f"サマリー保存エラー: {e}")
            import traceback
            traceback.print_exc()


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
