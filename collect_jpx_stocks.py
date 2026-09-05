# JPX全銘柄5年分株価データ収集システム - 配当日・動的時価総額・市場区分対応版
# 2026-09 修正: JPXが data_j.xls -> data_j.xlsx に差し替えたため取得ロジックを動的解決に変更
import os
import sys
import re

import yfinance as yf
import pandas as pd
import numpy as np
import requests
import io
import time
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
# ルートロガーを潰さない（自分のログが消えるため）
logging.getLogger('yfinance').setLevel(logging.CRITICAL)
logging.getLogger('urllib3').setLevel(logging.ERROR)
logger = logging.getLogger(__name__)

JPX_PAGE_URL = "https://www.jpx.co.jp/markets/statistics-equities/misc/01.html"
JPX_ATT_BASE = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/"


class JPXStockCollector:
    """JPX株価データ収集 - レート制限対策・配当日・動的時価総額対応"""

    def __init__(self):
        self.jpx_symbols = []
        self.stock_data = {}
        self.failed_symbols = []
        self.symbol_metadata = {}
        self.lock = threading.Lock()
        self.s3_client = None

        self.data_dir = Path("jpx_stock_data")
        self.data_dir.mkdir(exist_ok=True)

        self.config = {
            "max_workers": 3,
            "request_delay": 2.0,
            "chunk_size": 300,
            "chunk_delay": 120,
        }

        print("JPX株価収集システム - 配当日・動的時価総額対応版")
        print(f"設定: 並列{self.config['max_workers']}, 待機{self.config['request_delay']}秒, "
              f"チャンク{self.config['chunk_size']}")

    # ------------------------------------------------------------------ S3

    def setup_s3(self):
        """S3接続"""
        try:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                region_name="ap-northeast-1",
            )
            self.s3_client.head_bucket(Bucket="m-s3storage")
            print("S3接続成功: s3://m-s3storage/japan-stocks-5years-chart/")
            return True
        except Exception as e:
            print(f"S3接続失敗: {e}")
            return False

    # -------------------------------------------------------------- 銘柄取得

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

    def _resolve_jpx_urls(self):
        """一覧ページから data_j.* のリンクを抽出し、既知の直リンクをフォールバックに足す"""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': JPX_PAGE_URL,
        }

        candidates = []
        try:
            html = requests.get(JPX_PAGE_URL, headers=headers, timeout=60).text
            for href in re.findall(r'href="([^"]*data_j\.(?:xlsx|xls))"', html):
                candidates.append(requests.compat.urljoin(JPX_PAGE_URL, href))
            if candidates:
                print(f"一覧ページからリンク検出: {len(candidates)} 件")
            else:
                print("警告: 一覧ページに data_j リンクが見つかりません（フォールバックを使用）")
        except Exception as e:
            print(f"JPX一覧ページ取得失敗: {e}（フォールバックを使用）")

        # xlsx を先に試す（2026-09 時点の現行形式）
        candidates += [JPX_ATT_BASE + "data_j.xlsx", JPX_ATT_BASE + "data_j.xls"]

        seen = set()
        return [u for u in candidates if not (u in seen or seen.add(u))]

    @staticmethod
    def _engines_for(content):
        """マジックナンバーから読み込みエンジンを決定"""
        if content[:2] == b'PK':  # xlsx = zip
            return ['openpyxl', 'calamine']
        if content[:8] == b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1':  # xls = OLE2
            return ['xlrd', 'calamine']
        return ['openpyxl', 'xlrd', 'calamine']

    def _download_jpx_data(self):
        """JPX 銘柄一覧を取得（URL・拡張子の変更に自動追従）"""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': JPX_PAGE_URL,
        }

        for url in self._resolve_jpx_urls():
            try:
                r = requests.get(url, headers=headers, timeout=60)
                print(f"  試行 {url.split('/')[-1]} -> HTTP {r.status_code}, {len(r.content):,} bytes")

                if r.status_code != 200:
                    continue
                content = r.content
                if len(content) < 100_000:
                    print("    サイズ不足のためスキップ")
                    continue
                if b'<html' in content[:500].lower():
                    print("    HTMLが返却されたためスキップ")
                    continue

                for eng in self._engines_for(content):
                    try:
                        df = pd.read_excel(io.BytesIO(content), engine=eng, dtype=str)
                        if len(df) > 1000:
                            print(f"JPX Excel解析成功: {len(df)} 行 (engine={eng})")
                            return df
                        print(f"    {eng}: 行数不足 {len(df)}")
                    except ImportError:
                        print(f"    {eng}: 未インストール")
                    except Exception as e:
                        print(f"    {eng}: 解析失敗 {e}")

            except Exception as e:
                print(f"  {url} 取得失敗: {e}")

        return None

    def _extract_symbols(self, df):
        """銘柄コード抽出 - 英字銘柄対応 + 市場区分・業種取得"""
        print(f"銘柄コード抽出開始: {df.shape}")
        print(f"列名: {list(df.columns)}")

        symbols = {}

        code_col = name_col = market_col = None
        sector_33_col = sector_17_col = size_col = None

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

        print("\n検出された列:")
        print(f"  コード列: {code_col}")
        print(f"  銘柄名列: {name_col}")
        print(f"  市場区分列: {market_col}")
        print(f"  33業種区分列: {sector_33_col}")
        print(f"  17業種区分列: {sector_17_col}")
        print(f"  規模区分列: {size_col}")

        # 列レイアウト変更に早期に気づけるよう fail-fast
        if code_col is None:
            raise Exception(f"コード列を検出できません。列名: {list(df.columns)}")

        def cell(row, col):
            if col and pd.notna(row[col]):
                return str(row[col]).strip()
            return 'N/A'

        for _, row in df.iterrows():
            try:
                code_value = str(row[code_col]).strip()
                if not code_value or code_value == 'nan':
                    continue

                is_numeric4 = code_value.isdigit() and len(code_value) == 4 and 1000 <= int(code_value) <= 9999
                is_alnum = bool(re.match(r'^\d{3,4}[A-Z]{1,2}$', code_value))

                if not (is_numeric4 or is_alnum):
                    continue

                symbols[f"{code_value}.T"] = {
                    'code': code_value,
                    'name': cell(row, name_col),
                    'market': cell(row, market_col),
                    'sector_33': cell(row, sector_33_col),
                    'sector_17': cell(row, sector_17_col),
                    'size': cell(row, size_col),
                }
            except Exception:
                continue

        result = sorted(symbols.keys())

        alphabetic = [s for s in result if re.search(r'[A-Z]', s.replace('.T', ''))]
        markets = {}
        for info in symbols.values():
            markets[info['market']] = markets.get(info['market'], 0) + 1

        print(f"\n最終抽出結果: 合計 {len(result)} 銘柄")
        print(f"  数値のみ: {len(result) - len(alphabetic)} 銘柄")
        if alphabetic:
            print(f"  英字付き: {len(alphabetic)} 銘柄")

        print("\n市場区分別:")
        for market, count in sorted(markets.items()):
            print(f"  {market}: {count}銘柄")

        if result:
            print("\n抽出例:")
            for symbol in result[:5]:
                info = symbols[symbol]
                print(f"  {symbol}: {info['name']} | {info['market']} | {info['sector_17']}")

        self.symbol_metadata = symbols
        return result

    # -------------------------------------------------------------- 株価取得

    def get_stock_data_safe(self, symbol):
        """安全な株価データ取得（配当日・動的時価総額・追加指標対応）"""
        try:
            time.sleep(self.config["request_delay"])

            ticker = yf.Ticker(symbol)
            data = ticker.history(period="5y")

            if data.empty or len(data) < 100:
                return None

            # 注意: 累積VWAP（5年通算）。日次指標として使う場合は rolling に置換すること
            typical_price = (data['High'] + data['Low'] + data['Close']) / 3
            data['VWAP'] = (typical_price * data['Volume']).cumsum() / data['Volume'].cumsum()

            try:
                info = ticker.info
                shares_outstanding = info.get('sharesOutstanding', None)

                # 注意: history() は auto_adjust=True のため終値は調整後。
                # 現在の株数を掛けた過去時価総額は概算値である点に留意。
                if shares_outstanding:
                    data['時価総額'] = data['Close'] * shares_outstanding
                else:
                    current_market_cap = info.get('marketCap', None)
                    if current_market_cap:
                        current_price = data['Close'].iloc[-1]
                        estimated_shares = current_market_cap / current_price
                        data['時価総額'] = data['Close'] * estimated_shares
                    else:
                        data['時価総額'] = np.nan  # None だと object dtype になり mean() が落ちる

                if len(data) >= 252:
                    week_52_high = data['High'].tail(252).max()
                    week_52_low = data['Low'].tail(252).min()
                else:
                    week_52_high = None
                    week_52_low = None

                company_info = {
                    'name': info.get('longName', info.get('shortName', 'N/A')),
                    'sector': info.get('sector', 'N/A'),
                    'industry': info.get('industry', 'N/A'),
                    'market_cap': info.get('marketCap', None),
                    'shares_outstanding': shares_outstanding,
                    'trailing_pe': info.get('trailingPE', None),
                    'price_to_book': info.get('priceToBook', None),
                    'beta': info.get('beta', None),
                    'dividend_yield': info.get('dividendYield', None),
                    'week_52_high': week_52_high,
                    'week_52_low': week_52_low,
                }
            except Exception:
                data['時価総額'] = np.nan
                company_info = {
                    'name': 'N/A', 'sector': 'N/A', 'industry': 'N/A',
                    'market_cap': None, 'shares_outstanding': None,
                    'trailing_pe': None, 'price_to_book': None, 'beta': None,
                    'dividend_yield': None, 'week_52_high': None, 'week_52_low': None,
                }

            # 配当データ
            data['配当金額'] = 0.0
            try:
                dividends = ticker.dividends
                if dividends is not None and len(dividends) > 0:
                    div_index = dividends.index
                    # タイムゾーン不一致で get_indexer が落ちるのを防ぐ
                    if getattr(div_index, 'tz', None) is not None and getattr(data.index, 'tz', None) is not None:
                        div_index = div_index.tz_convert(data.index.tz)
                    dividends = pd.Series(dividends.values, index=div_index)

                    recent = dividends[dividends.index >= data.index[0]]
                    for div_date, div_amount in recent.items():
                        pos = data.index.get_indexer([div_date], method='nearest')[0]
                        data.iloc[pos, data.columns.get_loc('配当金額')] = div_amount
            except Exception:
                pass

            return {'price_data': data, 'company_info': company_info}

        except Exception as e:
            if "Too Many Requests" not in str(e) and "Rate limited" not in str(e):
                print(f"{symbol} エラー: {e}")
            return None

    # ------------------------------------------------------------ アップロード

    def upload_chunk_to_s3(self, chunk_symbols):
        """チャンクをS3アップロード"""
        if not self.s3_client:
            return 0, []

        uploaded = 0
        failed = []

        for symbol in chunk_symbols:
            if symbol not in self.stock_data:
                continue
            try:
                clean_symbol = symbol.replace('.T', '')
                price_data = self.stock_data[symbol]['price_data']

                jp_data = price_data.rename(columns={
                    'Open': '始値', 'High': '高値', 'Low': '安値',
                    'Close': '終値', 'Volume': '出来高', 'VWAP': 'VWAP',
                })

                csv_buffer = io.StringIO()
                jp_data.to_csv(csv_buffer, encoding='utf-8-sig')

                self.s3_client.put_object(
                    Bucket="m-s3storage",
                    Key=f"japan-stocks-5years-chart/stocks/{clean_symbol}.csv",
                    Body=csv_buffer.getvalue().encode('utf-8'),
                    ContentType='text/csv',
                )
                uploaded += 1
            except Exception as e:
                failed.append((symbol, str(e)))

        if failed:
            print(f"  S3アップロード失敗: {len(failed)} 件 (例: {failed[0]})")

        return uploaded, failed

    # -------------------------------------------------------------- 収集本体

    def collect_all_stocks(self):
        """全銘柄収集"""
        if not self.jpx_symbols:
            print("銘柄リストなし")
            return False

        total = len(self.jpx_symbols)
        print(f"データ収集開始: {total} 銘柄")
        print(f"レート制限対策: {self.config['request_delay']}秒間隔, {self.config['max_workers']}並列")

        chunk_size = self.config["chunk_size"]
        chunks = [self.jpx_symbols[i:i + chunk_size] for i in range(0, total, chunk_size)]

        success_count = 0
        total_uploaded = 0

        for chunk_idx, chunk in enumerate(chunks):
            print(f"\nチャンク {chunk_idx + 1}/{len(chunks)} 処理中 ({len(chunk)} 銘柄)")

            chunk_success = 0
            chunk_start_time = time.time()

            with tqdm(total=len(chunk), desc=f"チャンク{chunk_idx + 1}") as pbar:
                with ThreadPoolExecutor(max_workers=self.config["max_workers"]) as executor:
                    futures = {executor.submit(self.get_stock_data_safe, s): s for s in chunk}

                    for future in as_completed(futures):
                        symbol = futures[future]
                        result = future.result()

                        with self.lock:
                            if result:
                                self.stock_data[symbol] = result
                                success_count += 1
                                chunk_success += 1
                            else:
                                self.failed_symbols.append(symbol)

                        pbar.update(1)
                        done = success_count + len(self.failed_symbols)
                        pbar.set_postfix({
                            'チャンク成功': chunk_success,
                            '総成功': success_count,
                            '成功率': f"{success_count / done * 100:.1f}%",
                        })

            chunk_time = time.time() - chunk_start_time

            if self.s3_client and chunk_success > 0:
                uploaded, _ = self.upload_chunk_to_s3(chunk)
                total_uploaded += uploaded
                print(f"S3アップロード完了: {uploaded} ファイル (累計: {total_uploaded})")

            remaining_chunks = len(chunks) - chunk_idx - 1
            eta = remaining_chunks * (chunk_time + self.config["chunk_delay"]) / 60
            print(f"チャンク{chunk_idx + 1}完了: {chunk_success}/{len(chunk)} 成功")
            print(f"残り推定時間: {eta:.1f} 分")

            if chunk_idx < len(chunks) - 1:
                print(f"休憩中... {self.config['chunk_delay']} 秒")
                time.sleep(self.config["chunk_delay"])

        done = success_count + len(self.failed_symbols)
        print("\n収集完了:")
        print(f"  成功: {success_count} 銘柄")
        print(f"  失敗: {len(self.failed_symbols)} 銘柄")
        print(f"  成功率: {success_count / done * 100:.1f}%")
        print(f"  S3アップロード: {total_uploaded} ファイル")

        if self.s3_client and self.stock_data:
            self._save_summary_to_s3()

        return success_count > 0

    # ---------------------------------------------------------------- サマリー

    def _build_summary_row(self, symbol, data_info):
        """1銘柄分のサマリー行を作成（失敗しても他銘柄に波及させない）"""
        price_data = data_info.get('price_data')
        company_info = data_info.get('company_info', {})
        jpx_meta = self.symbol_metadata.get(symbol, {})

        if price_data is None or len(price_data) < 100:
            return None

        first_price = price_data['Close'].iloc[0]
        latest_price = price_data['Close'].iloc[-1]
        total_return = (latest_price / first_price - 1) * 100

        returns = price_data['Close'].pct_change()
        returns = returns.replace([np.inf, -np.inf], np.nan).dropna()

        if len(returns) > 50:
            volatility = returns.std() * np.sqrt(252)
            annual_return = returns.mean() * 252
            sharpe_ratio = annual_return / volatility if volatility > 0 else None
            cumulative = (1 + returns).cumprod()
            drawdown = (cumulative - cumulative.cummax()) / cumulative.cummax()
            max_drawdown = drawdown.min()
        else:
            volatility = annual_return = sharpe_ratio = max_drawdown = None

        trading_value = price_data['Close'] * price_data['Volume']

        dividend_rows = price_data[price_data['配当金額'] > 0]
        dividend_count = len(dividend_rows)
        total_dividends = dividend_rows['配当金額'].sum()

        # yfinance のバージョンで 0.025 / 2.5 の両方があるため正規化
        dy = company_info.get('dividend_yield')
        if dy is not None:
            dividend_yield = dy * 100 if dy < 1 else dy
        elif total_dividends > 0 and latest_price > 0:
            dividend_yield = (total_dividends / 5 / latest_price) * 100
        else:
            dividend_yield = None

        def safe_int(v):
            return int(v) if pd.notna(v) else None

        return {
            '銘柄コード': symbol.replace('.T', ''),
            '会社名': jpx_meta.get('name', company_info.get('name', 'N/A')),

            '市場区分': jpx_meta.get('market', 'N/A'),
            '33業種区分': jpx_meta.get('sector_33', 'N/A'),
            '17業種区分': jpx_meta.get('sector_17', 'N/A'),
            '規模区分': jpx_meta.get('size', 'N/A'),

            'セクター(YF)': company_info.get('sector', 'N/A'),
            '業種(YF)': company_info.get('industry', 'N/A'),

            '期間開始': str(price_data.index[0].date()),
            '期間終了': str(price_data.index[-1].date()),
            'データ日数': len(price_data),

            '開始価格': round(first_price, 2),
            '最新価格': round(latest_price, 2),
            '最高値': round(price_data['High'].max(), 2),
            '最安値': round(price_data['Low'].min(), 2),
            '52週高値': company_info.get('week_52_high'),
            '52週安値': company_info.get('week_52_low'),

            '5年変化率(%)': round(total_return, 2),
            '年率リターン(%)': round(annual_return * 100, 2) if annual_return is not None else None,

            'ボラティリティ(%)': round(volatility * 100, 2) if volatility is not None else None,
            'シャープレシオ': round(sharpe_ratio, 2) if sharpe_ratio is not None else None,
            '最大ドローダウン(%)': round(max_drawdown * 100, 2) if max_drawdown is not None else None,

            'PER': company_info.get('trailing_pe'),
            'PBR': company_info.get('price_to_book'),
            'ベータ': company_info.get('beta'),

            '平均出来高': safe_int(price_data['Volume'].mean()),
            '中央値出来高': safe_int(price_data['Volume'].median()),
            '平均売買代金': safe_int(trading_value.mean()),
            '中央値売買代金': safe_int(trading_value.median()),

            '配当回数': dividend_count,
            '総配当額': round(total_dividends, 2),
            '配当利回り(%)': round(dividend_yield, 2) if dividend_yield else None,

            '最新時価総額': price_data['時価総額'].iloc[-1],
            '平均時価総額': price_data['時価総額'].mean(),

            '上昇日数': int((returns > 0).sum()),
            '下落日数': int((returns < 0).sum()),
            '勝率(%)': round((returns > 0).mean() * 100, 2),
            '価格出来高相関': price_data['Close'].corr(price_data['Volume']),
        }

    def _save_summary_to_s3(self):
        """サマリーCSVをS3に保存（1銘柄の失敗で全損しないよう分離）"""
        summary_data = []
        skipped = []

        for symbol, data_info in self.stock_data.items():
            try:
                row = self._build_summary_row(symbol, data_info)
                if row:
                    summary_data.append(row)
            except Exception as e:
                skipped.append((symbol, str(e)))

        if skipped:
            print(f"サマリー生成スキップ: {len(skipped)} 銘柄 (例: {skipped[0]})")

        if not summary_data:
            print("サマリー生成対象なし")
            return

        try:
            summary_df = pd.DataFrame(summary_data)
            csv_data = summary_df.to_csv(index=False, encoding='utf-8-sig')

            self.s3_client.put_object(
                Bucket="m-s3storage",
                Key="japan-stocks-5years-chart/summary.csv",
                Body=csv_data.encode('utf-8'),
                ContentType='text/csv',
            )
            print(f"サマリー保存完了: {len(summary_df)} 銘柄")
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
        raise Exception("銘柄取得失敗")

    collector.jpx_symbols = symbols
    print(f"対象銘柄: {len(symbols)} 件")

    if not collector.setup_s3():
        raise Exception("S3接続失敗")

    estimated_time = (len(symbols) / collector.config["chunk_size"]) * (
        collector.config["chunk_size"] * collector.config["request_delay"] / collector.config["max_workers"]
        + collector.config["chunk_delay"]
    ) / 60

    print("\n実行確認:")
    print(f"  対象銘柄数: {len(symbols):,}")
    print(f"  推定処理時間: {estimated_time:.1f} 分（ticker.info の分だけ実測はこれより長くなります）")

    start_time = time.time()
    success = collector.collect_all_stocks()
    elapsed = time.time() - start_time

    print(f"\n処理完了: {elapsed / 60:.1f} 分")
    print("S3保存先: s3://m-s3storage/japan-stocks-5years-chart/stocks/")

    if not success:
        raise Exception("収集件数ゼロ")

    return collector


if __name__ == "__main__":
    detail = ""
    try:
        collector = run_safe_collection()
        ok = len(collector.stock_data) if collector else 0
        ng = len(collector.failed_symbols) if collector else 0
        status = f"✅ 成功: {ok} 銘柄取得 / {ng} 銘柄失敗"
        color = "good"
    except Exception as e:
        status = f"❌ 失敗: {e}"
        color = "danger"
        import traceback
        traceback.print_exc()

    slack_webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if slack_webhook_url:
        message = {
            "attachments": [{
                "color": color,
                "title": "データ収集完了",
                "text": status,
                "footer": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }]
        }
        try:
            requests.post(slack_webhook_url, json=message, timeout=30)
        except Exception as e:
            print(f"Slack通知失敗: {e}")
    else:
        print("警告: SLACK_WEBHOOK_URLが設定されていません")
