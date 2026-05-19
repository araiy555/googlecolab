#!/usr/bin/env python3
"""
市場指標データ収集システム＋株価変動理由づけ
日経平均、日経225先物、VIX、金利、商品市場（金・WTI・ブレント・銅・天然ガス）、
為替、米国株指数、ビットコイン、経済指標（貿易収支・消費者信頼感・住宅着工）を取得し、
S3に保存、株価変動理由を自動分析
"""

import boto3
import json
import os
from datetime import datetime, timedelta
import yfinance as yf
import requests


class MarketIndicatorsCollector:
    def __init__(self):
        """市場指標収集システム初期化"""
        print("市場指標収集システム初期化中...")

        self.bucket_name = "m-s3storage"
        self.s3_prefix = "japan-stocks-5years-chart/market-indicators/"

        # Yahoo Financeティッカー（日次OHLCデータ）
        self.tickers = {
            "日経平均": "^N225",
            "日経225先物": "NIY=F",
            "VIX": "^VIX",
            "米国10年国債利回り": "^TNX",
            "WTI原油": "CL=F",
            "ブレント原油": "BZ=F",
            "金価格": "GC=F",
            "銅": "HG=F",
            "天然ガス": "NG=F",
            "USDJPY": "JPY=X",
            "S&P500": "^GSPC",
            "BTC-USD": "BTC-USD",
        }

        # FRED経済指標（要 FRED_API_KEY 環境変数）
        # API key無料登録: https://fred.stlouisfed.org/docs/api/api_key.html
        self.fred_series = {
            "米国貿易収支": "BOPGSTB",        # 月次, 億ドル
            "消費者信頼感指数": "UMCSENT",     # Michigan Consumer Sentiment, 月次
            "米国住宅着工件数": "HOUST",       # 月次, 千戸 SAAR
        }

        self.fred_api_key = os.getenv("FRED_API_KEY")

        # S3クライアント
        self.s3 = self._init_s3_client()
        print("初期化完了")

    def _init_s3_client(self):
        """S3クライアント初期化"""
        try:
            os.environ['AWS_ACCESS_KEY_ID'] = os.getenv('AWS_ACCESS_KEY_ID')
            os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv('AWS_SECRET_ACCESS_KEY')
            return boto3.client('s3', region_name="ap-northeast-1")
        except Exception as e:
            print(f"S3初期化失敗: {e}")
            return None

    def _get_date_range(self, years=5):
        """開始日と終了日を返す"""
        end = datetime.today()
        start = end - timedelta(days=365 * years)
        return start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')

    def _fetch_data(self, ticker_symbol, years=5):
        """Yahoo Finance からデータ取得"""
        start, end = self._get_date_range(years)
        ticker = yf.Ticker(ticker_symbol)
        hist = ticker.history(start=start, end=end, interval="1d")
        data = []
        for date, row in hist.iterrows():
            data.append({
                'date': date.strftime('%Y-%m-%d'),
                'open': round(float(row['Open']), 4),
                'high': round(float(row['High']), 4),
                'low': round(float(row['Low']), 4),
                'close': round(float(row['Close']), 4),
                'volume': int(row['Volume'])
            })
        return data

    def fetch_indicator(self, name, ticker, years=5):
        """汎用インジケータ取得"""
        print(f"{name} データ取得中（過去{years}年）...")
        try:
            data = self._fetch_data(ticker, years)
            if name == "VIX":
                for row in data:
                    row['fear_level'] = self._classify_fear_level(row['close'])
            print(f"✓ {name}: {len(data)}日分取得")
            return data
        except Exception as e:
            print(f"✗ {name}取得エラー: {e}")
            return []

    def fetch_fred_indicator(self, name, series_id, years=5):
        """FREDから経済指標を取得（月次・四半期データ）"""
        if not self.fred_api_key:
            print(f"⚠ FRED_API_KEY未設定のため {name} をスキップ")
            return []

        print(f"{name} (FRED: {series_id}) 取得中...")
        start, end = self._get_date_range(years)
        url = "https://api.stlouisfed.org/fred/series/observations"
        params = {
            "series_id": series_id,
            "api_key": self.fred_api_key,
            "file_type": "json",
            "observation_start": start,
            "observation_end": end,
        }

        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            observations = resp.json().get("observations", [])
            data = []
            for obs in observations:
                # FREDは欠損値を "." で返す
                if obs.get("value") in (None, "", "."):
                    continue
                try:
                    data.append({
                        "date": obs["date"],
                        "value": round(float(obs["value"]), 4)
                    })
                except (ValueError, TypeError):
                    continue
            print(f"✓ {name}: {len(data)}件取得")
            return data
        except Exception as e:
            print(f"✗ {name}取得エラー: {e}")
            return []

    def _classify_fear_level(self, vix_value):
        """VIXから恐怖レベル分類"""
        if vix_value < 12:
            return "極低ボラティリティ"
        elif vix_value < 20:
            return "低ボラティリティ"
        elif vix_value < 30:
            return "通常"
        elif vix_value < 40:
            return "高ボラティリティ"
        else:
            return "極高ボラティリティ"

    def calculate_basis(self, nikkei_data, futures_data):
        """日経平均と先物のベーシス計算"""
        print("ベーシス（先物プレミアム）計算中...")
        basis_data = []
        nikkei_dict = {d['date']: d['close'] for d in nikkei_data}
        for futures in futures_data:
            date = futures['date']
            if date in nikkei_dict:
                spot_price = nikkei_dict[date]
                futures_price = futures['close']
                basis = round(futures_price - spot_price, 2)
                basis_pct = round((basis / spot_price) * 100, 3)
                basis_data.append({
                    'date': date,
                    'spot_price': spot_price,
                    'futures_price': futures_price,
                    'basis': basis,
                    'basis_percent': basis_pct
                })
        print(f"✓ ベーシス: {len(basis_data)}日分計算完了")
        return basis_data

    def analyze_stock_movement(self, today_nikkei, prev_nikkei, indicators_latest, fred_latest):
        """株価変動理由づけ分析"""
        reason = []

        # 前日比（バグ修正: 元コードでは同日closeを参照してdiff常に0だった）
        prev_close = prev_nikkei['close']
        today_close = today_nikkei['close']
        diff = today_close - prev_close
        pct_change = round((diff / prev_close) * 100, 2) if prev_close != 0 else 0

        if diff > 0:
            reason.append(f"日経平均は前日比上昇 +{pct_change}% ({prev_close:.2f}→{today_close:.2f})")
        elif diff < 0:
            reason.append(f"日経平均は前日比下落 {pct_change}% ({prev_close:.2f}→{today_close:.2f})")
        else:
            reason.append("日経平均は前日比横ばい")

        # 為替影響
        usd_jpy = indicators_latest.get("USDJPY", {}).get('close')
        if usd_jpy:
            if usd_jpy > 150:
                reason.append(f"ドル円高({usd_jpy:.2f}) → 輸出企業に追い風")
            else:
                reason.append(f"ドル円安({usd_jpy:.2f}) → 輸入コスト軽減")

        # 米国株
        sp500 = indicators_latest.get("S&P500", {}).get('close')
        if sp500:
            reason.append(f"S&P500: {sp500:.2f} → 市場心理に影響")

        # VIX
        vix_data = indicators_latest.get("VIX", {})
        if vix_data.get('close'):
            reason.append(f"VIX: {vix_data['close']:.2f} ({vix_data.get('fear_level', '')})")

        # 商品市場
        oil = indicators_latest.get("WTI原油", {}).get('close')
        brent = indicators_latest.get("ブレント原油", {}).get('close')
        gold = indicators_latest.get("金価格", {}).get('close')
        copper = indicators_latest.get("銅", {}).get('close')
        natgas = indicators_latest.get("天然ガス", {}).get('close')
        us10y = indicators_latest.get("米国10年国債利回り", {}).get('close')

        if oil:
            tag = " → 企業コスト上昇要因" if oil > 100 else ""
            reason.append(f"WTI原油: {oil:.2f}{tag}")
        if brent:
            tag = " → エネルギーコスト警戒" if brent > 100 else ""
            reason.append(f"ブレント原油: {brent:.2f}{tag}")
        if gold:
            tag = " → リスク回避マネー流入の可能性" if gold > 2000 else ""
            reason.append(f"金: {gold:.2f}{tag}")
        if copper:
            reason.append(f"銅(Dr.Copper): {copper:.4f} → 世界景気の先行指標")
        if natgas:
            reason.append(f"天然ガス: {natgas:.2f}")
        if us10y:
            tag = " → 株式売り圧力" if us10y > 4 else ""
            reason.append(f"米国10年金利: {us10y:.2f}%{tag}")

        # ビットコイン
        btc = indicators_latest.get("BTC-USD", {}).get('close')
        if btc:
            tag = " → リスクオン傾向" if btc > 50000 else ""
            reason.append(f"BTC: ${btc:,.0f}{tag}")

        # FRED経済指標（月次）
        trade = fred_latest.get("米国貿易収支")
        if trade:
            reason.append(f"米国貿易収支: {trade['value']:.1f}億ドル ({trade['date']})")
        sentiment = fred_latest.get("消費者信頼感指数")
        if sentiment:
            reason.append(f"消費者信頼感指数(Michigan): {sentiment['value']:.1f} ({sentiment['date']})")
        housing = fred_latest.get("米国住宅着工件数")
        if housing:
            reason.append(f"米国住宅着工: {housing['value']:.0f}千戸 ({housing['date']})")

        return reason

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
                ContentType='application/json'
            )
            print(f"✓ S3保存完了: {key}")
            return True
        except Exception as e:
            print(f"✗ S3保存エラー: {e}")
            return False

    def run_collection(self, years=5):
        """データ収集実行"""
        print("=" * 60)
        print(f"市場指標データ収集開始（過去{years}年）")
        print("=" * 60)

        # --- Yahoo Finance データ収集 ---
        results = {}
        for name, ticker in self.tickers.items():
            results[name] = self.fetch_indicator(name, ticker, years)

        # ベーシス計算
        if results.get("日経平均") and results.get("日経225先物"):
            results["ベーシス"] = self.calculate_basis(
                results["日経平均"], results["日経225先物"]
            )

        # --- FRED 経済指標収集 ---
        print("\n--- FRED経済指標 ---")
        fred_results = {}
        for name, series_id in self.fred_series.items():
            fred_results[name] = self.fetch_fred_indicator(name, series_id, years)

        today = datetime.now().strftime('%Y-%m-%d')
        complete_data = {
            'collection_date': today,
            'years': years,
            'data': results,
            'fred_data': fred_results,
            'latest_values': {},
            'fred_latest_values': {},
        }

        # 最新値抽出
        for name, data in results.items():
            if data:
                complete_data['latest_values'][name] = data[-1]
        for name, data in fred_results.items():
            if data:
                complete_data['fred_latest_values'][name] = data[-1]

        # 株価変動理由づけ分析（前日比計算には2日以上のデータが必要）
        nikkei_data = results.get("日経平均", [])
        if len(nikkei_data) >= 2:
            reasons = self.analyze_stock_movement(
                nikkei_data[-1],
                nikkei_data[-2],
                complete_data['latest_values'],
                complete_data['fred_latest_values'],
            )
            complete_data['stock_movement_reasons'] = reasons

        # S3保存
        self.save_to_s3(f"complete-market-data-{today}.json", complete_data)

        # サマリー出力
        print("\n収集完了サマリー")
        print("=" * 60)
        for name, data in results.items():
            print(f"  {name}: {len(data)}日分")
        for name, data in fred_results.items():
            print(f"  [FRED] {name}: {len(data)}件")
        if "stock_movement_reasons" in complete_data:
            print("\n株価変動理由:")
            for r in complete_data['stock_movement_reasons']:
                print(f"  - {r}")
        print("=" * 60)
        return complete_data


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
            "title": f"{emoji} 市場指標データ収集",
            "text": message,
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
        collector = MarketIndicatorsCollector()
        result = collector.run_collection(years=5)

        s3_path = f"s3://{collector.bucket_name}/{collector.s3_prefix}"
        print("\n処理完了")
        print(f"データ保存先: {s3_path}")

        notify_slack(
            "success",
            f"市場指標データ収集が完了しました\nデータ保存先: {s3_path}"
        )
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        notify_slack("failure", f"エラーが発生しました: {str(e)}")


if __name__ == "__main__":
    main()
