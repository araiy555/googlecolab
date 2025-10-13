#!/usr/bin/env python3
"""
市場指標データ収集システム＋株価変動理由づけ
日経平均、日経225先物、VIX、金利、商品市場、為替、米国株指数、ビットコインを取得し、
S3に保存、株価変動理由を自動分析
"""

import boto3
import json
import os
from datetime import datetime, timedelta
import yfinance as yf

class MarketIndicatorsCollector:
    def __init__(self):
        """市場指標収集システム初期化"""
        print("市場指標収集システム初期化中...")

        self.bucket_name = "m-s3storage"
        self.s3_prefix = "japan-stocks-5years-chart/market-indicators/"

        # Yahoo Financeティッカー
        self.tickers = {
            "日経平均": "^N225",
            "日経225先物": "NIY=F",
            "VIX": "^VIX",
            "米国10年国債利回り": "^TNX",
            "WTI原油": "CL=F",
            "金価格": "GC=F",
            "USDJPY": "JPY=X",
            "S&P500": "^GSPC",
            "BTC-USD": "BTC-USD"  # ビットコイン追加
        }

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
        start = end - timedelta(days=365*years)
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

    def analyze_stock_movement(self, latest_nikkei, indicators_latest):
        """株価変動理由づけ分析（簡易）"""
        reason = []
        # 前日比
        prev_close = latest_nikkei['close']
        today_close = latest_nikkei['close']
        diff = today_close - prev_close
        pct_change = round((diff / prev_close) * 100, 2) if prev_close != 0 else 0

        if diff > 0:
            reason.append(f"日経平均は前日比上昇 {pct_change}%")
        elif diff < 0:
            reason.append(f"日経平均は前日比下落 {pct_change}%")
        else:
            reason.append("日経平均は前日比横ばい")

        # 為替影響
        usd_jpy = indicators_latest.get("USDJPY", {}).get('close', None)
        if usd_jpy:
            if usd_jpy > 150:
                reason.append("ドル円高 → 輸出企業にとって株価下押し要因")
            else:
                reason.append("ドル円安 → 輸出企業にとって株価上押し要因")

        # 米国株影響
        sp500 = indicators_latest.get("S&P500", {}).get('close', None)
        if sp500:
            reason.append("米国株(S&P500)動向も市場心理に影響")

        # VIX影響
        vix = indicators_latest.get("VIX", {}).get('fear_level', None)
        if vix:
            reason.append(f"VIX: {vix} → 投資家心理を反映")

        # 商品・金利影響
        oil = indicators_latest.get("WTI原油", {}).get('close', None)
        gold = indicators_latest.get("金価格", {}).get('close', None)
        us10y = indicators_latest.get("米国10年国債利回り", {}).get('close', None)

        if oil and oil > 100:
            reason.append("原油高 → 企業コスト上昇で株価下押し要因")
        if gold and gold > 2000:
            reason.append("金高 → リスク回避資金流入の可能性")
        if us10y and us10y > 4:
            reason.append("米国金利高 → 株式売り圧力")

        # ビットコイン影響
        btc = indicators_latest.get("BTC-USD", {}).get('close', None)
        if btc and btc > 50000:
            reason.append("ビットコイン高 → 暗号資産関連企業株価押し上げ要因")

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
        print("="*60)
        print(f"市場指標データ収集開始（過去{years}年）")
        print("="*60)

        results = {}
        for name, ticker in self.tickers.items():
            results[name] = self.fetch_indicator(name, ticker, years)

        # ベーシス計算
        if results.get("日経平均") and results.get("日経225先物"):
            results["ベーシス"] = self.calculate_basis(results["日経平均"], results["日経225先物"])

        today = datetime.now().strftime('%Y-%m-%d')
        complete_data = {
            'collection_date': today,
            'years': years,
            'data': results,
            'latest_values': {}
        }

        # 最新値追加
        for name, data in results.items():
            if data:
                latest = data[-1]
                complete_data['latest_values'][name] = latest

        # 株価変動理由づけ分析
        if "日経平均" in complete_data['latest_values']:
            latest_nikkei = complete_data['latest_values']["日経平均"]
            reasons = self.analyze_stock_movement(latest_nikkei, complete_data['latest_values'])
            complete_data['stock_movement_reasons'] = reasons

        # S3保存
        self.save_to_s3(f"complete-market-data-{today}.json", complete_data)

        print("\n収集完了サマリー")
        print("="*60)
        for name, data in results.items():
            print(f"{name}: {len(data)}日分")
        if "stock_movement_reasons" in complete_data:
            print("\n株価変動理由:")
            for r in complete_data['stock_movement_reasons']:
                print(f" - {r}")
        print("="*60)
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
        
        notify_slack("success", f"市場指標データ収集が完了しました\nデータ保存先: {s3_path}")
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        notify_slack("failure", f"エラーが発生しました: {str(e)}")

if __name__ == "__main__":
    main()
