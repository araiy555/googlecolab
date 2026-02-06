#!/usr/bin/env python3
"""
株探開示情報日次収集システム（修正版）
修正点:
  1. テーブルのカラム位置ベースでパース（会社名スキップ問題を修正）
  2. 日付パースを YY/MM/DD HH:MM 形式に対応（日の精度喪失を修正）
  3. parse_disclosure_item のゴミHTML混入を防止
"""

import requests
import boto3
import json
import os
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import time
import re
import random
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class KabutanDailyCollector:
    def __init__(self):
        """日次収集システム初期化"""
        print("日次収集システム初期化中...")

        self.base_url = "https://kabutan.jp"
        self.disclosure_url = "https://kabutan.jp/disclosures/"
        self.bucket_name = "m-s3storage"
        self.s3_prefix = "japan-stocks-5years-chart/monthly-disclosures/"

        self.delay_base = 1.0
        self.delay_variance = 0.3
        self.retry_delay = 5.0
        self.max_retries = 3
        self.max_consecutive_empty = 20
        self.session_reset_interval = 200

        self.s3 = self._init_s3_client()
        self.session = None
        self.request_count = 0
        self._init_session()

        self.stats = {
            'requests': 0,
            'success': 0,
            'total_disclosures': 0,
            'start_time': None
        }

        print("初期化完了")

    def _init_s3_client(self):
        try:
            aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
            aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
            if aws_access_key and aws_secret_key:
                return boto3.client('s3',
                    aws_access_key_id=aws_access_key,
                    aws_secret_access_key=aws_secret_key,
                    region_name="ap-northeast-1")
            else:
                return boto3.client('s3', region_name="ap-northeast-1")
        except Exception as e:
            print(f"S3クライアント初期化失敗: {e}")
            return None

    def _init_session(self):
        self.session = requests.Session()
        retry_strategy = Retry(
            total=2, backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        ]
        self.session.headers.update({
            'User-Agent': random.choice(user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9',
            'Accept-Language': 'ja,en-US;q=0.7,en;q=0.3',
            'Connection': 'keep-alive',
        })

    # =========================================================================
    #  🔧 修正1: 日付パース
    # =========================================================================
    def parse_datetime_str(self, text, year):
        """
        株探の日時文字列をパース
        対応フォーマット: "YY/MM/DD HH:MM" (例: "20/04/13 15:30")
        
        Returns:
            tuple: (date_str "YYYY-MM-DD", time_str "HH:MM") or (None, None)
        """
        # YY/MM/DD HH:MM パターン
        match = re.search(r'(\d{2})/(\d{2})/(\d{2})\s+(\d{1,2}:\d{2})', text)
        if match:
            yy = int(match.group(1))
            mm = int(match.group(2))
            dd = int(match.group(3))
            time_str = match.group(4)
            
            # 2桁年 → 4桁年 (00-99 → 2000-2099)
            full_year = 2000 + yy
            
            # バリデーション
            if 1 <= mm <= 12 and 1 <= dd <= 31:
                return f"{full_year}-{mm:02d}-{dd:02d}", time_str
        
        # YY/MM/DD のみ（時刻なし）
        match = re.search(r'(\d{2})/(\d{2})/(\d{2})', text)
        if match:
            yy = int(match.group(1))
            mm = int(match.group(2))
            dd = int(match.group(3))
            full_year = 2000 + yy
            if 1 <= mm <= 12 and 1 <= dd <= 31:
                return f"{full_year}-{mm:02d}-{dd:02d}", None
        
        return None, None

    # =========================================================================
    #  🔧 修正2: テーブル行パース（カラム位置ベース）
    # =========================================================================
    def parse_disclosure_row(self, cells, year, month):
        """
        テーブル行から開示情報をパース（カラム位置ベース）
        
        株探のテーブル構造:
          [0] コード  [1] 会社名  [2] 市場  [3] 情報種別  [4] タイトル  [5] 開示日時
        
        ※ セル数が6未満の場合はヘッダー行などなのでスキップ
        """
        try:
            texts = [cell.get_text().strip() for cell in cells]

            # 6列（コード/会社名/市場/情報種別/タイトル/開示日時）を期待
            if len(texts) < 6:
                return None

            # --- カラム0: 銘柄コード ---
            stock_code_text = texts[0]
            code_match = re.match(r'^(\d{4})$', stock_code_text)
            if not code_match:
                return None
            stock_code = code_match.group(1)
            if not (1000 <= int(stock_code) <= 9999):
                return None

            # --- カラム1: 会社名 ---
            company_name = texts[1].strip() if texts[1].strip() else "不明"

            # --- カラム2: 市場（使わないが記録可） ---
            # market = texts[2].strip()

            # --- カラム3: 情報種別 ---
            info_type = texts[3].strip()

            # --- カラム4: タイトル ---
            title = texts[4].strip() if texts[4].strip() else "不明"

            # --- カラム5: 開示日時 ---
            datetime_text = texts[5].strip()
            date_str, time_str = self.parse_datetime_str(datetime_text, year)

            if not date_str:
                # 日時パースに失敗した場合、月初をフォールバック
                date_str = f"{year}-{month:02d}-01"

            # カテゴリ分類
            category = self.categorize_disclosure(title)

            return {
                'stock_code': stock_code,
                'company_name': company_name,
                'title': title,
                'date': date_str,
                'datetime': f"{date_str} {time_str}" if time_str else date_str,
                'category': category,
                'info_type': info_type,  # 情報種別も保持
                'source': '株探',
                'year': int(date_str[:4]),
                'month': int(date_str[5:7])
            }

        except Exception as e:
            # デバッグ用: パースエラーをログ
            # print(f"  parse_disclosure_row error: {e}")
            pass

        return None

    # =========================================================================
    #  🔧 修正3: parse_disclosure_item のゴミ除外
    # =========================================================================
    def parse_disclosure_item(self, item, year, month):
        """
        アイテムから開示情報をパース
        
        修正: ページネーション要素・ヘッダー行を除外
        """
        try:
            text = item.get_text().strip()

            # --- ゴミ除外フィルター ---
            # ページネーション文字列
            if any(keyword in text for keyword in ['次へ', '前へ', '＞»', '«＜']):
                return None
            
            # テーブルヘッダー
            if any(keyword in text for keyword in ['コード\n', '会社名\n', '開示日時\n']):
                return None
            
            # 短すぎるテキスト
            if len(text) < 15:
                return None

            # 銘柄コード抽出
            codes = re.findall(r'\b(\d{4})\b', text)
            stock_code = None
            for code in codes:
                if 1000 <= int(code) <= 9999:
                    stock_code = code
                    break

            if not stock_code:
                return None

            # テキストから日時を抽出試行
            date_str, time_str = self.parse_datetime_str(text, year)
            if not date_str:
                date_str = f"{year}-{month:02d}-01"

            return {
                'stock_code': stock_code,
                'company_name': "抽出中",
                'title': text[:200],
                'date': date_str,
                'category': self.categorize_disclosure(text),
                'source': '株探',
                'year': year,
                'month': month
            }

        except Exception:
            pass

        return None

    # =========================================================================
    #  ページ解析（修正: テーブルのヘッダー行を検出してカラム順を確認）
    # =========================================================================
    def extract_disclosures_from_page(self, soup, year, month):
        """ページから開示情報を抽出"""
        disclosures = []

        # メインテーブルからの抽出
        tables = soup.find_all('table')
        for table in tables:
            rows = table.find_all('tr')
            
            # ヘッダー行を検出して正しいテーブルか確認
            is_disclosure_table = False
            for row in rows:
                header_cells = row.find_all('th')
                if header_cells:
                    header_text = ' '.join(cell.get_text().strip() for cell in header_cells)
                    if 'コード' in header_text and 'タイトル' in header_text:
                        is_disclosure_table = True
                        break
            
            if not is_disclosure_table:
                continue
            
            for row in rows:
                cells = row.find_all(['td'])  # th はスキップ（データ行のみ）
                if len(cells) >= 6:
                    row_data = self.parse_disclosure_row(cells, year, month)
                    if row_data:
                        disclosures.append(row_data)

        # parse_disclosure_item はフォールバック（テーブル以外の構造用）
        # ただし、テーブルから取得できた場合はスキップ
        if not disclosures:
            disclosure_items = soup.find_all(['div', 'li'], 
                                            class_=re.compile(r'disclosure|item|news'))
            for item in disclosure_items:
                item_data = self.parse_disclosure_item(item, year, month)
                if item_data:
                    disclosures.append(item_data)

        return disclosures

    # =========================================================================
    #  以下は変更なし（カテゴリ分類、S3保存、収集ロジック等）
    # =========================================================================
    def categorize_disclosure(self, title):
        if any(w in title for w in ['決算', '業績', '四半期', '売上', '利益']):
            return '決算・業績'
        elif any(w in title for w in ['配当', '株主優待', '自己株式']):
            return '配当・株主還元'
        elif any(w in title for w in ['人事', '役員', '代表取締役']):
            return '人事・組織'
        elif any(w in title for w in ['買収', 'M&A', '資本提携', '業務提携']):
            return 'M&A・提携'
        elif any(w in title for w in ['新製品', '新サービス', '開発', '特許']):
            return '事業・製品'
        else:
            return 'その他'

    def load_existing_month_data(self, year, month):
        if not self.s3:
            return None
        try:
            key = f"{self.s3_prefix}{year}-{month:02d}.json"
            response = self.s3.get_object(Bucket=self.bucket_name, Key=key)
            return json.loads(response['Body'].read().decode('utf-8'))
        except:
            return None

    def fetch_month_disclosures(self, year, month):
        all_disclosures = []
        date_param = f"{year}{month:02d}00"
        print(f"取得開始: {year}年{month}月 (date={date_param})")

        try:
            page = 1
            consecutive_empty = 0

            while consecutive_empty < self.max_consecutive_empty:
                if self.request_count >= self.session_reset_interval:
                    self._init_session()
                    self.request_count = 0

                url = f"{self.disclosure_url}?kubun=&date={date_param}&page={page}"

                success = False
                for retry in range(self.max_retries):
                    try:
                        print(f"  ページ{page}", end='', flush=True)
                        delay = self.delay_base + random.uniform(-self.delay_variance, self.delay_variance)
                        if retry > 0:
                            delay += self.retry_delay
                        time.sleep(delay)

                        response = self.session.get(url, timeout=30)
                        self.stats['requests'] += 1
                        self.request_count += 1

                        if response.status_code == 200:
                            self.stats['success'] += 1
                            success = True
                            break
                        elif response.status_code == 429:
                            print(" - レート制限", flush=True)
                            time.sleep(10)
                        else:
                            print(f" - HTTPエラー: {response.status_code}", flush=True)
                            time.sleep(self.retry_delay)
                    except Exception as e:
                        print(f" - エラー: {e}", flush=True)
                        time.sleep(self.retry_delay)

                if not success:
                    consecutive_empty += 1
                    page += 1
                    print(" - 失敗", flush=True)
                    continue

                soup = BeautifulSoup(response.content, 'html.parser')
                page_disclosures = self.extract_disclosures_from_page(soup, year, month)

                if page_disclosures:
                    consecutive_empty = 0
                    all_disclosures.extend(page_disclosures)
                    print(f" - {len(page_disclosures)}件", flush=True)
                else:
                    consecutive_empty += 1
                    print(" - 0件", flush=True)

                page += 1
                if page > 1000:
                    print(f"  最大ページ数到達: {page-1}")
                    break

            print(f"完了: {year}年{month}月 - {len(all_disclosures)}件")
            return all_disclosures

        except Exception as e:
            print(f"エラー: {year}年{month}月 - {e}")
            return []

    def save_month_to_s3(self, year, month, disclosures):
        if not self.s3:
            print("S3クライアントが利用できません")
            return False
        try:
            month_data = {
                'year': year,
                'month': month,
                'total_disclosures': len(disclosures),
                'disclosures': disclosures,
                'categories': {},
                'companies': {},
                'updated_at': datetime.now().isoformat()
            }
            for d in disclosures:
                cat = d.get('category', 'その他')
                month_data['categories'][cat] = month_data['categories'].get(cat, 0) + 1
                comp = d.get('company_name', '不明')
                month_data['companies'][comp] = month_data['companies'].get(comp, 0) + 1

            key = f"{self.s3_prefix}{year}-{month:02d}.json"
            self.s3.put_object(
                Bucket=self.bucket_name, Key=key,
                Body=json.dumps(month_data, ensure_ascii=False, indent=2).encode('utf-8'),
                ContentType='application/json')
            print(f"S3保存完了: {key}")
            return True
        except Exception as e:
            print(f"S3保存エラー: {e}")
            return False

    def get_target_months(self):
        today = datetime.now()
        current_year = today.year
        current_month = today.month
        months = []
        if current_month == 1:
            months.append((current_year - 1, 12))
        else:
            months.append((current_year, current_month - 1))
        months.append((current_year, current_month))
        return months

    def run_daily_collection(self):
        print("=" * 60)
        print("株探日次開示情報収集開始")
        print("=" * 60)
        self.stats['start_time'] = time.time()

        target_months = self.get_target_months()
        print(f"対象月: {target_months[0][0]}年{target_months[0][1]}月 と "
              f"{target_months[1][0]}年{target_months[1][1]}月")
        print("=" * 60)

        total_new = 0
        try:
            for year, month in target_months:
                print(f"\n処理: {year}年{month}月")
                existing_data = self.load_existing_month_data(year, month)
                existing_count = len(existing_data.get('disclosures', [])) if existing_data else 0

                new_disclosures = self.fetch_month_disclosures(year, month)
                if new_disclosures:
                    if self.save_month_to_s3(year, month, new_disclosures):
                        new_count = len(new_disclosures)
                        diff = new_count - existing_count
                        total_new += new_count
                        print(f"  結果: {new_count}件 (前回比: {diff:+d}件)")
                else:
                    print("  結果: 0件")

            elapsed = time.time() - self.stats['start_time']
            print("\n" + "=" * 60)
            print("収集完了")
            print(f"総開示件数: {total_new}件")
            print(f"処理時間: {elapsed:.1f}秒")
            print("=" * 60)
            return True
        except Exception as e:
            print(f"エラー: {e}")
            return False


def notify_slack(status, message):
    slack_webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not slack_webhook_url:
        print("警告: SLACK_WEBHOOK_URLが設定されていません")
        return
    color = "good" if status == "success" else "danger"
    emoji = "✅" if status == "success" else "❌"
    payload = {
        "attachments": [{
            "color": color,
            "title": f"{emoji} 日次データ収集",
            "text": message,
            "footer": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }]
    }
    try:
        requests.post(slack_webhook_url, json=payload)
    except Exception as e:
        print(f"Slack通知エラー: {e}")


def main():
    try:
        collector = KabutanDailyCollector()
        collector.run_daily_collection()
        print("日次データ収集が完了しました")
        notify_slack("success", "日次データ収集が正常に完了しました")
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        notify_slack("failure", f"エラーが発生しました: {str(e)}")


if __name__ == "__main__":
    main()
