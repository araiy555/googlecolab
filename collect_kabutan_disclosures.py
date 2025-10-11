#!/usr/bin/env python3
"""
株探5年分開示情報収集システム（月ごとS3保存版）
https://kabutan.jp/disclosures/?kubun=&date=YYYYMM00 から月ごとデータ取得
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

class KabutanMonthlyCollector:
    def __init__(self):
        """月ごと収集システム初期化"""
        print("月ごと収集システム初期化中...")

        # 基本設定
        self.base_url = "https://kabutan.jp"
        self.disclosure_url = "https://kabutan.jp/disclosures/"
        self.bucket_name = "m-s3storage"
        self.s3_prefix = "japan-stocks-5years-chart/monthly-disclosures/"

        # 高速取得設定
        self.delay_base = 1.0  # 基本間隔1秒
        self.delay_variance = 0.3  # ±0.3秒
        self.retry_delay = 5.0  # リトライ間隔
        self.max_retries = 3  # リトライ回数
        self.max_consecutive_empty = 20  # 連続空ページ
        self.session_reset_interval = 200

        # S3クライアント初期化
        self.s3 = self._init_s3_client()
        self.existing_months = set()

        # セッション初期化
        self.session = None
        self.request_count = 0
        self._init_session()

        # 統計
        self.stats = {
            'requests': 0,
            'success': 0,
            'retries': 0,
            'total_disclosures': 0,
            'skipped_months': 0,
            'start_time': None
        }

        print("初期化完了")

    def _init_s3_client(self):
        """S3クライアント初期化"""
        try:
            aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
            aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')

            if aws_access_key and aws_secret_key:
                return boto3.client(
                    's3',
                    aws_access_key_id=aws_access_key,
                    aws_secret_access_key=aws_secret_key,
                    region_name="ap-northeast-1"
                )
            else:
                return boto3.client('s3', region_name="ap-northeast-1")
        except Exception as e:
            print(f"S3クライアント初期化失敗: {e}")
            return None

    def _init_session(self):
        """セッション初期化"""
        self.session = requests.Session()

        retry_strategy = Retry(
            total=2,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]

        self.session.headers.update({
            'User-Agent': random.choice(user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ja,en-US;q=0.7,en;q=0.3',
            'Connection': 'keep-alive',
        })

    def load_existing_months(self):
        """既存の月データをチェック"""
        if not self.s3:
            print("S3クライアントが利用できません")
            return

        try:
            print("既存月データ確認中...")

            paginator = self.s3.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.bucket_name, Prefix=self.s3_prefix)

            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        # ファイル名から年月を抽出 (例: 2024-01.json)
                        filename = obj['Key'].split('/')[-1]
                        if filename.endswith('.json') and len(filename) == 12:  # YYYY-MM.json
                            year_month = filename[:-5]  # .json を除去
                            self.existing_months.add(year_month)

            print(f"既存月データ: {len(self.existing_months)}ヶ月")

        except Exception as e:
            print(f"既存データ確認エラー: {e}")

    def fetch_month_disclosures(self, year, month):
        """指定月の全開示情報を取得"""
        all_disclosures = []
        date_param = f"{year}{month:02d}00"

        print(f"取得開始: {year}年{month}月 (date={date_param})")

        try:
            page = 1
            consecutive_empty = 0

            while consecutive_empty < self.max_consecutive_empty:
                # セッションリセット判定
                if self.request_count >= self.session_reset_interval:
                    print("  セッションリセット")
                    self._init_session()
                    self.request_count = 0

                # URLパラメータ設定
                url = f"{self.disclosure_url}?kubun=&date={date_param}&page={page}"

                success = False
                for retry in range(self.max_retries):
                    try:
                        print(f"  ページ{page}", end='')

                        # ランダム遅延
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
                            print(" - レート制限")
                            time.sleep(10)
                            self.stats['retries'] += 1
                        else:
                            print(f" - HTTPエラー: {response.status_code}")
                            time.sleep(self.retry_delay)
                            self.stats['retries'] += 1

                    except Exception as e:
                        print(f" - エラー: {e}")
                        time.sleep(self.retry_delay)
                        self.stats['retries'] += 1

                if not success:
                    consecutive_empty += 1
                    page += 1
                    print(" - 失敗")
                    continue

                # HTML解析
                soup = BeautifulSoup(response.content, 'html.parser')
                page_disclosures = self.extract_disclosures_from_page(soup, year, month)

                if page_disclosures:
                    consecutive_empty = 0
                    all_disclosures.extend(page_disclosures)
                    print(f" - {len(page_disclosures)}件")
                else:
                    consecutive_empty += 1
                    print(" - 0件")

                page += 1

                # 上限チェック
                if page > 1000:
                    print(f"  最大ページ数到達: {page-1}")
                    break

            print(f"完了: {year}年{month}月 - {len(all_disclosures)}件")
            return all_disclosures

        except Exception as e:
            print(f"エラー: {year}年{month}月 - {e}")
            return []

    def extract_disclosures_from_page(self, soup, year, month):
        """ページから開示情報を抽出"""
        disclosures = []

        # テーブルから開示情報を抽出
        tables = soup.find_all('table')
        for table in tables:
            rows = table.find_all('tr')

            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 3:  # 最低3列必要
                    row_data = self.parse_disclosure_row(cells, year, month)
                    if row_data:
                        disclosures.append(row_data)

        # リストから開示情報を抽出（テーブル以外）
        disclosure_items = soup.find_all(['div', 'li'], class_=re.compile(r'disclosure|item|news'))
        for item in disclosure_items:
            item_data = self.parse_disclosure_item(item, year, month)
            if item_data:
                disclosures.append(item_data)

        return disclosures

    def parse_disclosure_row(self, cells, year, month):
        """テーブル行から開示情報をパース"""
        try:
            texts = [cell.get_text().strip() for cell in cells]

            # 株式コードを検索
            stock_code = None
            date_info = None
            title = None
            company_name = None

            for i, text in enumerate(texts):
                # 株式コード検索（4桁数字）
                if not stock_code:
                    codes = re.findall(r'\b(\d{4})\b', text)
                    for code in codes:
                        if 1000 <= int(code) <= 9999:
                            stock_code = code
                            break

                # 日付情報検索
                if not date_info:
                    date_matches = re.findall(r'(\d{1,2})/(\d{1,2})', text)
                    if date_matches:
                        month_day = date_matches[0]
                        date_info = f"{year}-{month:02d}-{int(month_day[1]):02d}"

                # 会社名とタイトル
                if len(text) > 5 and not re.match(r'^\d+$', text):
                    if not company_name and stock_code:
                        company_name = text
                    elif not title:
                        title = text

            if stock_code:
                return {
                    'stock_code': stock_code,
                    'company_name': company_name or "不明",
                    'title': title or "不明",
                    'date': date_info or f"{year}-{month:02d}-01",
                    'category': self.categorize_disclosure(title or ""),
                    'source': '株探',
                    'year': year,
                    'month': month
                }

        except Exception:
            pass

        return None

    def parse_disclosure_item(self, item, year, month):
        """アイテムから開示情報をパース"""
        try:
            text = item.get_text().strip()

            # 株式コード検索
            codes = re.findall(r'\b(\d{4})\b', text)
            stock_code = None
            for code in codes:
                if 1000 <= int(code) <= 9999:
                    stock_code = code
                    break

            if stock_code and len(text) > 10:
                return {
                    'stock_code': stock_code,
                    'company_name': "抽出中",
                    'title': text[:200],
                    'date': f"{year}-{month:02d}-01",
                    'category': self.categorize_disclosure(text),
                    'source': '株探',
                    'year': year,
                    'month': month
                }

        except Exception:
            pass

        return None

    def categorize_disclosure(self, title):
        """開示情報のカテゴリ分類"""
        title_lower = title.lower()

        if any(word in title for word in ['決算', '業績', '四半期', '売上', '利益']):
            return '決算・業績'
        elif any(word in title for word in ['配当', '株主優待', '自己株式']):
            return '配当・株主還元'
        elif any(word in title for word in ['人事', '役員', '代表取締役']):
            return '人事・組織'
        elif any(word in title for word in ['買収', 'M&A', '資本提携', '業務提携']):
            return 'M&A・提携'
        elif any(word in title for word in ['新製品', '新サービス', '開発', '特許']):
            return '事業・製品'
        else:
            return 'その他'

    def save_month_to_s3(self, year, month, disclosures):
        """月ごとのデータをS3に保存"""
        if not self.s3:
            print("S3クライアントが利用できません")
            return False

        try:
            # データ構造
            month_data = {
                'year': year,
                'month': month,
                'total_disclosures': len(disclosures),
                'disclosures': disclosures,
                'categories': {},
                'companies': {},
                'updated_at': datetime.now().isoformat()
            }

            # カテゴリ別集計
            for disclosure in disclosures:
                category = disclosure.get('category', 'その他')
                month_data['categories'][category] = month_data['categories'].get(category, 0) + 1

                company = disclosure.get('company_name', '不明')
                month_data['companies'][company] = month_data['companies'].get(company, 0) + 1

            # S3に保存
            key = f"{self.s3_prefix}{year}-{month:02d}.json"

            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=json.dumps(month_data, ensure_ascii=False, indent=2).encode('utf-8'),
                ContentType='application/json'
            )

            print(f"S3保存完了: {key}")
            return True

        except Exception as e:
            print(f"S3保存エラー: {e}")
            return False

    def run_five_year_monthly_collection(self):
        """5年分月ごとデータ収集実行"""
        print("=" * 60)
        print("株探5年分月ごと開示情報収集開始")
        print("=" * 60)

        self.stats['start_time'] = time.time()

        # 既存データチェック
        self.load_existing_months()

        # 5年分の月リスト生成（2020年1月〜2025年9月）
        current = datetime.now()
        months = []

        for year in range(2020, current.year + 1):
            start_month = 1
            end_month = 12
            if year == current.year:
                end_month = current.month

            for month in range(start_month, end_month + 1):
                months.append((year, month))

        print(f"対象期間: {months[0][0]}年{months[0][1]}月 ～ {months[-1][0]}年{months[-1][1]}月")
        print(f"総月数: {len(months)}ヶ月")
        print("=" * 60)

        # 月ごとデータ収集
        for i, (year, month) in enumerate(months):
            try:
                print(f"\n[{i+1}/{len(months)}] {year}年{month}月")

                # スキップ判定
                year_month_key = f"{year}-{month:02d}"
                if year_month_key in self.existing_months:
                    print("  → スキップ（既存データあり）")
                    self.stats['skipped_months'] += 1
                    continue

                # 月データ取得
                disclosures = self.fetch_month_disclosures(year, month)

                if disclosures:
                    # S3保存
                    if self.save_month_to_s3(year, month, disclosures):
                        self.stats['total_disclosures'] += len(disclosures)

                # 進捗表示
                elapsed = time.time() - self.stats['start_time']
                progress = (i + 1) / len(months) * 100

                print(f"  進捗: {i+1}/{len(months)} ({progress:.1f}%)")
                print(f"  月次開示: {len(disclosures)}件")
                print(f"  累計開示: {self.stats['total_disclosures']:,}件")
                print(f"  スキップ: {self.stats['skipped_months']}月")
                print(f"  経過時間: {elapsed/3600:.1f}時間")

                if i >= 2:
                    processed_months = i + 1 - self.stats['skipped_months']
                    if processed_months > 0:
                        avg_time_per_month = elapsed / processed_months
                        remaining_months = len(months) - i - 1
                        remaining_time = avg_time_per_month * remaining_months
                        print(f"  残り推定: {remaining_time/3600:.1f}時間")

                # 月間の間隔
                time.sleep(2)

            except Exception as e:
                print(f"エラー: {year}年{month}月 - {e}")
                continue

        print("\n" + "=" * 60)
        print("収集完了")
        print(f"処理月数: {len(months) - self.stats['skipped_months']}")
        print(f"スキップ月数: {self.stats['skipped_months']}")
        print(f"総開示件数: {self.stats['total_disclosures']:,}")

        elapsed = time.time() - self.stats['start_time']
        print(f"総実行時間: {elapsed/3600:.1f}時間")
        print("=" * 60)

        return True

def main():
    """メイン実行関数"""
    # 環境変数または直接設定
    os.environ['AWS_ACCESS_KEY_ID'] = os.getenv('AWS_ACCESS_KEY_ID')
    os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv('AWS_SECRET_ACCESS_KEY')

    collector = KabutanMonthlyCollector()
    collector.run_five_year_monthly_collection()

    print("月ごとデータ収集が完了しました")

if __name__ == "__main__":
    main()
