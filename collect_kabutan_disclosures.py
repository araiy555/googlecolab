#!/usr/bin/env python3
"""
株探開示情報日次収集システム
毎日当月と前月のデータを取得・更新（データ漏れ防止）

修正内容:
  - extract_disclosures_from_page: 全テーブルのヒューリスティック解析を廃止
    → table.stock_table の固定6カラム（コード/会社名/市場/情報種別/タイトル/日時）で直接抽出
  - <time datetime="YYYY-MM-DDT..."> から正確な日付を取得
  - 銘柄コード 149A 等の英字含むコードにも対応
  - 重複除去（stock_code+date+title）
  - get_target_months: ★一時的に5年分。終わったら元に戻す
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

        # 基本設定
        self.base_url = "https://kabutan.jp"
        self.disclosure_url = "https://kabutan.jp/disclosures/"
        self.bucket_name = "m-s3storage"
        self.s3_prefix = "japan-stocks-5years-chart/monthly-disclosures/"

        # 高速取得設定
        self.delay_base = 1.0
        self.delay_variance = 0.3
        self.retry_delay = 5.0
        self.max_retries = 3
        self.max_consecutive_empty = 20
        self.session_reset_interval = 200

        # S3クライアント初期化
        self.s3 = self._init_s3_client()

        # セッション初期化
        self.session = None
        self.request_count = 0
        self._init_session()

        # 統計
        self.stats = {
            'requests': 0,
            'success': 0,
            'total_disclosures': 0,
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
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        ]

        self.session.headers.update({
            'User-Agent': random.choice(user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9',
            'Accept-Language': 'ja,en-US;q=0.7,en;q=0.3',
            'Connection': 'keep-alive',
        })

    def load_existing_month_data(self, year, month):
        """既存の月データをS3から取得"""
        if not self.s3:
            return None

        try:
            key = f"{self.s3_prefix}{year}-{month:02d}.json"
            response = self.s3.get_object(Bucket=self.bucket_name, Key=key)
            data = json.loads(response['Body'].read().decode('utf-8'))
            return data
        except:
            return None

    def fetch_month_disclosures(self, year, month):
        """指定月の全開示情報を取得"""
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

                # HTML解析
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

            # 重複除去（同じstock_code+date+titleのレコードを1つにまとめる）
            seen = set()
            unique_disclosures = []
            for d in all_disclosures:
                key = f"{d.get('stock_code')}_{d.get('date')}_{d.get('title')}"
                if key not in seen:
                    seen.add(key)
                    unique_disclosures.append(d)
            dedup_count = len(all_disclosures) - len(unique_disclosures)
            if dedup_count > 0:
                print(f"  重複除去: {dedup_count}件")

            print(f"完了: {year}年{month}月 - {len(unique_disclosures)}件")
            return unique_disclosures

        except Exception as e:
            print(f"エラー: {year}年{month}月 - {e}")
            return []

    def extract_disclosures_from_page(self, soup, year, month):
        """ページから開示情報を抽出（stock_tableの固定カラム位置で抽出）
        
        kabutan.jp の開示テーブル構造:
          <table class="stock_table">
            <thead><tr>
              <th>コード</th><th>会社名</th><th>市場</th>
              <th>情報種別</th><th>タイトル</th><th>開示日時</th>
            </tr></thead>
            <tbody><tr>
              <td>[0] コード</td>
              <th>[1] 会社名</th>
              <td>[2] 市場</td>
              <td>[3] 情報種別</td>
              <td>[4] タイトル（<a>内）</td>
              <td>[5] 日時（<time datetime="YYYY-MM-DDT...">）</td>
            </tr></tbody>
        """
        disclosures = []

        table = soup.find('table', class_='stock_table')
        if not table:
            return disclosures

        tbody = table.find('tbody')
        if not tbody:
            return disclosures

        for row in tbody.find_all('tr'):
            cells = row.find_all(['td', 'th'])
            if len(cells) < 6:
                continue

            try:
                # [0] 銘柄コード（<a>タグ内、149Aなど英字含むコードにも対応）
                code_link = cells[0].find('a')
                stock_code = code_link.get_text().strip() if code_link else cells[0].get_text().strip()
                if not stock_code:
                    continue

                # [1] 会社名
                company_name = cells[1].get_text().strip() or "不明"

                # [2] 市場
                market = cells[2].get_text().strip()

                # [3] 情報種別（HTMLから直接取得）
                info_type = cells[3].get_text().strip()

                # [4] タイトル（<a>タグ内のテキスト）
                title_link = cells[4].find('a')
                title = title_link.get_text().strip() if title_link else cells[4].get_text().strip()
                title = title or "不明"

                # [5] 開示日時（<time datetime="2026-02-06T18:05:00+09:00">）
                time_tag = cells[5].find('time')
                if time_tag and time_tag.get('datetime'):
                    date_info = time_tag['datetime'][:10]  # "2026-02-06"
                else:
                    date_text = cells[5].get_text().strip()
                    date_match = re.search(r'(\d{2})/(\d{2})/(\d{2})', date_text)
                    if date_match:
                        yy = int(date_match.group(1))
                        mm = int(date_match.group(2))
                        dd = int(date_match.group(3))
                        date_info = f"{2000+yy}-{mm:02d}-{dd:02d}"
                    else:
                        date_info = f"{year}-{month:02d}-01"

                disclosures.append({
                    'stock_code': stock_code,
                    'company_name': company_name,
                    'title': title,
                    'date': date_info,
                    'market': market,
                    'info_type': info_type,
                    'category': self.categorize_disclosure(title),
                    'source': '株探',
                    'year': year,
                    'month': month
                })

            except Exception:
                continue

        return disclosures

    def categorize_disclosure(self, title):
        """開示情報のカテゴリ分類 ★変更なし"""
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
        """月ごとのデータをS3に保存 ★変更なし"""
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

            for disclosure in disclosures:
                category = disclosure.get('category', 'その他')
                month_data['categories'][category] = month_data['categories'].get(category, 0) + 1

                company = disclosure.get('company_name', '不明')
                month_data['companies'][company] = month_data['companies'].get(company, 0) + 1

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

    def get_target_months(self):
        """対象月を取得（★一時的に5年分に変更。終わったら元に戻す）"""
        today = datetime.now()
        months = []
        for y in range(today.year - 5, today.year + 1):
            for m in range(1, 13):
                if (y, m) <= (today.year, today.month):
                    months.append((y, m))
        return months

    def run_daily_collection(self):
        """日次収集実行（当月と前月） ★変更なし"""
        print("=" * 60)
        print("株探日次開示情報収集開始")
        print("=" * 60)

        self.stats['start_time'] = time.time()

        # 対象月を取得
        target_months = self.get_target_months()
        print(f"対象月: {len(target_months)}ヶ月分 ({target_months[0][0]}年{target_months[0][1]}月 〜 {target_months[-1][0]}年{target_months[-1][1]}月)")
        print("=" * 60)

        total_new = 0

        try:
            for idx, (year, month) in enumerate(target_months, 1):
                print(f"\n[{idx}/{len(target_months)}] 処理: {year}年{month}月")

                # 既存データを取得
                existing_data = self.load_existing_month_data(year, month)
                existing_count = len(existing_data.get('disclosures', [])) if existing_data else 0

                # 最新データを取得
                new_disclosures = self.fetch_month_disclosures(year, month)

                if new_disclosures:
                    # S3に保存
                    if self.save_month_to_s3(year, month, new_disclosures):
                        new_count = len(new_disclosures)
                        diff = new_count - existing_count
                        total_new += new_count
                        print(f"  結果: {new_count}件 (前回比: {diff:+d}件)")
                else:
                    print(f"  結果: 0件")

            # 完了レポート
            elapsed = time.time() - self.stats['start_time']
            print("\n" + "=" * 60)
            print("収集完了")
            print(f"総開示件数: {total_new}件")
            print(f"処理時間: {elapsed:.1f}秒")
            print("=" * 60)

            return True

        except Exception as e:
            import traceback
            print(f"エラー: {e}")
            traceback.print_exc()
            return False


def notify_slack(status, message):
    """Slackに通知を送る ★変更なし"""
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
    """メイン実行関数"""
    import traceback
    try:
        collector = KabutanDailyCollector()
        collector.run_daily_collection()
        print("日次データ収集が完了しました")
        notify_slack("success", "日次データ収集が正常に完了しました")
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        traceback.print_exc()
        notify_slack("failure", f"エラーが発生しました: {str(e)}")

if __name__ == "__main__":
    main()
