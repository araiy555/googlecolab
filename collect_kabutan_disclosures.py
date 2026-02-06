#!/usr/bin/env python3
"""
株探開示情報収集システム（★一時的に5年分取得モード）

HTMLテーブル構造（2026年2月時点で確認済み）:
  <table class="stock_table">
    <thead><tr>
      <th>コード</th><th>会社名</th><th>市場</th>
      <th>情報種別</th><th>タイトル</th><th>開示日時</th>
    </tr></thead>
    <tbody><tr>
      <td>[0]コード(<a>内)</td>
      <th>[1]会社名</th>
      <td>[2]市場</td>
      <td>[3]情報種別</td>
      <td>[4]タイトル(<a>内)</td>
      <td>[5]日時(<time datetime="ISO">)</td>
    </tr></tbody>

ページネーション:
  <div class="pagination">
    ...
    <li><a href="?kubun=&date=YYYYMM00&page=90">»</a></li>
  </div>
  → «»» リンクの page=N が最終ページ番号
"""

import requests
import boto3
import json
import os
import re
import time
import random
import traceback
from bs4 import BeautifulSoup
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class KabutanDailyCollector:
    def __init__(self):
        print("初期化中...")

        self.disclosure_url = "https://kabutan.jp/disclosures/"
        self.bucket_name = "m-s3storage"
        self.s3_prefix = "japan-stocks-5years-chart/monthly-disclosures/"

        # リクエスト設定
        self.delay_base = 0.7
        self.delay_variance = 0.2
        self.retry_delay = 5.0
        self.max_retries = 5
        self.session_reset_interval = 200

        # S3
        self.s3 = self._init_s3_client()

        # HTTPセッション
        self.session = None
        self.request_count = 0
        self._init_session()

        # 統計
        self.stats = {'requests': 0, 'success': 0, 'start_time': None}

        print("初期化完了")

    # ─────────────────────────────────────────────
    # インフラ
    # ─────────────────────────────────────────────

    def _init_s3_client(self):
        try:
            ak = os.getenv('AWS_ACCESS_KEY_ID')
            sk = os.getenv('AWS_SECRET_ACCESS_KEY')
            if ak and sk:
                return boto3.client('s3', aws_access_key_id=ak,
                                    aws_secret_access_key=sk,
                                    region_name="ap-northeast-1")
            else:
                return boto3.client('s3', region_name="ap-northeast-1")
        except Exception as e:
            print(f"S3初期化失敗: {e}")
            return None

    def _init_session(self):
        self.session = requests.Session()
        retry = Retry(total=2, backoff_factor=0.5,
                      status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.session.headers.update({
            'User-Agent': random.choice([
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            ]),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9',
            'Accept-Language': 'ja,en-US;q=0.7,en;q=0.3',
            'Connection': 'keep-alive',
        })
        self.request_count = 0

    # ─────────────────────────────────────────────
    # HTTP取得
    # ─────────────────────────────────────────────

    def _fetch_url(self, url):
        """URLを取得。成功→Response、失敗→None。リトライ付き"""
        for attempt in range(self.max_retries):
            try:
                delay = self.delay_base + random.uniform(-self.delay_variance, self.delay_variance)
                if attempt > 0:
                    delay += self.retry_delay * attempt
                time.sleep(delay)

                resp = self.session.get(url, timeout=30)
                self.stats['requests'] += 1
                self.request_count += 1

                # セッションリセット
                if self.request_count >= self.session_reset_interval:
                    self._init_session()

                if resp.status_code == 200:
                    self.stats['success'] += 1
                    return resp
                elif resp.status_code == 429:
                    wait = 30 * (attempt + 1)
                    print(f" [429→{wait}s待機]", end='', flush=True)
                    time.sleep(wait)
                else:
                    print(f" [HTTP{resp.status_code}]", end='', flush=True)
                    time.sleep(self.retry_delay)
            except Exception as e:
                print(f" [ERR:{e}]", end='', flush=True)
                time.sleep(self.retry_delay)

        return None

    # ─────────────────────────────────────────────
    # ページネーション解析
    # ─────────────────────────────────────────────

    def _get_total_pages(self, soup):
        """paginationの «»» リンクから最終ページ番号を取得"""
        pagination = soup.find('div', class_='pagination')
        if not pagination:
            return 1

        # «»» (raquo) リンクを探す → page=90 のような最終ページ番号
        for a in pagination.find_all('a'):
            if '»' in a.get_text():
                m = re.search(r'page=(\d+)', a.get('href', ''))
                if m:
                    return int(m.group(1))

        # «»» がない場合: ページ番号リンクの最大値
        max_p = 1
        for li in pagination.find_all('li'):
            txt = li.get_text().strip()
            if txt.isdigit():
                max_p = max(max_p, int(txt))
        return max_p

    # ─────────────────────────────────────────────
    # HTML→開示レコード抽出
    # ─────────────────────────────────────────────

    def _extract_from_page(self, soup, year, month):
        """<table class="stock_table"> の固定6カラムからレコードを抽出"""
        records = []
        table = soup.find('table', class_='stock_table')
        if not table:
            return records

        tbody = table.find('tbody')
        if not tbody:
            return records

        for row in tbody.find_all('tr'):
            cells = row.find_all(['td', 'th'])
            if len(cells) < 6:
                continue
            try:
                # [0] 銘柄コード (4060, 149A など)
                a = cells[0].find('a')
                code = a.get_text().strip() if a else cells[0].get_text().strip()
                if not code:
                    continue

                # [1] 会社名
                name = cells[1].get_text().strip() or "不明"

                # [2] 市場
                market = cells[2].get_text().strip()

                # [3] 情報種別
                info_type = cells[3].get_text().strip()

                # [4] タイトル (<a>内テキスト)
                ta = cells[4].find('a')
                title = ta.get_text().strip() if ta else cells[4].get_text().strip()
                title = title or "不明"

                # [5] 日時 (<time datetime="2026-02-06T18:05:00+09:00">)
                tt = cells[5].find('time')
                if tt and tt.get('datetime'):
                    date_str = tt['datetime'][:10]  # "2026-02-06"
                else:
                    # フォールバック: テキストから YY/MM/DD
                    raw = cells[5].get_text().strip()
                    dm = re.search(r'(\d{2})/(\d{2})/(\d{2})', raw)
                    if dm:
                        date_str = f"{2000+int(dm.group(1))}-{int(dm.group(2)):02d}-{int(dm.group(3)):02d}"
                    else:
                        date_str = f"{year}-{month:02d}-01"

                records.append({
                    'stock_code': code,
                    'company_name': name,
                    'title': title,
                    'date': date_str,
                    'market': market,
                    'info_type': info_type,
                    'category': self._categorize(title),
                    'source': '株探',
                    'year': year,
                    'month': month,
                })
            except Exception:
                continue

        return records

    def _categorize(self, title):
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
        return 'その他'

    # ─────────────────────────────────────────────
    # 月単位の全ページ取得
    # ─────────────────────────────────────────────

    def fetch_month(self, year, month):
        """1ヶ月分の開示情報を全ページ取得して返す"""
        date_param = f"{year}{month:02d}00"
        base = f"{self.disclosure_url}?kubun=&date={date_param}"

        # 1ページ目 → 総ページ数取得
        resp = self._fetch_url(f"{base}&page=1")
        if not resp:
            print("p1失敗", flush=True)
            return []

        soup = BeautifulSoup(resp.content, 'html.parser')
        total_pages = self._get_total_pages(soup)
        records = self._extract_from_page(soup, year, month)
        print(f"p1→{len(records)}件(全{total_pages}p)", end='', flush=True)

        # データが全くない月
        if total_pages <= 1 and not records:
            print(" データなし", flush=True)
            return []

        # 2ページ目以降
        consecutive_fail = 0
        for page in range(2, total_pages + 1):
            if page % 20 == 0:
                print(f" p{page}", end='', flush=True)

            resp = self._fetch_url(f"{base}&page={page}")
            if not resp:
                consecutive_fail += 1
                if consecutive_fail >= 5:
                    print(f" p{page}で5連続失敗→中断", end='', flush=True)
                    break
                continue

            page_records = self._extract_from_page(
                BeautifulSoup(resp.content, 'html.parser'), year, month)
            if page_records:
                records.extend(page_records)
                consecutive_fail = 0
            else:
                consecutive_fail += 1
                if consecutive_fail >= 5:
                    print(f" p{page}で5連続空→終了", end='', flush=True)
                    break

        # 重複除去
        seen = set()
        unique = []
        for r in records:
            key = f"{r['stock_code']}_{r['date']}_{r['title']}"
            if key not in seen:
                seen.add(key)
                unique.append(r)

        dup = len(records) - len(unique)
        dup_info = f" dup{dup}" if dup else ""
        print(f" →{len(unique)}件{dup_info}", flush=True)
        return unique

    # ─────────────────────────────────────────────
    # S3保存 / 既存チェック
    # ─────────────────────────────────────────────

    def check_existing_month(self, year, month):
        """S3に既存データがあるか確認。件数を返す（なければ0）"""
        if not self.s3:
            return 0
        try:
            key = f"{self.s3_prefix}{year}-{month:02d}.json"
            resp = self.s3.get_object(Bucket=self.bucket_name, Key=key)
            data = json.loads(resp['Body'].read().decode('utf-8'))
            return data.get('total_disclosures', 0)
        except Exception:
            return 0

    def save_to_s3(self, year, month, disclosures):
        if not self.s3:
            print(" S3なし", flush=True)
            return False
        try:
            data = {
                'year': year,
                'month': month,
                'total_disclosures': len(disclosures),
                'disclosures': disclosures,
                'categories': {},
                'companies': {},
                'updated_at': datetime.now().isoformat(),
            }
            for d in disclosures:
                cat = d.get('category', 'その他')
                data['categories'][cat] = data['categories'].get(cat, 0) + 1
                co = d.get('company_name', '不明')
                data['companies'][co] = data['companies'].get(co, 0) + 1

            key = f"{self.s3_prefix}{year}-{month:02d}.json"
            self.s3.put_object(
                Bucket=self.bucket_name, Key=key,
                Body=json.dumps(data, ensure_ascii=False, indent=2).encode('utf-8'),
                ContentType='application/json',
            )
            return True
        except Exception as e:
            print(f" S3エラー:{e}", flush=True)
            return False

    # ─────────────────────────────────────────────
    # 対象月リスト
    # ─────────────────────────────────────────────

    def get_target_months(self):
        """★一時的に5年分。終わったら下のコメントアウト版に戻す"""
        today = datetime.now()
        months = []
        for y in range(today.year - 5, today.year + 1):
            for m in range(1, 13):
                if (y, m) <= (today.year, today.month):
                    months.append((y, m))
        return months
        # ---- 通常版（5年取得が終わったらこちらに戻す）----
        # today = datetime.now()
        # months = []
        # if today.month == 1:
        #     months.append((today.year - 1, 12))
        # else:
        #     months.append((today.year, today.month - 1))
        # months.append((today.year, today.month))
        # return months

    # ─────────────────────────────────────────────
    # メインループ
    # ─────────────────────────────────────────────

    def run(self):
        print("=" * 60)
        print("株探 開示情報収集開始")
        print("=" * 60)
        self.stats['start_time'] = time.time()

        months = self.get_target_months()
        total = len(months)
        print(f"対象: {total}ヶ月 ({months[0][0]}/{months[0][1]:02d} 〜 {months[-1][0]}/{months[-1][1]:02d})")
        print("=" * 60)

        ok = 0
        ng = 0
        skipped = 0
        total_records = 0
        today = datetime.now()

        for idx, (year, month) in enumerate(months, 1):
            elapsed = time.time() - self.stats['start_time']
            print(f"[{idx}/{total}] {year}/{month:02d} ", end='', flush=True)

            # 当月以外で既にS3にデータがある月はスキップ
            is_current_month = (year == today.year and month == today.month)
            if not is_current_month:
                existing_count = self.check_existing_month(year, month)
                if existing_count >= 100:  # 100件以上あれば取得済みとみなす
                    print(f"スキップ(既存{existing_count}件)", flush=True)
                    skipped += 1
                    total_records += existing_count
                    ok += 1
                    continue

            try:
                records = self.fetch_month(year, month)
                if records and self.save_to_s3(year, month, records):
                    ok += 1
                    total_records += len(records)
                else:
                    ng += 1
            except Exception as e:
                print(f" 例外:{e}", flush=True)
                traceback.print_exc()
                ng += 1

        elapsed = time.time() - self.stats['start_time']
        print("\n" + "=" * 60)
        print("収集完了")
        print(f"成功: {ok}ヶ月 / 失敗: {ng}ヶ月 / スキップ: {skipped}ヶ月")
        print(f"総開示件数: {total_records:,}件")
        print(f"HTTP: {self.stats['requests']}回 (成功{self.stats['success']}回)")
        print(f"処理時間: {elapsed/60:.1f}分")
        print("=" * 60)
        return ok > 0


def notify_slack(status, message):
    url = os.getenv("SLACK_WEBHOOK_URL")
    if not url:
        return
    color = "good" if status == "success" else "danger"
    emoji = "✅" if status == "success" else "❌"
    try:
        requests.post(url, json={"attachments": [{
            "color": color,
            "title": f"{emoji} 開示情報収集",
            "text": message,
            "footer": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }]})
    except Exception:
        pass


def main():
    try:
        collector = KabutanDailyCollector()
        result = collector.run()
        if result:
            notify_slack("success", "開示情報収集が正常に完了しました")
        else:
            notify_slack("failure", "開示情報収集で問題が発生しました")
    except Exception as e:
        print(f"致命的エラー: {e}")
        traceback.print_exc()
        notify_slack("failure", f"致命的エラー: {e}")


if __name__ == "__main__":
    main()
