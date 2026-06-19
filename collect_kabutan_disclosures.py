#!/usr/bin/env python3
"""
TDnet開示情報収集（Yanoshin TDnet WebAPI版）
- HTMLスクレイプではなくJSON APIで取得 → GitHub Actions等のDC IPでも弾かれにくい
- 出力S3形式は従来(kabutan版)と同一: japan-stocks-5years-chart/monthly-disclosures/{YYYY}-{MM}.json
  → WTCフロントは無変更で動く
API: https://webapi.yanoshin.jp/webapi/tdnet/list/{query}.json
  query例: recent / 20260601 / 20260601-20260617 / 7203
"""

import os, json, time, random, calendar, traceback
from datetime import datetime
import requests
import boto3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class YanoshinTDnetCollector:
    def __init__(self):
        print("初期化中...")
        self.api_base = "https://webapi.yanoshin.jp/webapi/tdnet/list"
        self.bucket_name = "m-s3storage"
        self.s3_prefix = "japan-stocks-5years-chart/monthly-disclosures/"

        self.delay_base = 0.4
        self.delay_variance = 0.2
        self.retry_delay = 4.0
        self.max_retries = 5
        self.page_limit = 100  # 1ページ件数

        self.s3 = self._init_s3_client()
        self.session = self._init_session()
        self.stats = {'requests': 0, 'success': 0, 'start_time': None}
        print("初期化完了")

    def _init_s3_client(self):
        ak, sk = os.getenv('AWS_ACCESS_KEY_ID'), os.getenv('AWS_SECRET_ACCESS_KEY')
        if ak and sk:
            return boto3.client('s3', aws_access_key_id=ak, aws_secret_access_key=sk, region_name="ap-northeast-1")
        return boto3.client('s3', region_name="ap-northeast-1")

    def _init_session(self):
        s = requests.Session()
        retry = Retry(total=2, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
        ad = HTTPAdapter(max_retries=retry)
        s.mount("https://", ad)
        s.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36',
            'Accept': 'application/json,text/javascript,*/*;q=0.1',
            'Accept-Language': 'ja,en-US;q=0.9,en;q=0.8',
        })
        return s

    # ── HTTP(JSON)取得 ──
    def _fetch_json(self, url):
        for attempt in range(self.max_retries):
            try:
                time.sleep(self.delay_base + random.uniform(-self.delay_variance, self.delay_variance) + (self.retry_delay * attempt if attempt else 0))
                r = self.session.get(url, timeout=30)
                self.stats['requests'] += 1
                if r.status_code == 200:
                    self.stats['success'] += 1
                    try:
                        return r.json()
                    except Exception:
                        return None
                elif r.status_code == 429:
                    wait = 20 * (attempt + 1)
                    print(f" [429→{wait}s]", end='', flush=True); time.sleep(wait)
                else:
                    print(f" [HTTP{r.status_code}]", end='', flush=True); time.sleep(self.retry_delay)
            except Exception as e:
                print(f" [ERR:{e}]", end='', flush=True); time.sleep(self.retry_delay)
        return None

    # ── 1件マッピング（kabutan版と同じ出力キー）──
    def _map_item(self, t, year, month):
        code = str(t.get('company_code') or '').strip()
        if len(code) == 5:           # 5桁(末尾チェックデジット)→4桁
            code = code[:4]
        if not code:
            return None
        pub = str(t.get('pubdate') or '')
        date_str = pub[:10] if len(pub) >= 10 else f"{year}-{month:02d}-01"
        title = (t.get('title') or '不明').strip() or '不明'
        return {
            'stock_code': code,
            'company_name': (t.get('company_name') or '不明').strip(),
            'title': title,
            'date': date_str,
            'market': t.get('markets_string') or '',
            'info_type': '',
            'category': self._categorize(title),
            'pdf_url': t.get('document_url') or t.get('url') or '',
            'source': 'TDnet',
            'year': year,
            'month': month,
        }

    def _categorize(self, title):
        if any(w in title for w in ['決算', '業績', '四半期', '売上', '利益']): return '決算・業績'
        if any(w in title for w in ['配当', '株主優待', '自己株式']):        return '配当・株主還元'
        if any(w in title for w in ['人事', '役員', '代表取締役']):          return '人事・組織'
        if any(w in title for w in ['買収', 'M&A', '資本提携', '業務提携']): return 'M&A・提携'
        if any(w in title for w in ['新製品', '新サービス', '開発', '特許']):return '事業・製品'
        return 'その他'

    # ── 1日分（ページング: offsetを実件数で進める）──
    def _fetch_day(self, dstr, year, month):
        base = f"{self.api_base}/{dstr}.json"
        out, seen, offset = [], set(), 0
        for _ in range(80):  # 安全上限
            data = self._fetch_json(f"{base}?limit={self.page_limit}&start={offset}")
            items = (data.get('items') if isinstance(data, dict) else data) or []
            if not items:
                break
            new = 0
            for it in items:
                t = it.get('Tdnet') if isinstance(it, dict) and 'Tdnet' in it else it
                rid = t.get('id') or f"{t.get('company_code')}_{t.get('title')}"
                if rid in seen:
                    continue
                seen.add(rid)
                rec = self._map_item(t, year, month)
                if rec:
                    out.append(rec); new += 1
            if new == 0:           # 新規が無い＝終端 or startが効かない
                break
            offset += len(items)   # 実件数で進める（limit上限が小さくても安全）
        return out

    # ── 1ヶ月分 ──
    def fetch_month(self, year, month):
        last_day = calendar.monthrange(year, month)[1]
        records = []
        for day in range(1, last_day + 1):
            ymd = f"{year}{month:02d}{day:02d}"
            recs = self._fetch_day(ymd, year, month)
            if recs:
                records.extend(recs)
        # 重複除去
        seen, uniq = set(), []
        for r in records:
            k = f"{r['stock_code']}_{r['date']}_{r['title']}"
            if k not in seen:
                seen.add(k); uniq.append(r)
        print(f" →{len(uniq)}件", flush=True)
        return uniq

    def save_to_s3(self, year, month, disclosures):
        if not self.s3:
            return False
        try:
            data = {'year': year, 'month': month, 'total_disclosures': len(disclosures),
                    'disclosures': disclosures, 'categories': {}, 'companies': {},
                    'updated_at': datetime.now().isoformat()}
            for d in disclosures:
                data['categories'][d['category']] = data['categories'].get(d['category'], 0) + 1
                data['companies'][d['company_name']] = data['companies'].get(d['company_name'], 0) + 1
            key = f"{self.s3_prefix}{year}-{month:02d}.json"
            self.s3.put_object(Bucket=self.bucket_name, Key=key,
                Body=json.dumps(data, ensure_ascii=False, indent=2).encode('utf-8'),
                ContentType='application/json')
            return True
        except Exception as e:
            print(f" S3エラー:{e}", flush=True)
            return False

      def get_target_months(self):
        """直近 MONTHS_BACK ヶ月（既定2）。当月から遡って生成。"""
        n = int(os.getenv('MONTHS_BACK', '2'))
        t = datetime.now()
        out = []
        y, m = t.year, t.month
        for _ in range(max(1, n)):
            out.append((y, m))
            m -= 1
            if m == 0:
                m = 12; y -= 1
        out.reverse()  # 古い順
        return out

    def run(self):
        print("=" * 60); print("TDnet(Yanoshin) 開示収集開始"); print("=" * 60)
        self.stats['start_time'] = time.time()
        months = self.get_target_months()
        ok = ng = total = 0
        for idx, (y, m) in enumerate(months, 1):
            print(f"[{idx}/{len(months)}] {y}/{m:02d}", end='', flush=True)
            try:
                recs = self.fetch_month(y, m)
                if recs and self.save_to_s3(y, m, recs):
                    ok += 1; total += len(recs)
                else:
                    ng += 1
            except Exception as e:
                print(f" 例外:{e}", flush=True); traceback.print_exc(); ng += 1
        el = time.time() - self.stats['start_time']
        print("\n" + "=" * 60)
        print(f"成功:{ok}ヶ月 / 失敗:{ng}ヶ月 / 総件数:{total:,} / HTTP:{self.stats['requests']}回(成功{self.stats['success']}) / {el/60:.1f}分")
        print("=" * 60)
        return ok > 0


def notify_slack(status, message):
    url = os.getenv("SLACK_WEBHOOK_URL")
    if not url:
        return
    try:
        requests.post(url, json={"attachments": [{
            "color": "good" if status == "success" else "danger",
            "title": f"{'✅' if status=='success' else '❌'} TDnet開示収集",
            "text": message, "footer": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}]})
    except Exception:
        pass


def main():
    try:
        c = YanoshinTDnetCollector()
        notify_slack("success" if c.run() else "failure",
                     "開示情報収集が完了しました" if True else "")
    except Exception as e:
        print(f"致命的エラー: {e}"); traceback.print_exc()
        notify_slack("failure", f"致命的エラー: {e}")


if __name__ == "__main__":
    main()
