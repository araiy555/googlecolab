#!/usr/bin/env python3
# collect_kabutan_disclosures.py
# TDnet本体(release.tdnet.info)から適時開示(タイトル等)を取得し、PDFをS3に保存して
# monthly-disclosures(YYYY-MM.json)に書き出す。これ1本で完結。yanoshin不使用。
#
#   pip install boto3 requests beautifulsoup4
#   python collect_kabutan_disclosures.py                       # 直近7日
#   FROM=2026-06-01 TO=2026-06-24 python collect_kabutan_disclosures.py

import os, sys, json, time
from datetime import datetime, timedelta
from urllib.parse import urlparse
import boto3, requests
from bs4 import BeautifulSoup

BUCKET, REGION = "m-s3storage", "ap-northeast-1"
JSON_PREFIX = "japan-stocks-5years-chart/monthly-disclosures"
PDF_PREFIX  = "disclosure-pdf"
S3_BASE     = f"https://{BUCKET}.s3.{REGION}.amazonaws.com"
TDNET = "https://www.release.tdnet.info/inbs"

DAYS = int(os.environ.get("DAYS", "7"))
FROM, TO = os.environ.get("FROM", ""), os.environ.get("TO", "")

s3 = boto3.client("s3", region_name=REGION)
sess = requests.Session()
sess.headers.update({"User-Agent": "Mozilla/5.0 (disclosure)"})


def date_range():
    if FROM and TO:
        a = datetime.strptime(FROM[:10], "%Y-%m-%d"); b = datetime.strptime(TO[:10], "%Y-%m-%d")
    else:
        b = datetime.now(); a = b - timedelta(days=DAYS)
    d = a
    while d <= b:
        yield d.strftime("%Y%m%d"); d += timedelta(days=1)


def s3_exists(key):
    try:
        s3.head_object(Bucket=BUCKET, Key=key); return True
    except Exception:
        return False


def doc_id(url):
    return os.path.splitext(os.path.basename(urlparse(url).path))[0]


def download_pdf(url, retries=4):
    backoff = 2
    for i in range(retries):
        try:
            r = sess.get(url, timeout=30)
            if r.status_code == 200 and r.content:
                return r.content
            if r.status_code in (403, 404):
                return None
        except requests.RequestException:
            pass
        if i < retries - 1:
            time.sleep(backoff); backoff *= 2
    return None


def mirror_pdf(src_url, yyyy, mm):
    did = doc_id(src_url)
    if not did:
        return ""
    dst = f"{PDF_PREFIX}/{yyyy}/{mm}/{did}.pdf"
    if s3_exists(dst):
        return f"{S3_BASE}/{dst}"
    content = download_pdf(src_url)
    if not content:
        return ""
    s3.put_object(Bucket=BUCKET, Key=dst, Body=content, ContentType="application/pdf")
    print(f"  [pdf] {did}")
    return f"{S3_BASE}/{dst}"


def fetch_day(yyyymmdd):
    """TDnetのその日の開示を全ページscrape。code/title/pdf を返す。"""
    out = []
    for page in range(1, 40):
        url = f"{TDNET}/I_list_{page:03d}_{yyyymmdd}.html"
        try:
            r = sess.get(url, timeout=30)
        except requests.RequestException:
            break
        if r.status_code != 200:
            break
        r.encoding = r.apparent_encoding or "utf-8"
        soup = BeautifulSoup(r.text, "html.parser")
        titles = soup.find_all("td", class_="kjTitle")
        if not titles:
            break
        for td in titles:
            a = td.find("a")
            tr = td.find_parent("tr")
            code_td = tr.find("td", class_="kjCode") if tr else None
            if not a or not code_td:
                continue
            href = a.get("href") or ""
            out.append({
                "code": code_td.get_text(strip=True),
                "title": a.get_text(strip=True),
                "pdf": f"{TDNET}/{href}" if href else "",
            })
    return out


def load_month(ym):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=f"{JSON_PREFIX}/{ym}.json")["Body"].read()).get("disclosures", [])
    except Exception:
        return []


def main():
    by_month = {}
    for ymd in date_range():
        try:
            items = fetch_day(ymd)
        except Exception as e:
            print(f"[err] {ymd}: {e}"); continue
        date = f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:8]}"
        for it in items:
            code = it["code"].strip()
            if len(code) == 5:
                code = code[:-1]
            title = it["title"].strip()
            if not (code and title):
                continue
            pdf_url = mirror_pdf(it["pdf"], date[:4], date[5:7]) if it["pdf"] else ""
            by_month.setdefault(date[:7], []).append({
                "stock_code": code, "date": date, "title": title,
                "category": "", "info_type": "", "pdf_url": pdf_url,
            })
        print(f"{ymd}: {len(items)}件")
        time.sleep(0.3)

    total = 0
    for ym, news in by_month.items():
        existing = load_month(ym)
        seen = {f"{d.get('stock_code')}|{d.get('date')}|{d.get('title')}" for d in existing}
        added = [e for e in news if f"{e['stock_code']}|{e['date']}|{e['title']}" not in seen]
        if not added:
            print(f"[save] {ym} 変更なし"); continue
        merged = existing + added
        merged.sort(key=lambda d: d.get("date", ""), reverse=True)
        s3.put_object(Bucket=BUCKET, Key=f"{JSON_PREFIX}/{ym}.json",
                      Body=json.dumps({"disclosures": merged}, ensure_ascii=False).encode("utf-8"),
                      ContentType="application/json")
        total += len(added)
        print(f"[save] {ym}: +{len(added)}")
    print(f"=== 新規 {total} 件 ===")


if __name__ == "__main__":
    main()
