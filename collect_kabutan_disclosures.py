#!/usr/bin/env python3
# collect_kabutan_disclosures.py
# Yanoshin TDnet APIで適時開示(タイトル等)を取得し、PDFをS3に保存して
# monthly-disclosures(YYYY-MM.json)に書き出す。これ1本で完結。
#
#   pip install boto3 requests
#   python collect_kabutan_disclosures.py                       # 直近7日
#   FROM=2026-06-01 TO=2026-06-24 python collect_kabutan_disclosures.py   # 期間指定

import os, sys, json, time
from datetime import datetime, timedelta
from urllib.parse import unquote, urlparse
import boto3, requests

BUCKET, REGION = "m-s3storage", "ap-northeast-1"
JSON_PREFIX = "japan-stocks-5years-chart/monthly-disclosures"
PDF_PREFIX  = "disclosure-pdf"
S3_BASE     = f"https://{BUCKET}.s3.{REGION}.amazonaws.com"
API = "https://webapi.yanoshin.jp/webapi/tdnet/list"

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


def direct_tdnet_url(url):
    if "rd.php?" in url:
        url = unquote(url.split("rd.php?", 1)[1])
    return url


def doc_id(url):
    real = direct_tdnet_url(url)
    return os.path.splitext(os.path.basename(urlparse(real).path))[0]


def download_pdf(url, retries=4):
    target = direct_tdnet_url(url)
    backoff = 2
    for i in range(retries):
        try:
            r = sess.get(target, timeout=30)
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
    """PDFをS3に保存しS3 URLを返す。冪等。失敗は空。"""
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
    r = sess.get(f"{API}/{yyyymmdd}.json", params={"limit": 1000}, timeout=30)
    r.raise_for_status()
    return r.json().get("items", [])


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
        for it in items:
            t = it.get("Tdnet") or it
            code = str(t.get("company_code") or "").strip()
            if len(code) == 5:
                code = code[:-1]
            title = str(t.get("title") or "").strip()
            pubdate = str(t.get("pubdate") or "").strip()
            date = pubdate[:10]
            src = str(t.get("document_url") or t.get("url") or "").strip()
            if not (code and date and title):
                continue
            pdf_url = mirror_pdf(src, date[:4], date[5:7]) if src else ""
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
