#!/usr/bin/env python3
# jquants_disclosures.py
# J-Quants /td/list で適時開示一覧(最大5年)を取得し、各PDFを /td/files から
# S3(disclosure-pdf/YYYY/MM/{discNo}.pdf)へ保存、monthly-disclosures に書き出す。
# GitHub Action 用。AWS認証=環境変数(secrets)、J-Quants=JQUANTS_API_KEY。冪等。
#
#   pip install boto3 requests
#   日次:        JQUANTS_API_KEY=xxx python jquants_disclosures.py
#   5年backfill: JQUANTS_API_KEY=xxx FROM=2021-01-01 TO=2021-12-31 python jquants_disclosures.py

import os, sys, json, time
from datetime import datetime, timedelta
import requests, boto3

BUCKET, REGION = "m-s3storage", "ap-northeast-1"
JSON_PREFIX = "japan-stocks-5years-chart/monthly-disclosures"
PDF_PREFIX  = "disclosure-pdf"
S3_BASE     = f"https://{BUCKET}.s3.{REGION}.amazonaws.com"

BASE_URL = "https://api.jquants.com/v2"
API_KEY  = os.environ.get("JQUANTS_API_KEY", "")
DAYS = int(os.environ.get("DAYS", "7"))
FROM, TO = os.environ.get("FROM", ""), os.environ.get("TO", "")

s3 = boto3.client("s3", region_name=REGION)
sess = requests.Session()
sess.headers.update({"x-api-key": API_KEY})


def date_range():
    if FROM and TO:
        a = datetime.strptime(FROM[:10], "%Y-%m-%d"); b = datetime.strptime(TO[:10], "%Y-%m-%d")
    else:
        b = datetime.now(); a = b - timedelta(days=DAYS)
    d = a
    while d <= b:
        yield d.strftime("%Y-%m-%d"); d += timedelta(days=1)


def s3_exists(key):
    try:
        s3.head_object(Bucket=BUCKET, Key=key); return True
    except Exception:
        return False


def td_list_day(date):
    rows, pkey = [], None
    while True:
        p = {"date": date}
        if pkey:
            p["pagination_key"] = pkey
        r = sess.get(f"{BASE_URL}/td/list", params=p, timeout=30)
        if r.status_code == 404:
            break
        r.raise_for_status()
        j = r.json()
        rows += j.get("td_list") or j.get("data") or j.get("disclosures") or []
        pkey = j.get("pagination_key")
        if not pkey:
            break
        time.sleep(0.2)
    return rows


def mirror_pdf(disc_no, yyyy, mm):
    dst = f"{PDF_PREFIX}/{yyyy}/{mm}/{disc_no}.pdf"
    if s3_exists(dst):
        return f"{S3_BASE}/{dst}"
    r = sess.get(f"{BASE_URL}/td/files", params={"discNo": disc_no, "docs": "g"}, timeout=30)
    if r.status_code == 404:
        return ""
    r.raise_for_status()
    url = (r.json().get("files") or {}).get("pdf")
    if not url:
        return ""
    pdf = sess.get(url, timeout=60)
    if pdf.status_code != 200 or not pdf.content:
        return ""
    s3.put_object(Bucket=BUCKET, Key=dst, Body=pdf.content, ContentType="application/pdf")
    print(f"  [pdf] {disc_no}")
    return f"{S3_BASE}/{dst}"


def load_month(ym):
    try:
        body = s3.get_object(Bucket=BUCKET, Key=f"{JSON_PREFIX}/{ym}.json")["Body"].read()
        return json.loads(body).get("disclosures", [])
    except Exception:
        return []


def main():
    if not API_KEY:
        print("JQUANTS_API_KEY 未設定"); sys.exit(1)

    by_month = {}
    for date in date_range():
        try:
            rows = td_list_day(date)
        except Exception as e:
            print(f"[err] {date}: {e}"); continue
        for row in rows:
            if str(row.get("DiscStatus") or "") == "delete":
                continue
            code = str(row.get("Code") or "")
            if len(code) == 5:
                code = code[:-1]
            ddate = str(row.get("DiscDate") or "")
            title = str(row.get("Title") or "")
            disc  = str(row.get("DiscNo") or "")
            if not (code and ddate and title and disc):
                continue
            pdf_url = mirror_pdf(disc, ddate[:4], ddate[5:7]) if "g" in (row.get("Docs") or []) else ""
            by_month.setdefault(ddate[:7], []).append({
                "stock_code": code, "date": ddate, "title": title,
                "category": "適時開示", "info_type": "", "pdf_url": pdf_url, "disc_no": disc,
            })
        time.sleep(0.15)

    total = 0
    for ym, news in by_month.items():
        existing = load_month(ym)
        seen = {d.get("disc_no") for d in existing if d.get("disc_no")}
        added = [e for e in news if e["disc_no"] not in seen]
        if not added:
            continue
        merged = existing + added
        merged.sort(key=lambda d: d.get("date", ""), reverse=True)
        s3.put_object(Bucket=BUCKET, Key=f"{JSON_PREFIX}/{ym}.json",
                      Body=json.dumps({"disclosures": merged}, ensure_ascii=False).encode("utf-8"),
                      ContentType="application/json")
        total += len(added)
        print(f"[save] {ym}: +{len(added)}")
    print(f"=== 完了: 新規 {total} 件 ===")


if __name__ == "__main__":
    main()
