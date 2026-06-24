#!/usr/bin/env python3
# collect_jquants_disclosures.py
# J-Quants V2(bulk) から決算/開示を取得し、既存 monthly-disclosures(YYYY-MM.json) へマージ。
# GitHub Action 用: AWS認証は環境変数、J-Quants APIキーは JQUANTS_API_KEY。
#
#   pip install boto3 requests
#   JQUANTS_API_KEY=xxxx python collect_jquants_disclosures.py
#   初回は DEBUG=1 で bulk構造とCSVヘッダを出力 → 列名を確定する。

import os, sys, json, gzip, csv, tempfile, time
from datetime import datetime, timedelta
import requests, boto3

BUCKET      = "m-s3storage"
REGION      = "ap-northeast-1"
JSON_PREFIX = "japan-stocks-5years-chart/monthly-disclosures"   # 既存と同じ

BASE_URL = "https://api.jquants.com/v2"
API_KEY  = os.environ.get("JQUANTS_API_KEY", "")
# ★要確認: 開示/決算のV2エンドポイント。tickは "/equities/trades"。
ENDPOINT = os.environ.get("JQ_ENDPOINT", "/equities/financial_statements")
YEARS    = int(os.environ.get("YEARS", "5"))
DEBUG    = os.environ.get("DEBUG", "1") == "1"

s3 = boto3.client("s3", region_name=REGION)
_session = requests.Session()
_session.headers.update({"x-api-key": API_KEY})

PERIOD_LABEL = {"1Q": "第1四半期", "2Q": "第2四半期", "3Q": "第3四半期", "FY": "通期"}


def bulk_list():
    r = _session.get(f"{BASE_URL}/bulk/list", params={"endpoint": ENDPOINT}, timeout=30)
    r.raise_for_status()
    data = r.json().get("data", [])
    if DEBUG and data:
        print("=== bulk/list サンプル(先頭3件) ===")
        for d in data[:3]:
            print(json.dumps(d, ensure_ascii=False))
    cutoff = (datetime.now() - timedelta(days=365 * YEARS)).strftime("%Y%m%d")
    out = []
    for f in data:
        key = f.get("Key", "")
        if "/live/" not in key:
            continue
        digits = "".join(c for c in key.split("/")[-1] if c.isdigit())
        date8 = digits[:8] if len(digits) >= 8 else ""
        if date8 and date8 >= cutoff:
            out.append(key)
    return out


def get_download_url(key):
    r = _session.get(f"{BASE_URL}/bulk/get", params={"key": key}, timeout=30)
    r.raise_for_status()
    return r.json().get("url")


def make_title(row):
    period = (row.get("TypeOfCurrentPeriod") or "").strip()
    fy = (row.get("CurrentFiscalYearEndDate") or row.get("CurrentPeriodEndDate") or "").strip()
    ym = ""
    if len(fy) >= 7:
        try:
            ym = f"{fy[:4]}年{int(fy[5:7])}月期"
        except Exception:
            ym = ""
    return f"{ym} {PERIOD_LABEL.get(period, period)} 決算短信".strip()


def row_to_disclosure(row):
    code = (row.get("LocalCode") or row.get("Code") or "").strip()
    if len(code) == 5:
        code = code[:-1]
    date = (row.get("DisclosedDate") or row.get("Date") or "").strip()
    if not code or not date:
        return None
    return {
        "stock_code": code,
        "date": date,
        "title": make_title(row),
        "category": "決算",
        "info_type": (row.get("TypeOfDocument") or "").strip(),
        "pdf_url": "",                     # J-QuantsはPDFなし(mirrorはTDnet側だけ処理)
    }


def collect_from_file(key):
    url = get_download_url(key)
    entries = []
    with tempfile.NamedTemporaryFile(suffix=".csv.gz", delete=True) as tmp:
        r = _session.get(url, timeout=300, stream=True)
        r.raise_for_status()
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            tmp.write(chunk)
        tmp.flush()
        with gzip.open(tmp.name, "rt", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if DEBUG and reader.fieldnames:
                print("=== CSVヘッダ（列名）===")
                print(", ".join(reader.fieldnames))
            for row in reader:
                e = row_to_disclosure(row)
                if e:
                    entries.append(e)
    return entries


def load_month(ym):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=f"{JSON_PREFIX}/{ym}.json")
        d = json.loads(obj["Body"].read())
        return d.get("disclosures", []) if isinstance(d, dict) else []
    except s3.exceptions.NoSuchKey:
        return []
    except Exception:
        return []


def save_month(ym, items):
    s3.put_object(
        Bucket=BUCKET, Key=f"{JSON_PREFIX}/{ym}.json",
        Body=json.dumps({"disclosures": items}, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )


def main():
    if not API_KEY:
        print("JQUANTS_API_KEY 未設定"); sys.exit(1)
    print(f"=== J-Quants開示取得 endpoint={ENDPOINT} years={YEARS} ===")
    files = bulk_list()
    print(f"対象ファイル: {len(files)} 件")

    by_month = {}
    for i, key in enumerate(files):
        try:
            for e in collect_from_file(key):
                by_month.setdefault(e["date"][:7], []).append(e)
            if DEBUG and i == 0:
                print("▲構造を確認したら DEBUG=0 で本実行")
            time.sleep(1)
        except Exception as ex:
            print(f"[err] {key}: {ex}")

    total = 0
    for ym, news in by_month.items():
        existing = load_month(ym)
        seen = {f"{d.get('stock_code')}|{d.get('date')}|{d.get('title')}" for d in existing}
        added = [e for e in news if f"{e['stock_code']}|{e['date']}|{e['title']}" not in seen]
        if not added:
            continue
        merged = existing + added
        merged.sort(key=lambda d: d.get("date", ""), reverse=True)
        save_month(ym, merged)
        total += len(added)
        print(f"[save] {ym}: +{len(added)} (計 {len(merged)})")
    print(f"=== 完了: 新規 {total} 件 / {len(by_month)} ヶ月 ===")


if __name__ == "__main__":
    main()
