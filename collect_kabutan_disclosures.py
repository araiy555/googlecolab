#!/usr/bin/env python3
# collect_jquants_disclosures.py
# J-Quants /td/list から適時開示インデックス(5年分)を取得し、
# 既存 monthly-disclosures(YYYY-MM.json) へマージ。PDFはオンデマンド(resolver Lambda)。
#
#   pip install boto3 requests
#   日次(直近7日):   JQUANTS_API_KEY=xxx python collect_jquants_disclosures.py
#   範囲backfill:    JQUANTS_API_KEY=xxx FROM=2021-01-01 TO=2021-12-31 python collect_jquants_disclosures.py
#   PDFリンク有効化:  TD_RESOLVER=https://xxxx.lambda-url... を併せて設定

import os, sys, json, time
from datetime import datetime, timedelta
import requests, boto3

BUCKET      = "m-s3storage"
REGION      = "ap-northeast-1"
JSON_PREFIX = "japan-stocks-5years-chart/monthly-disclosures"

BASE_URL = "https://api.jquants.com/v2"
API_KEY  = os.environ.get("JQUANTS_API_KEY", "")
TD_RESOLVER = os.environ.get("TD_RESOLVER", "")   # discNo→PDF を返すLambda(無ければpdf_url空)
DAYS = int(os.environ.get("DAYS", "7"))           # 範囲未指定なら直近N日
FROM = os.environ.get("FROM", "")                 # backfill用(YYYY-MM-DD)
TO   = os.environ.get("TO", "")

s3 = boto3.client("s3", region_name=REGION)
_session = requests.Session()
_session.headers.update({"x-api-key": API_KEY})


def date_range():
    if FROM and TO:
        a = datetime.strptime(FROM[:10].replace("/", "-"), "%Y-%m-%d")
        b = datetime.strptime(TO[:10].replace("/", "-"), "%Y-%m-%d")
    else:
        b = datetime.now()
        a = b - timedelta(days=DAYS)
    d = a
    while d <= b:
        yield d.strftime("%Y-%m-%d")
        d += timedelta(days=1)


def td_list_day(date):
    """その日の適時開示一覧を全件(ページング込み)取得。"""
    rows, pkey = [], None
    while True:
        params = {"date": date}
        if pkey:
            params["pagination_key"] = pkey
        r = _session.get(f"{BASE_URL}/td/list", params=params, timeout=30)
        if r.status_code == 404:           # データ無い日
            break
        r.raise_for_status()
        j = r.json()
        # レスポンスのリスト本体キーは仕様により "td_list" 等。両対応。
        items = j.get("td_list") or j.get("data") or j.get("disclosures") or []
        rows.extend(items)
        pkey = j.get("pagination_key")
        if not pkey:
            break
        time.sleep(0.2)
    return rows


def to_entry(row):
    code = str(row.get("Code") or "").strip()
    if len(code) == 5:                     # 5桁→4桁
        code = code[:-1]
    date = str(row.get("DiscDate") or "").strip()
    title = str(row.get("Title") or "").strip()
    disc = str(row.get("DiscNo") or "").strip()
    if not code or not date or not title:
        return None
    if str(row.get("DiscStatus") or "") == "delete":   # 削除開示は出さない
        return None
    docs = row.get("Docs") or []
    pdf_url = f"{TD_RESOLVER}?discNo={disc}" if (TD_RESOLVER and "g" in docs) else ""
    return {
        "stock_code": code,
        "date": date,
        "title": title,
        "category": "適時開示",
        "info_type": "",
        "pdf_url": pdf_url,
        "disc_no": disc,
    }


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

    by_month = {}
    n = 0
    for date in date_range():
        try:
            for row in td_list_day(date):
                e = to_entry(row)
                if e:
                    by_month.setdefault(e["date"][:7], []).append(e)
                    n += 1
        except Exception as ex:
            print(f"[err] {date}: {ex}")
        time.sleep(0.15)
    print(f"取得: {n} 件 / {len(by_month)} ヶ月")

    total = 0
    for ym in sorted(by_month):
        existing = load_month(ym)
        # 既存(TDnet/kabutan)と重複しないよう disc_no か code|date|title で除外
        seen_disc = {d.get("disc_no") for d in existing if d.get("disc_no")}
        seen_key  = {f"{d.get('stock_code')}|{d.get('date')}|{d.get('title')}" for d in existing}
        added = []
        for e in by_month[ym]:
            if e["disc_no"] and e["disc_no"] in seen_disc:
                continue
            if f"{e['stock_code']}|{e['date']}|{e['title']}" in seen_key:
                continue
            added.append(e)
        if not added:
            continue
        merged = existing + added
        merged.sort(key=lambda d: (d.get("date", ""), d.get("disc_no", "")), reverse=True)
        save_month(ym, merged)
        total += len(added)
        print(f"[save] {ym}: +{len(added)} (計 {len(merged)})")
    print(f"=== 完了: 新規 {total} 件 ===")


if __name__ == "__main__":
    main()
