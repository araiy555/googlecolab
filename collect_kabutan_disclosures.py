#!/usr/bin/env python3
# mirror_disclosure_pdfs.py
# 月次開示JSON(monthly-disclosures)を読み、各PDFをS3へミラーして
# pdf_url を release.tdnet.info から S3 のURLに書き換える。
# TDnetのPDFは約30日で消えるので「毎日」流すこと(古い分は取り返せない)。
#
#   pip install boto3 requests
#   AWS認証(環境変数 or ~/.aws)を設定して:
#   python mirror_disclosure_pdfs.py                # 当月+前月を処理
#   python mirror_disclosure_pdfs.py 2026-06 2026-05

import sys, json, time
from datetime import datetime
from urllib.parse import unquote
import boto3, requests

BUCKET      = "m-s3storage"
REGION      = "ap-northeast-1"
JSON_PREFIX = "japan-stocks-5years-chart/monthly-disclosures"   # {YYYY-MM}.json
PDF_PREFIX  = "disclosure-pdf"                                   # ミラー先
S3_BASE     = f"https://{BUCKET}.s3.{REGION}.amazonaws.com"

s3 = boto3.client("s3", region_name=REGION)

# セッションを使い回して接続を再利用(プロキシ非経由でTDnet直叩き)
_session = requests.Session()
_session.headers.update({"User-Agent": "Mozilla/5.0 (disclosure-mirror)"})


def months_to_process(argv):
    if argv:
        return argv
    now = datetime.now()
    prev = datetime(now.year, now.month, 1)
    pm = (prev.month - 2) % 12 + 1
    py = prev.year - (1 if prev.month == 1 else 0)
    return [f"{now.year}-{now.month:02d}", f"{py}-{pm:02d}"]


def load_json(month):
    key = f"{JSON_PREFIX}/{month}.json"
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return key, json.loads(obj["Body"].read())
    except s3.exceptions.NoSuchKey:
        print(f"[skip] {key} が存在しません")
        return key, None


def s3_exists(key):
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return True
    except Exception:
        return False


def direct_tdnet_url(url):
    """やのしんプロキシ(rd.php?...)なら、埋め込まれた本物のTDnet URLを取り出す。"""
    if "rd.php?" in url:
        url = url.split("rd.php?", 1)[1]
        url = unquote(url)            # 念のためURLデコード
    return url


def download_pdf(url, timeout=30, retries=4):
    """TDnet本体から直接DL。失敗時は指数バックオフでリトライ。Noneなら諦める。"""
    target = direct_tdnet_url(url)
    backoff = 2
    for attempt in range(retries):
        try:
            r = _session.get(target, timeout=timeout)
            if r.status_code == 200 and r.content:
                return r.content
            if r.status_code in (403, 404):      # もう消えている → リトライ無駄
                print(f"[dead] {target} ({r.status_code})")
                return None
            print(f"[retry] {target} ({r.status_code})")
        except requests.RequestException as e:
            print(f"[retry] {target}: {e}")
        if attempt < retries - 1:
            time.sleep(backoff)
            backoff *= 2                          # 2s, 4s, 8s
    print(f"[err] {target}: リトライ上限")
    return None


def mirror_pdf(src_url, month, code, seq):
    """TDnetのPDFをS3へコピーし、S3のURLを返す。失敗時はNone。"""
    yyyy, mm = month.split("-")
    code4 = str(code).zfill(4)
    dst_key = f"{PDF_PREFIX}/{yyyy}/{mm}/{code4}_{seq}.pdf"

    if s3_exists(dst_key):                        # 冪等: 既にミラー済みなら即返す
        return f"{S3_BASE}/{dst_key}"

    content = download_pdf(src_url)
    if not content:
        return None
    s3.put_object(
        Bucket=BUCKET, Key=dst_key, Body=content,
        ContentType="application/pdf",
    )
    print(f"[mirror] {code4} -> {dst_key}")
    return f"{S3_BASE}/{dst_key}"


def process(month):
    key, data = load_json(month)
    if not data:
        return
    items = data.get("disclosures") or []
    changed = False
    for i, d in enumerate(items):
        url = (d.get("pdf_url") or "").strip()
        if not url:
            continue
        if url.startswith(S3_BASE):               # 既にS3 → 何もしない
            continue
        new_url = mirror_pdf(url, month, d.get("stock_code", "0"), i)
        if new_url:
            d["pdf_url"] = new_url
            changed = True
        time.sleep(0.3)                           # TDnetへの負荷を抑える
    if changed:
        s3.put_object(
            Bucket=BUCKET, Key=key,
            Body=json.dumps(data, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json",
        )
        print(f"[save] {key} を更新しました")
    else:
        print(f"[save] {key} 変更なし")


if __name__ == "__main__":
    for m in months_to_process(sys.argv[1:]):
        print(f"=== {m} ===")
        process(m)
