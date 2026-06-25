
#!/usr/bin/env python3
# mirror_disclosure_pdfs.py
# 月次開示JSON(monthly-disclosures)を読み、各PDFをS3へミラーして
# pdf_url を release.tdnet.info から S3 のURLに書き換える。
# TDnetのPDFは約30日で消えるので「毎日」流すこと(古い分は取り返せない)。
# 既に2ヶ月分ミラー済みなので、毎日の実行では直近数日分だけを見る。
#
#   pip install boto3 requests
#   AWS認証(環境変数 or ~/.aws)を設定して:
#   python mirror_disclosure_pdfs.py                # 当月+前月(直近5日分)を処理
#   python mirror_disclosure_pdfs.py 2026-06 2026-05

import os, sys, json, time
from datetime import datetime
from urllib.parse import unquote, urlparse
import boto3, requests

BUCKET      = "m-s3storage"
REGION      = "ap-northeast-1"
JSON_PREFIX = "japan-stocks-5years-chart/monthly-disclosures"   # {YYYY-MM}.json
PDF_PREFIX  = "disclosure-pdf"                                   # ミラー先
S3_BASE     = f"https://{BUCKET}.s3.{REGION}.amazonaws.com"
WINDOW_DAYS = 5                                                 # 毎日実行前提。直近5日分だけ見る(取りこぼし/再挑戦の保険込み)

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


def parse_date(s):
    s = str(s)[:10]
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def out_of_window(date_str):
    """直近WINDOW_DAYSより古い開示はTrue(既に保存済みなのでスキップ)。"""
    d = parse_date(date_str)
    if not d:
        return False                      # 日付不明は念のため処理する
    return (datetime.now() - d).days > WINDOW_DAYS


def direct_tdnet_url(url):
    """やのしんプロキシ(rd.php?...)なら、埋め込まれた本物のTDnet URLを取り出す。"""
    if "rd.php?" in url:
        url = unquote(url.split("rd.php?", 1)[1])   # 念のためURLデコード
    return url


def doc_id(url):
    """TDnet URL末尾のドキュメントIDを返す。例: 140120260601558542"""
    real = direct_tdnet_url(url)
    return os.path.splitext(os.path.basename(urlparse(real).path))[0]


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


def mirror_pdf(src_url, month):
    """TDnetのPDFをS3へコピーし、S3のURLを返す。失敗時はNone。"""
    yyyy, mm = month.split("-")
    did = doc_id(src_url)
    dst_key = f"{PDF_PREFIX}/{yyyy}/{mm}/{did}.pdf"   # 文書IDで一意(配列順に依存しない)

    if s3_exists(dst_key):                        # 冪等: 既にミラー済みなら即返す
        return f"{S3_BASE}/{dst_key}"

    content = download_pdf(src_url)
    if not content:
        return None
    s3.put_object(
        Bucket=BUCKET, Key=dst_key, Body=content,
        ContentType="application/pdf",
    )
    print(f"[mirror] {did} -> {dst_key}")
    return f"{S3_BASE}/{dst_key}"


def process(month):
    key, data = load_json(month)
    if not data:
        return
    items = data.get("disclosures") or []
    changed = False
    for d in items:
        url = (d.get("pdf_url") or "").strip()
        if not url:
            continue
        if url.startswith(S3_BASE):               # 既にS3 → 何もしない
            continue
        if out_of_window(d.get("date")):          # 直近5日より古い → スキップ
            continue
        new_url = mirror_pdf(url, month)
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
