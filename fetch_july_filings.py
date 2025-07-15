#!/usr/bin/env python3
# ──────────────────────────────────────────────────────────────────────
#  ultra_light_edinet.py
#  ---------------------------------------------------------------
#  ・検証用の最小構成:
#      - 対象日付は 1 日だけ（過去日を推奨）
#      - XBRL ドキュメントも先頭 N 件だけ
#      - Google Sheets 書き込みは行数が 0 の場合スキップ
#  ・環境変数（Secrets が便利）
#      EDINET_KEY   : EDINET API キー           (必須)
#      GSHEET_JSON  : Google SA JSON           (必須)
#      TEST_DATE    : 取得日 (YYYY-MM-DD)      (省略時 2024-05-01)
#      MAX_DOCS     : 1 日あたり取得上限 N     (省略時 3)
#      SKIP_SHEET   : "1" で Sheets へ書かない (任意)
# ----------------------------------------------------------------

import os, datetime, io, zipfile, re, json, sys
from typing import Optional

import requests, pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dateutil import tz

# ── 設定 ───────────────────────────────────────────────
EDINET_KEY  = os.getenv("EDINET_KEY")  or sys.exit("❌ EDINET_KEY 未設定")
GSHEET_JSON = os.getenv("GSHEET_JSON") or sys.exit("❌ GSHEET_JSON 未設定")

TEST_DATE = os.getenv("TEST_DATE", "2024-05-01")           # 実データのある日を推奨
MAX_DOCS  = int(os.getenv("MAX_DOCS", "3"))                # 取得ファイル上限
SKIP_SHEET = os.getenv("SKIP_SHEET") == "1"                # 書込みスキップ?

SPREADSHEET = os.getenv("SPREADSHEET_NAME", "EDINET_MONITOR")
SHEET_NAME  = os.getenv("SHEET_NAME", "EDINET_FILINGS")

BASE_URL = "https://api.edinet-fsa.go.jp/api/v2"
JST = tz.gettz("Asia/Tokyo")

TAGS = {
    "Revenue":        ["jpcrp_cor:NetSales","ifrs-full:Revenue"],
    "OperatingIncome":["jpcrp_cor:OperatingIncome","ifrs-full:OperatingProfit"],
    "OrdinaryIncome": ["jpcrp_cor:OrdinaryIncome"],
    "ProfitParent":   ["jpcrp_cor:ProfitAttributableToOwnersOfParent","ifrs-full:ProfitLoss"],
    "EPS":            ["jpcrp_cor:EarningsPerShare","ifrs-full:BasicEarningsLossPerShare"],
}

# ── 関数 ───────────────────────────────────────────────
def grab_value(xbrl: str, tags: list[str]) -> Optional[float]:
    for tag in tags:
        m = re.search(fr"<{tag}[^>]*>([\d\.\-]+)</{tag}>", xbrl)
        if m:
            return float(m.group(1))
    return None

def fetch_xbrl_data(doc: dict) -> dict:
    z = requests.get(
        f"{BASE_URL}/documents/{doc['docID']}",
        params={"type":5,"Subscription-Key":EDINET_KEY}, timeout=30
    ).content
    with zipfile.ZipFile(io.BytesIO(z)) as zp:
        inst = next(n for n in zp.namelist() if n.endswith(".xbrl"))
        xbrl = zp.read(inst).decode("utf-8","ignore")

    rec = {
        "docID":        doc["docID"],
        "secCode":      doc.get("secCode",""),
        "submitDate":   doc.get("submitDateTime","")[:10],
        "fiscalYear":   doc.get("fiscalYear"),
        "fiscalPeriod": doc.get("fiscalPeriod"),
        "classification": f"FY{doc.get('fiscalYear')}{doc.get('fiscalPeriod')}",
    }
    for col,taglist in TAGS.items():
        rec[col] = grab_value(xbrl, taglist)
    return rec

# ── メイン ───────────────────────────────────────────
def main():
    print(f"▶︎ TEST_DATE = {TEST_DATE}, MAX_DOCS = {MAX_DOCS}")
    params = {"date": TEST_DATE, "type": 2, "Subscription-Key": EDINET_KEY}
    docs = requests.get(f"{BASE_URL}/documents.json", params=params, timeout=30).json().get("results", [])
    xbrl_docs = [d for d in docs if d.get("xbrlFlag") == "1"][:MAX_DOCS]

    print(f"  Found {len(docs)} docs, XBRL sliced to {len(xbrl_docs)}")

    recs = []
    for d in xbrl_docs:
        try:
            recs.append(fetch_xbrl_data(d))
        except Exception as e:
            print(f"⚠️ {d['docID']} skip: {e}")

    df = pd.DataFrame(recs)
    print(f"RESULT → rows = {len(df)}")
    if df.empty or SKIP_SHEET:
        print("🛈 DataFrame が空、または書込みスキップ指定。終了します。")
        return

    # Sheets 書込み
    creds = ServiceAccountCredentials.from_json_keyfile_dict(
        json.loads(GSHEET_JSON),
        ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
    )
    gc = gspread.authorize(creds)
    sh = gc.open(SPREADSHEET)
    try:
        ws = sh.worksheet(SHEET_NAME); ws.clear()
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=SHEET_NAME, rows=str(len(df)+1), cols=str(len(df.columns)))
    ws.update([df.columns.tolist()] + df.astype(str).values.tolist(), value_input_option="USER_ENTERED")
    print(f"✅ Wrote {len(df)} rows to '{SHEET_NAME}' in '{SPREADSHEET}'.")

if __name__ == "__main__":
    main()
