#!/usr/bin/env python3
# ---------------------------------------------------------------------------
#  edinet_to_sheet.py   2025-06  v1.0
# ---------------------------------------------------------------------------
#  ✓ 指定日（デフォルト：JST 当日）の EDINET 有報／四半報を取得
#  ✓ ZIP-XBRL を解析し主要数値を抽出
#  ✓ 期情報から分類列 (FY2025Q1 など) を生成
#  ✓ Google Sheets へ一括転記（DRY_RUN=1 で CSV のみ出力）
# ---------------------------------------------------------------------------
import os, sys, io, zipfile, re, json, datetime
from typing import Optional, List

import requests, pandas as pd
from dateutil import tz
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ── 必須環境変数 ─────────────────────────────────────────
EDINET_KEY  = os.getenv("EDINET_KEY")  or sys.exit("❌ EDINET_KEY 未設定")
GSHEET_JSON = os.getenv("GSHEET_JSON") or sys.exit("❌ GSHEET_JSON 未設定")

# ── 任意 ───────────────────────────────────────────────
TARGET_DATE  = os.getenv("TARGET_DATE")             # 'YYYY-MM-DD'
MAX_DOCS     = int(os.getenv("MAX_DOCS", "50"))     # 0=制限なし
DRY_RUN      = os.getenv("DRY_RUN") == "1"

SPREADSHEET  = os.getenv("SPREADSHEET_NAME", "EDINET_MONITOR")
SHEET_NAME   = os.getenv("SHEET_NAME",      "EDINET_FILINGS")

# ── 定数 ────────────────────────────────────────────────
BASE_URL = "https://api.edinet-fsa.go.jp/api/v2"
JST      = tz.gettz("Asia/Tokyo")

TAGS = {
    "Revenue":        ["jpcrp_cor:NetSales", "ifrs-full:Revenue"],
    "OperatingIncome":["jpcrp_cor:OperatingIncome", "ifrs-full:OperatingProfit"],
    "OrdinaryIncome": ["jpcrp_cor:OrdinaryIncome"],
    "ProfitParent":   ["jpcrp_cor:ProfitAttributableToOwnersOfParent",
                       "ifrs-full:ProfitLoss"],
    "EPS":            ["jpcrp_cor:EarningsPerShare",
                       "ifrs-full:BasicEarningsLossPerShare"],
}

# ── ユーティリティ ────────────────────────────────────
def grab(xbrl: str, tag_list: List[str]) -> Optional[float]:
    for tag in tag_list:
        m = re.search(fr"<{tag}[^>]*>([\d\.\-]+)</{tag}>", xbrl)
        if m:
            return float(m.group(1))
    return None

def fetch_xbrl_record(meta: dict) -> dict:
    doc_id = meta["docID"]
    r = requests.get(
        f"{BASE_URL}/documents/{doc_id}",
        params={"type": 5},
        headers={"Ocp-Apim-Subscription-Key": EDINET_KEY},
        timeout=30
    )
    r.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        xbrl_name = next(n for n in z.namelist() if n.endswith(".xbrl"))
        xbrl = z.read(xbrl_name).decode("utf-8", "ignore")

    rec = {
        "docID":        doc_id,
        "secCode":      meta.get("secCode",""),
        "submitDate":   meta.get("submitDateTime","")[:10],
        "fiscalYear":   meta.get("fiscalYear"),
        "fiscalPeriod": meta.get("fiscalPeriod"),
    }
    rec["classification"] = (
        f"FY{rec['fiscalYear']}{rec['fiscalPeriod']}" if rec["fiscalYear"] and rec["fiscalPeriod"] else ""
    )

    for col,tags in TAGS.items():
        rec[col] = grab(xbrl, tags)
    return rec

def get_target() -> str:
    if TARGET_DATE:
        return TARGET_DATE
    return datetime.datetime.now(JST).strftime("%Y-%m-%d")

# ── メイン ─────────────────────────────────────────────
def main():
    target = get_target()
    print(f"▶︎ TARGET_DATE={target}  MAX_DOCS={MAX_DOCS or '∞'}  DRY_RUN={DRY_RUN}")

    docs = requests.get(
        f"{BASE_URL}/documents.json",
        params={"date": target, "type": 2},
        headers={"Ocp-Apim-Subscription-Key": EDINET_KEY},
        timeout=30
    ).json().get("results", [])
    xbrl_docs = [d for d in docs if d.get("xbrlFlag")=="1"]
    if MAX_DOCS: xbrl_docs = xbrl_docs[:MAX_DOCS]

    print(f"  docs={len(docs)}, xbrl={len(xbrl_docs)}")

    records = []
    for d in xbrl_docs:
        try:
            records.append(fetch_xbrl_record(d))
        except Exception as e:
            print(f"⚠️ {d['docID']} skip: {e}")

    df = pd.DataFrame(records)
    print(f"RESULT rows={len(df)}")

    if DRY_RUN or df.empty:
        if not df.empty:
            df.to_csv("edinet_test.csv", index=False, encoding="utf-8-sig")
            print("💾 CSV 出力 → edinet_test.csv")
        return

    creds = ServiceAccountCredentials.from_json_keyfile_dict(
        json.loads(GSHEET_JSON),
        ["https://www.googleapis.com/auth/spreadsheets",
         "https://www.googleapis.com/auth/drive"]
    )
    gc = gspread.authorize(creds)
    ss = gc.open(SPREADSHEET)
    try:
        ws = ss.worksheet(SHEET_NAME); ws.clear()
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(title=SHEET_NAME, rows=str(len(df)+5), cols=str(len(df.columns)))

    ws.update([df.columns.tolist()] + df.astype(str).values.tolist(),
              value_input_option="USER_ENTERED")
    print(f"✅ Wrote {len(df)} rows → {SPREADSHEET}/{SHEET_NAME}")

if __name__ == "__main__":
    main()
