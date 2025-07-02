"""
当日提出された決算書類を EDINET API v2 から取得 → XBRL パース
→ RAW_FILINGS シートへ append
"""

import os
import datetime
import io
import zipfile
import re
import requests
import pandas as pd
from dateutil import tz
from utils.gspread_helper import get_sheet, append_df

# ── 設定 ──
JST       = tz.gettz("Asia/Tokyo")
TODAY     = datetime.datetime.now(JST).strftime("%Y-%m-%d")
BASE_URL  = "https://api.edinet-fsa.go.jp/api/v2"
EDINET_KEY = os.environ["EDINET_KEY"]

# ── 監視対象の証券コード一覧取得 ──
ws_codes = get_sheet("SEC_CODE_LIST")
codes    = { r["secCode"] for r in ws_codes.get_all_records() }

# ── 書類一覧 API 呼び出し ──
list_params = {
    "date": TODAY,
    "type": 2,                     # 2 = 有価証券報告書/四半期報告書
    "Subscription-Key": EDINET_KEY
}
resp = requests.get(f"{BASE_URL}/documents.json", params=list_params)
print(f"[EDINET] status = {resp.status_code}")
print(f"[EDINET] preview = {resp.text[:200]!r}")
resp.raise_for_status()

results = resp.json().get("results", [])

# ── XBRL を含む提出書類だけフィルタ ──
targets = [
    d for d in results
    if d.get("secCode") in codes and d.get("xbrlFlag") == "1"
]

records = []
for d in targets:
    doc_id = d["docID"]

    # ── ZIP (XBRL) ダウンロード ──
    file_params = {
        "type": 5,  # 5 = ZIP内のXBRLを取得
        "Subscription-Key": EDINET_KEY
    }
    file_resp = requests.get(f"{BASE_URL}/documents/{doc_id}", params=file_params)
    file_resp.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(file_resp.content)) as z:
        inst = next(name for name in z.namelist() if name.endswith(".xbrl"))
        xbrl = z.read(inst).decode("utf-8", "ignore")

    # ── XBRL タグ抽出ヘルパー ──
    def grab(tag: str) -> float | None:
        m = re.search(fr"<{tag}[^>]*>([\d\.\-]+)</{tag}>", xbrl)
        return float(m.group(1)) if m else None

    rec = {
        "docID":           doc_id,
        "secCode":         d.get("secCode", ""),
        "submitDate":      d.get("submitDateTime", "")[:10],
        "fy":              d.get("fiscalYear", ""),
        "fq":              d.get("fiscalPeriod", ""),
        "Revenue":         grab("jpcrp_cor:NetSales")      or grab("ifrs-full:Revenue"),
        "OperatingIncome": grab("jpcrp_cor:OperatingIncome") or grab("ifrs-full:OperatingProfit"),
        "OrdinaryIncome":  grab("jpcrp_cor:OrdinaryIncome"),
        "ProfitParent":    (grab("jpcrp_cor:ProfitAttributableToOwnersOfParent")
                            or grab("ifrs-full:ProfitLoss")),
        "EPS":             (grab("jpcrp_cor:EarningsPerShare")
                            or grab("ifrs-full:BasicEarningsLossPerShare")),
    }
    records.append(rec)

# ── Google Sheets へ追記 ──
if records:
    df = pd.DataFrame(records)
    append_df("RAW_FILINGS", df)
    print(f"[RAW_FILINGS] Added {len(records)} filings")
else:
    print("[RAW_FILINGS] No new filings today")
