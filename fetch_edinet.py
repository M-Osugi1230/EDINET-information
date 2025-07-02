"""
当日提出された決算書類を EDINET API から取得 → XBRL パース
→ RAW_FILINGS シートへ append
"""
import os, json, datetime, io, zipfile, re, requests, pandas as pd
from dateutil import tz
from utils.gspread_helper import get_sheet, append_df

EDINET_KEY = os.environ["EDINET_KEY"]
JST = tz.gettz("Asia/Tokyo")
TODAY = datetime.datetime.now(JST).strftime("%Y-%m-%d")
BASE_URL = "https://disclosure.edinet-fsa.go.jp/api/v1"

# ---- Google Sheets 読込 ----
ws_codes = get_sheet("SEC_CODE_LIST")
codes = {r["secCode"] for r in ws_codes.get_all_records()}

# ---- EDINET 書類一覧 ----
params = {"date": TODAY, "type": "2", "Subscription-Key": EDINET_KEY}
docs = requests.get(f"{BASE_URL}/documents.json", params=params).json()["results"]

targets = [d for d in docs if d["secCode"] in codes and d["xbrlFlag"] == "1"]
records = []

def grab(tag: str, text: str):
    m = re.search(fr"<{tag}[^>]*>([\d\.\-]+)</{tag}>", text)
    return float(m.group(1)) if m else None

for d in targets:
    doc_id = d["docID"]
    # ZIP ダウンロード
    bin_data = requests.get(f"{BASE_URL}/documents/{doc_id}",
                            params={"type": "5", "Subscription-Key": EDINET_KEY}).content
    with zipfile.ZipFile(io.BytesIO(bin_data)) as z:
        xbrl_file = next(n for n in z.namelist() if n.endswith(".xbrl"))
        xbrl_txt = z.read(xbrl_file).decode("utf-8", "ignore")

    rec = {
        "docID": doc_id,
        "secCode": d["secCode"],
        "submitDate": d["submitDateTime"][:10],
        "fy": d.get("fiscalYear", ""),
        "fq": d.get("fiscalPeriod", ""),
        "Revenue": grab("jpcrp_cor:NetSales", xbrl_txt) or grab("ifrs-full:Revenue", xbrl_txt),
        "OperatingIncome": grab("jpcrp_cor:OperatingIncome", xbrl_txt)
                           or grab("ifrs-full:OperatingProfit", xbrl_txt),
        "OrdinaryIncome": grab("jpcrp_cor:OrdinaryIncome", xbrl_txt),
        "ProfitParent": grab("jpcrp_cor:ProfitAttributableToOwnersOfParent", xbrl_txt)
                        or grab("ifrs-full:ProfitLoss", xbrl_txt),
        "EPS": grab("jpcrp_cor:EarningsPerShare", xbrl_txt)
               or grab("ifrs-full:BasicEarningsLossPerShare", xbrl_txt),
    }
    records.append(rec)

if records:
    append_df("RAW_FILINGS", pd.DataFrame(records))
    print(f"Added {len(records)} filings")
else:
    print("No filings today")
