#!/usr/bin/env python3
"""
テスト用：2025年6月1日～実行日までに提出されたEDINET決算書類を取得し、
XBRLから主要数値（Revenue, OperatingIncome, OrdinaryIncome, ProfitParent, EPS）を抜き出して
Google Sheets の「JULY_FILINGS」シートに書き込みます。
"""

import os, datetime, io, zipfile, re, json
import requests, pandas as pd
from dateutil import rrule, tz
from typing import Optional
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ── 必須環境変数 ──
EDINET_KEY  = os.getenv("EDINET_KEY")  or exit("ERROR: EDINET_KEY not set")
GSHEET_JSON = os.getenv("GSHEET_JSON") or exit("ERROR: GSHEET_JSON not set")

BASE_URL = "https://api.edinet-fsa.go.jp/api/v2"
JST = tz.gettz("Asia/Tokyo")

# ── 抽出対象タグ ──
TAGS = {
    "Revenue":        ["jpcrp_cor:NetSales", "ifrs-full:Revenue"],
    "OperatingIncome":["jpcrp_cor:OperatingIncome", "ifrs-full:OperatingProfit"],
    "OrdinaryIncome": ["jpcrp_cor:OrdinaryIncome"],
    "ProfitParent":   ["jpcrp_cor:ProfitAttributableToOwnersOfParent", "ifrs-full:ProfitLoss"],
    "EPS":            ["jpcrp_cor:EarningsPerShare", "ifrs-full:BasicEarningsLossPerShare"],
}

def grab_value(xbrl: str, tags: list[str]) -> Optional[float]:
    for tag in tags:
        m = re.search(fr"<{tag}[^>]*>([\d\.\-]+)</{tag}>", xbrl)
        if m:
            return float(m.group(1))
    return None

def fetch_and_parse(doc: dict) -> dict:
    docid = doc["docID"]
    r = requests.get(
        f"{BASE_URL}/documents/{docid}",
        params={"type":5, "Subscription-Key":EDINET_KEY}, timeout=30
    )
    r.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        inst = next(n for n in z.namelist() if n.endswith(".xbrl"))
        xbrl = z.read(inst).decode("utf-8","ignore")

    rec = {
        "docID": docid,
        "secCode": doc.get("secCode",""),
        "submitDate": doc.get("submitDateTime","")[:10],
        "fiscalYear": doc.get("fiscalYear"),
        "fiscalPeriod": doc.get("fiscalPeriod"),
    }
    for name, taglist in TAGS.items():
        rec[name] = grab_value(xbrl, taglist)
    return rec

if __name__=="__main__":
    # ── 期間：2025-06-01～今日 ──
    today = datetime.datetime.now(JST).date()
    start = datetime.date(2025,6,1)
    end   = today if today>=start else start

    records = []
    for dt in rrule.rrule(rrule.DAILY, dtstart=datetime.datetime.combine(start, datetime.time(0), tzinfo=JST),
                              until=datetime.datetime.combine(end,   datetime.time(0), tzinfo=JST)):
        ds = dt.strftime("%Y-%m-%d")
        print(f"▶︎ Fetching filings for {ds}...")
        resp = requests.get(
            f"{BASE_URL}/documents.json",
            params={"date":ds, "type":2, "Subscription-Key":EDINET_KEY},
            timeout=30
        )
        resp.raise_for_status()
        docs = resp.json().get("results",[])
        xs = [d for d in docs if d.get("xbrlFlag")=="1"]
        print(f"  Found {len(docs)} docs, XBRL={len(xs)}")
        for d in xs:
            try:
                records.append(fetch_and_parse(d))
            except Exception as e:
                print(f"  ⚠️ Error {d.get('docID')}: {e}")

    df = pd.DataFrame(records)
    print(f"\nRESULT: Total records = {len(df)}")
    if df.empty:
        print("No data to write; exiting.")
        exit(0)

    # ── Google Sheets 書き込み ──
    creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(GSHEET_JSON), [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ])
    gc = gspread.authorize(creds)
    ss = gc.open("EDINET_MONITOR")
    ws_name = "JULY_FILINGS"
    try:
        ws = ss.worksheet(ws_name)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(title=ws_name, rows=str(len(df)+1), cols=str(len(df.columns)))

    ws.update([df.columns.tolist()]+df.astype(str).values.tolist(), value_input_option="USER_ENTERED")
    print(f"✅ Wrote {len(df)} rows to sheet '{ws_name}'.")
