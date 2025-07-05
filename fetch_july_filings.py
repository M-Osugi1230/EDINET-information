#!/usr/bin/env python3
"""
2025年7月中に提出されたEDINET決算書類を取得し、
XBRLから売上高・営業利益・経常利益・親会社株主帰属当期純利益・EPSを抽出、
結果をGoogle Sheetsの「JULY_FILINGS」シートに書き込むスクリプト。

── テスト仕様 ──
・テスト実行日が2025-07-01以前の場合はExit 0で終了（取得対象なし）
・テスト実行日が7月中であれば、7/1 ～ テスト当日までを取得
"""

import os
import datetime
import io
import zipfile
import re
import requests
import pandas as pd
from dateutil import rrule, tz
from typing import Optional
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ── 設定 ──
EDINET_KEY = os.environ.get("EDINET_KEY")
if not EDINET_KEY:
    raise RuntimeError("環境変数 EDINET_KEY が設定されていません。")

GSHEET_JSON = os.environ.get("GSHEET_JSON")
if not GSHEET_JSON:
    raise RuntimeError("環境変数 GSHEET_JSON が設定されていません。")

BASE_URL = "https://api.edinet-fsa.go.jp/api/v2"
JST = tz.gettz("Asia/Tokyo")

# ── 解析対象タグ ──
TAGS = {
    "Revenue": ["jpcrp_cor:NetSales", "ifrs-full:Revenue"],
    "OperatingIncome": ["jpcrp_cor:OperatingIncome", "ifrs-full:OperatingProfit"],
    "OrdinaryIncome": ["jpcrp_cor:OrdinaryIncome"],
    "ProfitParent": [
        "jpcrp_cor:ProfitAttributableToOwnersOfParent",
        "ifrs-full:ProfitLoss"
    ],
    "EPS": ["jpcrp_cor:EarningsPerShare", "ifrs-full:BasicEarningsLossPerShare"],
}

def grab_value(xbrl: str, tags: list[str]) -> Optional[float]:
    """複数候補タグから順に検索し、最初にマッチした数値を返す"""
    for tag in tags:
        m = re.search(fr"<{tag}[^>]*>([\d\.\-]+)</{tag}>", xbrl)
        if m:
            return float(m.group(1))
    return None

def fetch_and_parse(doc: dict) -> dict:
    """単一ドキュメントをZIP→XBRLパースし、主要指標を返す"""
    doc_id = doc.get("docID")
    resp = requests.get(
        f"{BASE_URL}/documents/{doc_id}",
        params={"type": 5, "Subscription-Key": EDINET_KEY},
        timeout=30
    )
    resp.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        inst = next(n for n in z.namelist() if n.endswith(".xbrl"))
        xbrl = z.read(inst).decode("utf-8", "ignore")

    rec = {
        "docID":        doc_id,
        "secCode":      doc.get("secCode", ""),
        "submitDate":   doc.get("submitDateTime", "")[:10],
        "fiscalYear":   doc.get("fiscalYear"),
        "fiscalPeriod": doc.get("fiscalPeriod"),
    }
    for key, tags in TAGS.items():
        rec[key] = grab_value(xbrl, tags)
    return rec

if __name__ == '__main__':
    # テスト用：実行日を基準に対象期間を動的に決定
    today = datetime.datetime.now(JST).date()
    start_date = datetime.date(2025, 7, 1)
    if today < start_date:
        print(f"【INFO】テスト実行日({today})が開始日({start_date})より前。処理なしで正常終了。")
        exit(0)
    end_date = min(today, datetime.date(2025, 7, 31))

    start = datetime.datetime.combine(start_date, datetime.time(0, 0), tzinfo=JST)
    end   = datetime.datetime.combine(end_date,   datetime.time(0, 0), tzinfo=JST)

    records = []
    for dt in rrule.rrule(rrule.DAILY, dtstart=start, until=end):
        date_str = dt.strftime("%Y-%m-%d")
        print(f"▶︎ Fetching filings for {date_str}...")
        resp = requests.get(
            f"{BASE_URL}/documents.json",
            params={"date": date_str, "type": 2, "Subscription-Key": EDINET_KEY},
            timeout=30
        )
        resp.raise_for_status()
        docs = resp.json().get("results", [])
        xbrl_targets = [d for d in docs if d.get("xbrlFlag") == "1"]
        print(f"  Found {len(docs)} docs, XBRL={len(xbrl_targets)}")
        for d in xbrl_targets:
            try:
                records.append(fetch_and_parse(d))
            except Exception as e:
                print(f"  ⚠️ Error {d.get('docID')}: {e}")

    df = pd.DataFrame(records)
    print(f"\n【RESULT】Total records: {len(df)}")
    if not df.empty:
        print(df.head(10).to_string(index=False))

    # ── Google Sheets へ書き込み ──
    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(
        json.loads(GSHEET_JSON), SCOPES
    )
    client = gspread.authorize(creds)
    ss = client.open("EDINET_MONITOR")
    sheet_name = "JULY_FILINGS"
    try:
        ws = ss.worksheet(sheet_name)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(title=sheet_name, rows=str(len(df)+1), cols=str(len(df.columns)))

    ws.update(
        [df.columns.tolist()] + df.astype(str).values.tolist(),
        value_input_option="USER_ENTERED"
    )
    print(f"✅ Google Sheets '{sheet_name}' に {len(df)} 件を書き込みました。")
