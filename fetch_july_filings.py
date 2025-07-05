#!/usr/bin/env python3
"""
2025年7月中に提出されたEDINET決算書類を取得し、
XBRLから売上高・営業利益・経常利益・親会社株主帰属当期純利益・EPSを抽出
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

# ── 設定 ──
EDINET_KEY = os.environ.get("EDINET_KEY")
if not EDINET_KEY:
    raise RuntimeError("環境変数 EDINET_KEY が設定されていません。")
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
        "docID": doc_id,
        "secCode": doc.get("secCode", ""),
        "submitDate": doc.get("submitDateTime", "")[:10],
        "fiscalYear": doc.get("fiscalYear"),
        "fiscalPeriod": doc.get("fiscalPeriod"),
    }
    for key, tags in TAGS.items():
        rec[key] = grab_value(xbrl, tags)
    return rec

if __name__ == '__main__':
    records = []
    start = datetime.datetime(2025, 7, 1, tzinfo=JST)
    end   = datetime.datetime(2025, 7, 31, tzinfo=JST)
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
                print(f"  ⚠️ Error processing {d.get('docID')}: {e}")

    df = pd.DataFrame(records)
    print(f"\nTotal records: {len(df)}")
    if not df.empty:
        print(df.head(10).to_string(index=False))
