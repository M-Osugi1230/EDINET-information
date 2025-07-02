"""
Yahoo!ファイナンス「業績予想」ページをスクレイピング
→ CONSENSUS シートへ Upsert
"""
import os, json, time, requests, pandas as pd
from bs4 import BeautifulSoup
from utils.gspread_helper import get_sheet, upsert_df

BASE_URL = "https://finance.yahoo.co.jp/quote/{sec}.T/analysis"
ws_codes = get_sheet("SEC_CODE_LIST")
codes = [r["secCode"] for r in ws_codes.get_all_records()]

cons_records = []
for sec in codes:
    url = BASE_URL.format(sec=sec)
    r = requests.get(url, timeout=15)
    if r.status_code != 200:
        continue
    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table", {"aria-label": "業績予想"})
    if not table:
        continue
    rows = table.find_all("tr")
    # 行: 通期会社予想 / 通期アナリスト予想 を優先
    mapping = {}
    for tr in rows:
        th = tr.find("th").text.strip()
        if "通期会社予想" in th or "通期予想" in th:
            tds = [td.text.replace(",", "").strip() for td in tr.find_all("td")]
            mapping["CompanyRevenue"], mapping["CompanyEPS"] = tds[0], tds[2]
        elif "通期アナリスト予想" in th:
            tds = [td.text.replace(",", "").strip() for td in tr.find_all("td")]
            mapping["AnalystRevenue"], mapping["AnalystEPS"] = tds[0], tds[2]
    if not mapping:
        continue
    mapping.update({"secCode": sec, "scrapeDate": pd.Timestamp("today").strftime("%Y-%m-%d")})
    cons_records.append(mapping)
    time.sleep(0.3)  # polite

if cons_records:
    upsert_df("CONSENSUS", pd.DataFrame(cons_records), ["secCode"])
    print(f"Consensus upserted: {len(cons_records)}")
else:
    print("No consensus scraped")
