"""
当日提出分の YoY / 予想差 などを計算 → DIFF_TODAY シートをフル置換
"""
import pandas as pd
from utils.gspread_helper import get_sheet, replace_df

raw = pd.DataFrame(get_sheet("RAW_FILINGS").get_all_records())
cons = pd.DataFrame(get_sheet("CONSENSUS").get_all_records())

if raw.empty:
    print("RAW_FILINGS is empty – skip")
    exit()

# 今日提出分に絞る
today = pd.Timestamp("today").strftime("%Y-%m-%d")
today_df = raw[raw["submitDate"] == today].copy()
if today_df.empty:
    print("No filings today – DIFF_TODAY cleared")
    replace_df("DIFF_TODAY", pd.DataFrame())
    exit()

# 前年同期実績を結合して YoY 計算
prev_df = raw.copy()
prev_df["key"] = prev_df["secCode"] + "_" + prev_df["fy"].astype(str)
today_df["prevKey"] = today_df["secCode"] + "_" + (today_df["fy"].astype(int) - 1).astype(str)
today_df = today_df.merge(prev_df.add_prefix("prev_"), left_on="prevKey", right_on="prev_key", how="left")

def yoy(cur, prev): return (cur / prev - 1) * 100 if prev else None
today_df["RevenueYoY%"] = today_df.apply(lambda r: yoy(r["Revenue"], r["prev_Revenue"]), axis=1)
today_df["EPSYoY%"] = today_df.apply(lambda r: yoy(r["EPS"], r["prev_EPS"]), axis=1)

# 予想差
today_df = today_df.merge(cons.add_prefix("cons_"), left_on="secCode", right_on="cons_secCode", how="left")
def diff(cur, est): return (cur / est - 1) * 100 if est else None
today_df["RevenueSurprise%"] = today_df.apply(lambda r: diff(r["Revenue"], r["cons_AnalystRevenue"]), axis=1)
today_df["EPSSurprise%"]   = today_df.apply(lambda r: diff(r["EPS"], r["cons_AnalystEPS"]), axis=1)

out_cols = ["secCode", "RevenueYoY%", "EPSYoY%", "RevenueSurprise%", "EPSSurprise%"]
diff_df = today_df[out_cols].round(2).sort_values("RevenueSurprise%", ascending=False)

replace_df("DIFF_TODAY", diff_df)
print(f"DIFF_TODAY rows: {len(diff_df)}")
