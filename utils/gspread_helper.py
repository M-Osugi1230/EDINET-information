"""
Google Sheets 共通ユーティリティ
"""
import os, json, gspread, pandas as pd
from oauth2client.service_account import ServiceAccountCredentials

SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(
    json.loads(os.environ["GSHEET_JSON"]), SCOPES)
gc = gspread.authorize(creds)
ss = gc.open("EDINET_MONITOR")

def get_sheet(name: str):
    return ss.worksheet(name)

def append_df(sheet_name: str, df: pd.DataFrame):
    ws = get_sheet(sheet_name)
    ws.append_rows(df.astype(str).values.tolist(), value_input_option="USER_ENTERED")

def upsert_df(sheet_name: str, df: pd.DataFrame, keys: list[str]):
    ws = get_sheet(sheet_name)
    existing = pd.DataFrame(ws.get_all_records())
    if existing.empty:
        append_df(sheet_name, df)
        return
    merged = df.merge(existing, how="left", on=keys, indicator=True)
    updates, inserts = merged[merged["_merge"] == "both"], merged[merged["_merge"] == "left_only"]
    # 行更新
    for _, row in updates.iterrows():
        cond = True
        for k in keys:
            cond &= existing[k] == row[k]
        idx = existing[cond].index[0] + 2  # header offset
        ws.update(f"A{idx}", [row[df.columns].astype(str).tolist()])
    # 行追加
    if not inserts.empty:
        append_df(sheet_name, inserts[df.columns])

def replace_df(sheet_name: str, df: pd.DataFrame):
    ws = get_sheet(sheet_name)
    ws.clear()
    if not df.empty:
        ws.update([df.columns.tolist()] + df.astype(str).values.tolist())
