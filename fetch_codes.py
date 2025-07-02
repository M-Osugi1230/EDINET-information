"""
証券コード一覧ファイルを読み込み
→ Google Sheets の SEC_CODE_LIST シートへ一括書き込み

対応フォーマット
- data/sec_codes.csv         （推奨：UTF-8／カンマ区切り）
- data/sec_codes.xls         （Excel 97-2003 形式）
- data/sec_codes.xlsx        （xlsx 形式）
※ファイル名は "data/" フォルダ直下に置けば任意でも可
"""

import os, json, glob, sys
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ---------- 1. 読み込みファイルの探索 ----------
DATA_DIR = "data"
patterns = [ "*.csv", "*.xls", "*.xlsx" ]
files = []
for pat in patterns:
    files.extend(glob.glob(os.path.join(DATA_DIR, pat)))

if not files:
    sys.exit("❌ 証券コードファイルが data/ に見つかりませんでした。")

file_path = files[0]          # 最初に見つかったものを採用
ext = os.path.splitext(file_path)[1].lower()

# ---------- 2. pandas で読み込み ----------
if ext == ".csv":
    df = pd.read_csv(file_path, header=None,
                     names=["secCode", "companyName", "edinetCode"])
elif ext in [".xls", ".xlsx"]:
    df = pd.read_excel(file_path, header=None,
                       names=["secCode", "companyName", "edinetCode"])
else:
    sys.exit(f"❌ 未対応フォーマット: {ext}")

# ---------- 3. Google Sheets 接続 ----------
SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(
    json.loads(os.environ["GSHEET_JSON"]), SCOPES)
gc = gspread.authorize(creds)
ss = gc.open("EDINET_MONITOR")
ws = ss.worksheet("SEC_CODE_LIST")

# ---------- 4. シート全置換 ----------
ws.clear()
ws.update([df.columns.tolist()] + df.astype(str).values.tolist())
print(f"✅ {len(df)} 行を SEC_CODE_LIST にアップロードしました（{os.path.basename(file_path)}）")
