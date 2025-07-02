"""
証券コード一覧（data/data_j.xls.download.zip 内）を読み込み
→ Google Sheets の SEC_CODE_LIST シートへ一括書き込み
※初回実行のみ。完了後は Secrets: SEC_CODE_DONE=true でスキップ
"""
import os, json, zipfile, io
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ---- Google Sheets 接続 ----
SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(
    json.loads(os.environ["GSHEET_JSON"]), SCOPES)
gc = gspread.authorize(creds)
ss = gc.open("EDINET_MONITOR")
ws = ss.worksheet("SEC_CODE_LIST")

# ---- 証券コード読み込み ----
zip_path = "data/data_j.xls.download.zip"
with zipfile.ZipFile(zip_path) as z:
    # 最初に見つかった .xls を使用
    xls_name = next(n for n in z.namelist() if n.endswith(".xls"))
    with z.open(xls_name) as f:
        df = pd.read_excel(f, header=None,
                           names=["secCode", "companyName", "edinetCode"])
# ---- シート更新（ヘッダー＋データ）----
ws.clear()
ws.update([df.columns.tolist()] + df.astype(str).values.tolist())
print(f"Uploaded {len(df)} codes to SEC_CODE_LIST")
