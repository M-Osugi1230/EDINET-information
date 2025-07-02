"""
DIFF_TODAY シートを HTML メールで送信
"""
import os, json, smtplib, jinja2
from email.mime.text import MIMEText
from utils.gspread_helper import get_sheet
import pandas as pd

df = pd.DataFrame(get_sheet("DIFF_TODAY").get_all_records())
html_table = df.to_html(index=False, justify="center") if not df.empty else "<p>本日の決算提出はありませんでした。</p>"

template = """
<p>{{ date }} の決算サマリーです。</p>
{{ table|safe }}
<p>詳細は Google Sheets をご覧ください。</p>
"""
body = jinja2.Template(template).render(date=pd.Timestamp("today").strftime("%Y-%m-%d"), table=html_table)

msg = MIMEText(body, "html", "utf-8")
msg["Subject"] = f"[決算速報] {pd.Timestamp('today').strftime('%Y-%m-%d')}"
msg["From"] = os.environ["GMAIL_USER"]
msg["To"] = "osugimurata@icloud.com"

with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
    smtp.login(os.environ["GMAIL_USER"], os.environ["GMAIL_PASS"])
    smtp.send_message(msg)
print("Mail sent")
