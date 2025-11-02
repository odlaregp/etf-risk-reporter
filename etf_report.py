#!/usr/bin/env python3
# etf_report.py
# Weekly ETF holdings risk reporter for ISIN LU1681045370

import os, io, datetime, logging, smtplib, requests, pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

logging.basicConfig(level=logging.INFO)
TODAY = datetime.date.today().isoformat()

ISIN = "LU1681045370"
DISPLAY_NAME = "Amundi MSCI Emerging Markets Swap UCITS ETF (EUR Acc)"
DOWNLOAD_URL = os.getenv("HOLDINGS_URL", "https://www.amundi.com/.../fund_holdings.csv")

AI_KEYWORDS = {
    "semiconductor": ["semi", "chip", "micro", "TSMC", "SK hynix", "Samsung Electronics", "Micron"],
    "cloud/platform": ["cloud", "aws", "alibaba cloud", "tencent cloud", "amazon", "microsoft"],
    "internet/platforms": ["Tencent", "Alibaba", "Meituan", "Baidu", "JD.com", "PDD", "Sea Ltd", "Naspers"],
    "software/ai": ["NVIDIA", "Palantir", "C3.ai", "SAP", "Infosys", "TCS"],
}

ALERTS = {"top10_pct_alert": 0.35, "ai_exposure_alert": 0.15}

SMTP_ENABLED = True
SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")
EMAIL_FROM = os.getenv("EMAIL_FROM", "")
EMAIL_TO = os.getenv("EMAIL_TO", "")
SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK", None)

def fetch_holdings(url):
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    if "csv" in r.headers.get("Content-Type", "") or url.lower().endswith(".csv"):
        df = pd.read_csv(io.StringIO(r.text))
    else:
        df = pd.DataFrame(r.json())
    return df

def normalize(df):
    name_col = [c for c in df.columns if any(k in c.lower() for k in ["name", "holding", "security"])][0]
    weight_col = [c for c in df.columns if "weight" in c.lower() or "%" in c][0]
    df = df[[name_col, weight_col]].copy()
    df.columns = ["name", "weight"]
    df["weight"] = df["weight"].astype(str).str.replace("%", "").str.replace(",", "").astype(float)
    if df["weight"].sum() < 1.5: df["weight"] *= 100
    return df.sort_values(by="weight", ascending=False).reset_index(drop=True)

def minimal_70(df):
    csum = df["weight"].cumsum()
    idx = (csum >= 70).idxmax()
    return df.loc[:idx]

def ai_flag(name):
    name_l = name.lower()
    for cat, kws in AI_KEYWORDS.items():
        for kw in kws:
            if kw.lower() in name_l:
                return True
    return False

def compute_metrics(df):
    top10 = df.head(10)
    top10_pct = top10["weight"].sum() / 100
    hhi = float(((df["weight"]/100)**2).sum())
    ai_pct = df[df["name"].apply(ai_flag)]["weight"].sum() / 100
    return {"top10_pct": top10_pct, "hhi": hhi, "ai_pct": ai_pct, "top10": top10}

def generate_report(df, metrics, seventy):
    lines = [f"ETF report for {DISPLAY_NAME} ({ISIN}) — {TODAY}",
             f"Top 10 weight: {metrics['top10_pct']*100:.2f}%",
             f"HHI: {metrics['hhi']:.4f}",
             f"AI exposure: {metrics['ai_pct']*100:.2f}%", ""]
    lines.append("Top 10 holdings:")
    for i, r in metrics["top10"].iterrows():
        mark = " [AI]" if ai_flag(r["name"]) else ""
        lines.append(f"{i+1}. {r['name']} — {r['weight']:.2f}%{mark}")
    lines.append("")
    lines.append("Holdings up to 70% cumulative weight:")
    for _, r in seventy.iterrows():
        mark = " [AI]" if ai_flag(r["name"]) else ""
        lines.append(f"- {r['name']} — {r['weight']:.2f}%{mark}")
    return "\n".join(lines)

def send_email(subject, body):
    msg = MIMEMultipart()
    msg["Subject"], msg["From"], msg["To"] = subject, EMAIL_FROM, EMAIL_TO
    msg.attach(MIMEText(body, "plain"))
    s = smtplib.SMTP(SMTP_HOST, SMTP_PORT)
    s.starttls()
    s.login(SMTP_USER, SMTP_PASS)
    s.sendmail(EMAIL_FROM, [EMAIL_TO], msg.as_string())
    s.quit()

def main():
    df = normalize(fetch_holdings(DOWNLOAD_URL))
    metrics = compute_metrics(df)
    seventy = minimal_70(df)
    report = generate_report(df, metrics, seventy)
    print(report)
    if SMTP_ENABLED and SMTP_HOST and EMAIL_TO:
        send_email(f"ETF Report {TODAY}", report)

if __name__ == "__main__":
    main()
