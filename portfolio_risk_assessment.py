# Trigger workflow test
#!/usr/bin/env python3
# portfolio_risk_assessment.py
# Aggregates all CSV holdings files in the repository and computes portfolio-wide risk metrics.
# Assumes each CSV is a fund holdings file with a 'weight' column (per-fund weights sum ~100).
# Output: prints a portfolio summary and writes portfolio_summary.csv

import os, sys, datetime
import pandas as pd

DISPLAY_NAME = "My Portfolio — Aggregated Holdings"
OUTPUT_CSV = "portfolio_summary.csv"
ASSUME_EQUAL_FUND_WEIGHT = True  # treat each CSV as equal-sized fund; change if you have allocations

AI_NAME_KEYWORDS = ["nvidia","intel","tsmc","sk hynix","micron","amd","qualcomm","tencent","alibaba","baidu","sea ltd","pdd","jd.com","amazon","microsoft","alphabet","google","sap","infosys","tcs"]
AI_SECTOR_KEYWORDS = ["technology","information technology","semiconductors","software","internet","data processing","it services","communications","software & services"]

def find_csv_files():
    files = [f for f in os.listdir('.') if f.lower().endswith('.csv') and f != OUTPUT_CSV]
    return sorted(files)

def detect_columns(df):
    cols = {c.lower(): c for c in df.columns}
    mapping = {}
    # map likely names
    for key in ["isin","name","asset class","asset_class","currency","weight","sector","country"]:
        for lower, orig in cols.items():
            if key.replace("_"," ") in lower or key in lower:
                mapping[key.strip()] = orig
    # fallback heuristics
    if "name" not in mapping:
        for lower, orig in cols.items():
            if "name" in lower or "holding" in lower or "security" in lower:
                mapping["name"] = orig; break
    if "weight" not in mapping:
        for lower, orig in cols.items():
            if "weight" in lower or "%" in lower:
                mapping["weight"] = orig; break
    if "isin" not in mapping:
        for lower, orig in cols.items():
            if "isin" in lower or "security id" in lower:
                mapping["isin"] = orig; break
    if "sector" not in mapping:
        for lower, orig in cols.items():
            if "sector" in lower:
                mapping["sector"] = orig; break
    if "country" not in mapping:
        for lower, orig in cols.items():
            if "country" in lower or "geography" in lower or "region" in lower:
                mapping["country"] = orig; break
    return mapping

def normalize_weights(s):
    s = s.astype(str).str.replace('%','').str.replace(',','').astype(float)
    if s.sum() < 1.5:
        s = s * 100.0
    return s

def ai_flag(name, sector):
    name_l = str(name).lower()
    sector_l = str(sector).lower() if pd.notna(sector) else ""
    for kw in AI_NAME_KEYWORDS:
        if kw.lower() in name_l:
            return True
    for kw in AI_SECTOR_KEYWORDS:
        if kw.lower() in sector_l:
            return True
    return False

def main():
    csv_files = find_csv_files()
    if not csv_files:
        print("No CSV files found in repository root. Upload holdings CSV(s) and re-run.")
        sys.exit(1)

    funds = []
    for f in csv_files:
        try:
            df = pd.read_csv(f)
        except Exception as e:
            print(f"Failed to read {f}: {e}")
            continue
        mapping = detect_columns(df)
        if "name" not in mapping or "weight" not in mapping:
            print(f"Could not detect 'name'/'weight' columns in {f}. Columns: {list(df.columns)}")
            continue
        cols = [mapping["name"], mapping["weight"]]
        for optional in ("isin","sector","country"):
            if optional in mapping:
                cols.append(mapping[optional])
        df = df[cols].copy()
        col_names = ["name","weight"]
        if mapping.get("isin"): col_names.append("isin")
        if mapping.get("sector"): col_names.append("sector")
        if mapping.get("country"): col_names.append("country")
        df.columns = col_names
        df["weight"] = normalize_weights(df["weight"])
        total = df["weight"].sum()
        if total <= 0:
            print(f"Warning: total weights for {f} is {total}. Skipping.")
            continue
        df["weight"] = df["weight"] * (100.0 / total)
        funds.append({"file": f, "df": df})

    if not funds:
        print("No valid fund files parsed. Exiting.")
        sys.exit(1)

    num_funds = len(funds)
    fund_multiplier = {fund["file"]: 1.0/num_funds for fund in funds} if ASSUME_EQUAL_FUND_WEIGHT else {fund["file"]: 1.0/num_funds for fund in funds}

    rows = []
    for fund in funds:
        mult = fund_multiplier[fund["file"]]
        for _, r in fund["df"].iterrows():
            rows.append({
                "source_file": fund["file"],
                "name": r.get("name",""),
                "isin": r.get("isin","") if "isin" in r.index else "",
                "sector": r.get("sector","") if "sector" in r.index else "",
                "country": r.get("country","") if "country" in r.index else "",
                "fund_weight_pct": r.get("weight",0.0),
                "portfolio_weight_pct": r.get("weight",0.0) * mult
            })

    port_df = pd.DataFrame(rows)
    key = "isin" if port_df["isin"].notna().sum() > 0 else "name"

    if key == "isin":
        agg = port_df.groupby(["isin","name"]).agg({
            "sector": lambda x: x.dropna().iloc[0] if len(x.dropna())>0 else "",
            "country": lambda x: x.dropna().iloc[0] if len(x.dropna())>0 else "",
            "portfolio_weight_pct": "sum"
        }).reset_index()
    else:
        agg = port_df.groupby(["name"]).agg({
            "sector": lambda x: x.dropna().iloc[0] if len(x.dropna())>0 else "",
            "country": lambda x: x.dropna().iloc[0] if len(x.dropna())>0 else "",
            "portfolio_weight_pct": "sum"
        }).reset_index()

    agg["ai_flag"] = agg.apply(lambda r: ai_flag(r.get("name",""), r.get("sector","")), axis=1)
    agg = agg.sort_values(by="portfolio_weight_pct", ascending=False).reset_index(drop=True)
    agg["rank"] = agg.index + 1

    top20 = agg.head(20).copy()
    sector_agg = agg.groupby("sector")["portfolio_weight_pct"].sum().sort_values(ascending=False)
    country_agg = agg.groupby("country")["portfolio_weight_pct"].sum().sort_values(ascending=False)
    total_ai_pct = agg[agg["ai_flag"]]["portfolio_weight_pct"].sum()
    ws = agg["portfolio_weight_pct"].fillna(0) / 100.0
    hhi = float((ws**2).sum())

    now = datetime.datetime.utcnow().isoformat()
    print(f"{DISPLAY_NAME} — Portfolio risk assessment")
    print(f"Date (UTC): {now}")
    print(f"CSV files processed: {num_funds}: {', '.join(csv_files)}")
    print(f"Aggregation mode: equal-weight funds = {ASSUME_EQUAL_FUND_WEIGHT}")
    print("-"*60)
    print(f"Total AI-related exposure: {total_ai_pct:.2f}% of portfolio")
    print(f"HHI concentration index: {hhi:.4f}")
    print("-"*60)
    print("Top 20 holdings (by portfolio %):")
    for i, r in top20.iterrows():
        ai = " [AI]" if r["ai_flag"] else ""
        isin_str = r.get("isin","") if "isin" in r.columns else ""
        print(f"{i+1:2d}. {r.get('name','')[:60]} {('('+str(isin_str)+')') if isin_str else ''} — {r['portfolio_weight_pct']:.2f}%{ai}")
    print("-"*60)
    print("Top sectors:")
    for s, w in sector_agg.head(10).items():
        print(f"- {s[:40]} — {w:.2f}%")
    print("-"*60)
    print("Top countries:")
    for c, w in country_agg.head(10).items():
        print(f"- {c[:40]} — {w:.2f}%")
    print("-"*60)

    # Save aggregated CSV
    output_cols = ["rank"]
    if "isin" in agg.columns:
        output_cols += ["isin","name"]
    else:
        output_cols += ["name"]
    output_cols += ["sector","country","portfolio_weight_pct","ai_flag"]
    agg_out = agg[output_cols].rename(columns={"portfolio_weight_pct":"weight_pct"})
    agg_out.to_csv(OUTPUT_CSV, index=False)
    print(f"Aggregated portfolio summary written to {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
