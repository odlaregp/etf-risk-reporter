#!/usr/bin/env python3
# portfolio_risk_assessment.py
# Aggregates all CSV holdings files in the repository and computes portfolio-wide risk metrics.
# Assumption: Each CSV is a fund holdings file whose 'weight' column sums to ~100 for that fund.
#             If you have explicit portfolio weights for each fund, you can modify the script to use them.
# Output: prints a portfolio summary and writes portfolio_summary.csv to the workspace.

import glob, os, sys, math, datetime
import pandas as pd

# Configuration
DISPLAY_NAME = "My Portfolio — Aggregated Holdings"
OUTPUT_CSV = "portfolio_summary.csv"
ASSUME_EQUAL_FUND_WEIGHT = True  # If True, each CSV (fund) is treated as equal-sized in your portfolio

# AI detection keywords (extend as needed)
AI_NAME_KEYWORDS = ["nvidia", "intel", "tsmc", "sk hynix", "micron", "amd", "qualcomm", "tencent", "alibaba", "baidu", "sea ltd", "pdd", "jd.com", "amazon", "microsoft", "google", "alphabet", "sap", "infosys", "tcs"]
AI_SECTOR_KEYWORDS = ["technology", "information technology", "semiconductors", "software", "internet services", "data processing", "it services", "communications"]

def find_csv_files():
    # Find all .csv files in repo root (ignore this script and output CSV)
    files = [f for f in os.listdir('.') if f.lower().endswith('.csv') and f != OUTPUT_CSV]
    return sorted(files)

def detect_columns(df):
    # Map expected columns (case-insensitive)
    cols = {c.lower(): c for c in df.columns}
    mapping = {}
    for key in ["isin", "name", "asset class", "asset_class", "assetclass", "currency", "weight", "sector", "country"]:
        for lower, orig in cols.items():
            if key.replace("_"," ") in lower or key in lower:
                mapping[key] = orig
    # fallback greedily
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

def normalize_weights(df, weight_col):
    # Remove percent signs, commas, convert to float. If sum < 1.5 assume decimals and *100
    s = df[weight_col].astype(str).str.replace('%','').str.replace(',','').astype(float)
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
        print("No CSV files found in repository root. Upload your holdings CSV(s) and re-run.")
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
            print(f"Could not detect necessary columns in {f}. Columns found: {list(df.columns)}")
            continue
        name_col = mapping["name"]; weight_col = mapping["weight"]
        isin_col = mapping.get("isin", None)
        sector_col = mapping.get("sector", None)
        country_col = mapping.get("country", None)
        # Normalize weights to sum to 100 per fund
        df = df[[name_col, weight_col] + ([isin_col] if isin_col else []) + ([sector_col] if sector_col else []) + ([country_col] if country_col else [])]
        df.columns = ["name", "weight"] + (["isin"] if isin_col else []) + (["sector"] if sector_col else []) + (["country"] if country_col else [])
        df["weight"] = normalize_weights(df, "weight")
        # If weights do not sum to 100, rescale to sum to 100 (protects from missing cash line)
        total = df["weight"].sum()
        if total <= 0:
            print(f"Warning: total weights for {f} is {total}. Skipping file.")
            continue
        df["weight"] = df["weight"] * (100.0 / total)
        funds.append({"file": f, "df": df})
    if not funds:
        print("No valid fund files parsed. Exiting.")
        sys.exit(1)

    num_funds = len(funds)
    # Compute per-file multiplier for portfolio aggregation (equal-weighted funds unless you change this)
    if ASSUME_EQUAL_FUND_WEIGHT:
        fund_multiplier = {fund["file"]: 1.0/num_funds for fund in funds}
    else:
        # If you have fund-level allocations, implement here. For now fallback to equal.
        fund_multiplier = {fund["file"]: 1.0/num_funds for fund in funds}

    # Aggregate holdings across funds into a portfolio table
    rows = []
    for fund in funds:
        mult = fund_multiplier[fund["file"]]
        for idx, r in fund["df"].iterrows():
            rows.append({
                "source_file": fund["file"],
                "name": r.get("name", ""),
                "isin": r.get("isin", ""),
                "sector": r.get("sector", ""),
                "country": r.get("country", ""),
                "fund_weight_pct": r.get("weight", 0.0),
                "portfolio_weight_pct": r.get("weight", 0.0) * mult
            })
    port_df = pd.DataFrame(rows)
    # Consolidate by ISIN when present, otherwise by name
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

    # Compute AI flag per holding
    agg["ai_flag"] = agg.apply(lambda r: ai_flag(r.get("name",""), r.get("sector","")), axis=1)

    # Sorting and metrics
    agg = agg.sort_values(by="portfolio_weight_pct", ascending=False).reset_index(drop=True)
    agg["rank"] = agg.index + 1
    # Top N
    top20 = agg.head(20).copy()
    # Sector & country concentration
    sector_agg = agg.groupby("sector")["portfolio_weight_pct"].sum().sort_values(ascending=False)
    country_agg = agg.groupby("country")["portfolio_weight_pct"].sum().sort_values(ascending=False)
    # AI exposure
    total_ai_pct = agg[agg["ai_flag"]]["portfolio_weight_pct"].sum()
    # HHI (weights as fractions)
    ws = agg["portfolio_weight_pct"].fillna(0) / 100.0
    hhi = float((ws**2).sum())

    # Print summary
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
    # Save aggregated portfolio CSV
    agg = agg[["rank"] + ([ "isin","name"] if "isin" in agg.columns else ["name"]) + ["sector","country","portfolio_weight_pct","ai_flag"]]
    agg = agg.rename(columns={"portfolio_weight_pct":"weight_pct"})
    agg.to_csv(OUTPUT_CSV, index=False)
    print(f"Aggregated portfolio summary written to {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
