import argparse
from pathlib import Path
from typing import Optional, Sequence

import numpy as np
import pandas as pd


REQUIRED_COLUMNS = {
    "total_assets": "fs_combas_A001000000",       # Total Assets (A001000000)
    "current_assets": "fs_combas_A001100000",     # Current Assets (A001100000)
    "current_liabilities": "fs_combas_A002100000",# Current Liabilities (A002100000)
    "total_liabilities": "fs_combas_A002000000",  # Total Liabilities (A002000000)
    "retained_earnings": "fs_comins_B002000000",  # Retained Earnings (B002000000)
    "operating_profit": "fs_comins_B001300000",   # Operating Profit / EBIT (B001300000)
    "operating_revenue": "fs_comins_B001101000",  # Operating Revenue (B001101000)
    "market_value": "bdt_fin_MarketValueOfCompany1",  # Market Value from BDT_FinDistMertonDD
    "net_profit": "fs_comins_B001000000",         # Net profit for ROA
    "fixed_assets": "fs_combas_A001212000",       # Tangible fixed assets
}


def load_merged(data_dir: Path) -> tuple[pd.DataFrame, Path]:
    candidates: Sequence[Path] = [data_dir / "filtered" / "merged_filtered.csv", data_dir / "merged_filtered.csv"]
    for path in candidates:
        if path.exists():
            return pd.read_csv(path), path
    raise FileNotFoundError("merged_filtered.csv not found in data-dir or data-dir/filtered")


def safe_div(numer: pd.Series, denom: pd.Series) -> pd.Series:
    denom = denom.replace({0: np.nan})
    return numer / denom


def compute_metrics(df: pd.DataFrame) -> pd.DataFrame:
    missing = [col for col in REQUIRED_COLUMNS.values() if col not in df.columns]
    if missing:
        raise KeyError(f"Missing required columns in merged data: {missing}")

    years = pd.to_numeric(df.get("Date"), errors="coerce").astype("Int64")
    df = df.loc[years.notna()].copy()
    df["Date"] = years.loc[years.notna()].astype(int)
    if df.empty:
        raise ValueError("No valid year values found in merged data; cannot compute metrics.")

    ta = df[REQUIRED_COLUMNS["total_assets"]]
    ca = df[REQUIRED_COLUMNS["current_assets"]]
    cl = df[REQUIRED_COLUMNS["current_liabilities"]]
    tl = df[REQUIRED_COLUMNS["total_liabilities"]]
    re = df[REQUIRED_COLUMNS["retained_earnings"]]
    ebit = df[REQUIRED_COLUMNS["operating_profit"]]
    sales = df[REQUIRED_COLUMNS["operating_revenue"]]
    mv = df[REQUIRED_COLUMNS["market_value"]]
    net_profit = df[REQUIRED_COLUMNS["net_profit"]]
    fixed_assets = df[REQUIRED_COLUMNS["fixed_assets"]]

    x1 = safe_div(ca - cl, ta)
    x2 = safe_div(re, ta)
    x3 = safe_div(ebit, ta)
    x4 = safe_div(mv, tl)
    x5 = safe_div(sales, ta)

    z = 1.2 * x1 + 1.4 * x2 + 3.3 * x3 + 0.6 * x4 + 0.999 * x5

    out = pd.DataFrame()
    out["Symbol"] = df.get("Symbol")
    out["Date"] = df.get("Date")
    out["AltmanZScore"] = z
    out["X1_WorkingCapitalToTotalAssets"] = x1
    out["X2_RetainedEarningsToTotalAssets"] = x2
    out["X3_EBITToTotalAssets"] = x3
    out["X4_MarketValueToTotalLiabilities"] = x4
    out["X5_SalesToTotalAssets"] = x5

    out["FirmSize_LogTotalAssets"] = np.where(ta > 0, np.log(ta), np.nan)
    out["Leverage"] = safe_div(tl, ta)
    out["ROA"] = safe_div(net_profit, ta)
    out["FixedAssetsRatio"] = safe_div(fixed_assets, ta)

    growth_frame = pd.DataFrame({"Symbol": df.get("Symbol"), "Date": df.get("Date"), "sales": sales})
    growth_frame = growth_frame.sort_values(["Symbol", "Date"])
    growth_frame["SalesGrowth"] = growth_frame.groupby("Symbol")[["sales"]].pct_change()
    out["SalesGrowth"] = growth_frame.sort_index()["SalesGrowth"]

    return out


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Compute Altman Z and derived metrics from merged_filtered.csv")
    parser.add_argument("--data-dir", type=Path, default=Path.cwd(), help="Base data directory (expects filtered/merged_filtered.csv)")
    parser.add_argument("--output", type=Path, default=None, help="Optional separate CSV; if omitted, only merged_filtered.csv is updated")
    parser.add_argument(
        "--no-update-merged",
        action="store_true",
        help="Do not write derived metrics back into merged_filtered.csv",
    )
    args = parser.parse_args(argv)

    base_dir = args.data_dir.resolve()
    output = args.output  # None by default to avoid a separate file

    merged_df, merged_path = load_merged(base_dir)
    result = compute_metrics(merged_df)

    if not args.no_update_merged:
        merged_with = merged_df.copy()
        if "AltmanZ_Band" in merged_with.columns:
            merged_with = merged_with.drop(columns=["AltmanZ_Band"])
        for col in result.columns:
            if col in {"Symbol", "Date"}:
                continue
            merged_with[col] = result[col]
        merged_path.parent.mkdir(parents=True, exist_ok=True)
        merged_with.to_csv(merged_path, index=False)
        print(f"Merged file updated with analytics metrics -> {merged_path}")
    else:
        print("Merged file not updated (--no-update-merged set)")

    if output is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
        result.to_csv(output, index=False)
        print(f"Analytics metrics also written separately to {output} (rows={len(result)})")
    elif args.no_update_merged:
        print("No outputs written (suppressing merged update and no --output provided)")


if __name__ == "__main__":
    main()
