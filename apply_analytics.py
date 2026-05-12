import argparse
from pathlib import Path
from typing import Optional, Sequence

import numpy as np
import pandas as pd
from pandas import DataFrame


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
    "roi_numerator": "fs_comins_B002000000",      # Net Profit (B002000000) for ROI numerator
}


def load_merged(data_dir: Path) -> tuple[pd.DataFrame, Path]:
    candidates: Sequence[Path] = [data_dir / "data" / "filtered" / "merged_filtered.csv", data_dir / "filtered" / "merged_filtered.csv", data_dir / "merged_filtered.csv"]
    for path in candidates:
        if path.exists():
            return pd.read_csv(path), path
    raise FileNotFoundError("merged_filtered.csv not found in data-dir, data-dir/data/filtered, or data-dir/filtered")


def safe_div(numer: pd.Series, denom: pd.Series) -> pd.Series:
    denom = denom.replace({0: np.nan})
    return numer / denom


def normalize_symbol(series: pd.Series) -> pd.Series:
    out = series.astype(str).str.strip()
    return out.str.replace(r"\.0$", "", regex=True)


def load_ocscore(data_dir: Path) -> Optional[DataFrame]:
    candidates = [
        data_dir / "data" / "filtered" / "ocscore_filtered.xlsx",
        data_dir / "data" / "filtered" / "ocscore_filtered.csv",
        data_dir / "filtered" / "ocscore_filtered.xlsx",
        data_dir / "filtered" / "ocscore_filtered.csv",
        data_dir / "ocscore.xlsx",
        data_dir / "ocscore.csv",
    ]
    for path in candidates:
        if path.exists():
            if path.suffix.lower() == ".csv":
                return pd.read_csv(path)
            return pd.read_excel(path, header=0)
    return None


def excel_col_letter(idx: int) -> str:
    """1-based column index to Excel column letters."""
    name = ""
    while idx > 0:
        idx, rem = divmod(idx - 1, 26)
        name = chr(65 + rem) + name
    return name


def normalize_ocscore(df: DataFrame) -> DataFrame:
    # Extract only Symbol, Date, and ocscore_* columns
    keep_cols = ["Symbol", "Date"] + [c for c in df.columns if str(c).startswith("ocscore_")]
    df = df[keep_cols].copy()

    if "Symbol" in df.columns:
        df["Symbol"] = df["Symbol"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    if "Date" in df.columns:
        df["Date"] = pd.to_numeric(df["Date"], errors="coerce")

    # Collapse to one row per Symbol-year if needed
    if "Date" in df.columns:
        grouped = df.groupby(["Symbol", "Date"], as_index=False).first()
    else:
        grouped = df

    return grouped


def normalize_all_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize outliers across all Z-Score components.
    Handles data errors and extreme values without removing data.
    Adds diagnostic flags for data quality tracking.
    
    Returns dataframe with:
    - Original columns (preserved)
    - Normalized columns (X1_Normalized, X2_Normalized, etc.)
    - Diagnostic flags (flag_x1_extreme, flag_x4_spike, etc.)
    - AltmanZScore_Normalized (corrected formula with normalized components)
    """
    
    df = df.copy()
    
    # ────────────────────────────────────────────────────────────
    # X1: Working Capital Ratio
    # ────────────────────────────────────────────────────────────
    df["flag_x1_extreme"] = (df["X1_WorkingCapitalToTotalAssets"].abs() > 2.0).astype(int)
    df["X1_Normalized"] = df["X1_WorkingCapitalToTotalAssets"].clip(lower=-1.0, upper=2.0)
    
    # ────────────────────────────────────────────────────────────
    # X2: Profitability Ratio (Net Profit / Total Assets)
    # ────────────────────────────────────────────────────────────
    df["flag_x2_extreme"] = (df["X2_RetainedEarningsToTotalAssets"] < -0.5).astype(int)
    df["X2_Normalized"] = df["X2_RetainedEarningsToTotalAssets"].clip(lower=-0.5)
    x2_p99 = df["X2_RetainedEarningsToTotalAssets"].quantile(0.99)
    df["X2_Normalized"] = df["X2_Normalized"].clip(upper=x2_p99)
    
    # ────────────────────────────────────────────────────────────
    # X3: EBIT Efficiency Ratio
    # ────────────────────────────────────────────────────────────
    df["flag_x3_extreme"] = (df["X3_EBITToTotalAssets"] < -0.3).astype(int)
    df["X3_Normalized"] = df["X3_EBITToTotalAssets"].clip(lower=-0.3)
    x3_p99 = df["X3_EBITToTotalAssets"].quantile(0.99)
    df["X3_Normalized"] = df["X3_Normalized"].clip(upper=x3_p99)
    
    # ────────────────────────────────────────────────────────────
    # X4: Market Value / Liabilities (Company-specific cap)
    # ────────────────────────────────────────────────────────────
    # Calculate historical median (2018-2022 complete data years)
    historical_median = df[df["Date"] <= 2022].groupby("Symbol")["X4_MarketValueToTotalLiabilities"].median()
    df["X4_Cap"] = df["Symbol"].map(lambda s: historical_median.get(s, 10) * 1.5)
    df["flag_x4_spike"] = (df["X4_MarketValueToTotalLiabilities"] > df["X4_Cap"]).astype(int)
    df["flag_x4_consistently_high"] = (df["X4_Cap"] > 50).astype(int)  # Flag extremely high valuations
    df["X4_Normalized"] = df[["X4_MarketValueToTotalLiabilities", "X4_Cap"]].min(axis=1)
    
    # ────────────────────────────────────────────────────────────
    # X5: Asset Turnover (Revenue / Total Assets)
    # ────────────────────────────────────────────────────────────
    # Flag data errors: negative revenue is impossible
    df["flag_x5_negative"] = (df["X5_SalesToTotalAssets"] < 0).astype(int)
    df["X5_Normalized"] = df["X5_SalesToTotalAssets"].clip(lower=0)  # Can't be negative
    x5_p99 = df["X5_Normalized"].quantile(0.99)
    df["X5_Normalized"] = df["X5_Normalized"].clip(upper=x5_p99)
    
    # ────────────────────────────────────────────────────────────
    # Leverage: Total Liabilities / Total Assets
    # ────────────────────────────────────────────────────────────
    df["flag_leverage_extreme"] = (df["Leverage"] > 1.0).astype(int)  # Insolvent on paper
    df["Leverage_Normalized"] = df["Leverage"].clip(upper=1.0)  # Can't exceed 1.0
    
    # ────────────────────────────────────────────────────────────
    # Calculate Normalized Z-Score
    # Uses fixed X5 coefficient (1.0 instead of 0.999)
    # ────────────────────────────────────────────────────────────
    df["AltmanZScore_Normalized"] = (
        1.2 * df["X1_Normalized"] +
        1.4 * df["X2_Normalized"] +
        3.3 * df["X3_Normalized"] +
        0.6 * df["X4_Normalized"] +
        1.0 * df["X5_Normalized"]
    )
    
    # ────────────────────────────────────────────────────────────
    # Data Quality Summary
    # Count total flags per row to identify problematic data
    # ────────────────────────────────────────────────────────────
    df["flag_data_quality_issues"] = (
        df["flag_x1_extreme"] + df["flag_x2_extreme"] +
        df["flag_x3_extreme"] + df["flag_x5_negative"] +
        df["flag_leverage_extreme"] + df["flag_x4_spike"]
    )
    
    return df


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
    roi_numerator = df[REQUIRED_COLUMNS["roi_numerator"]]

    x1 = safe_div(ca - cl, ta)
    x2 = safe_div(re, ta)
    x3 = safe_div(ebit, ta)
    x4 = safe_div(mv, tl)
    x5 = safe_div(sales, ta)

    z = 1.2 * x1 + 1.4 * x2 + 3.3 * x3 + 0.6 * x4 + 1.0 * x5

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
    out["ROI"] = safe_div(roi_numerator, fixed_assets)

    growth_frame = pd.DataFrame({"Symbol": df.get("Symbol"), "Date": df.get("Date"), "sales": sales})
    growth_frame = growth_frame.sort_values(["Symbol", "Date"])
    growth_frame["SalesGrowth"] = growth_frame.groupby("Symbol")[["sales"]].pct_change()
    out["SalesGrowth"] = growth_frame.sort_index()["SalesGrowth"]

    # Apply comprehensive normalization to handle outliers
    out = normalize_all_metrics(out)

    return out


def add_inline_formula_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Append Excel-style formula columns (_formula) alongside values in the merged CSV.

    Formulas use cell references (column letters) based on the existing column order. They are
    stored as strings; when opened in Excel, they will evaluate. Value columns remain unchanged.
    """

    base_cols = list(df.columns)
    col_letter = {col: excel_col_letter(i + 1) for i, col in enumerate(base_cols)}

    def has_cols(*cols: str) -> bool:
        return all(c in col_letter for c in cols)

    df_out = df.copy()
    rows = range(2, len(df_out) + 2)  # Excel rows (header is row 1)

    def add_col(col: str, builder) -> None:
        df_out[col] = [builder(r) for r in rows]

    # Altman components and Z
    if has_cols(
        REQUIRED_COLUMNS["current_assets"],
        REQUIRED_COLUMNS["current_liabilities"],
        REQUIRED_COLUMNS["total_assets"],
        REQUIRED_COLUMNS["retained_earnings"],
        REQUIRED_COLUMNS["operating_profit"],
        REQUIRED_COLUMNS["operating_revenue"],
        REQUIRED_COLUMNS["market_value"],
        REQUIRED_COLUMNS["total_liabilities"],
        REQUIRED_COLUMNS["roi_numerator"],
        REQUIRED_COLUMNS["fixed_assets"],
    ):
        ca = col_letter[REQUIRED_COLUMNS["current_assets"]]
        cl = col_letter[REQUIRED_COLUMNS["current_liabilities"]]
        ta = col_letter[REQUIRED_COLUMNS["total_assets"]]
        re = col_letter[REQUIRED_COLUMNS["retained_earnings"]]
        ebit = col_letter[REQUIRED_COLUMNS["operating_profit"]]
        sales = col_letter[REQUIRED_COLUMNS["operating_revenue"]]
        mv = col_letter[REQUIRED_COLUMNS["market_value"]]
        tl = col_letter[REQUIRED_COLUMNS["total_liabilities"]]
        roi_num = col_letter[REQUIRED_COLUMNS["roi_numerator"]]
        fa = col_letter[REQUIRED_COLUMNS["fixed_assets"]]

        add_col("X1_WorkingCapitalToTotalAssets_formula", lambda r: f"=IFERROR(({ca}{r}-{cl}{r})/{ta}{r},\"\")")
        add_col("X2_RetainedEarningsToTotalAssets_formula", lambda r: f"=IFERROR({re}{r}/{ta}{r},\"\")")
        add_col("X3_EBITToTotalAssets_formula", lambda r: f"=IFERROR({ebit}{r}/{ta}{r},\"\")")
        add_col("X4_MarketValueToTotalLiabilities_formula", lambda r: f"=IFERROR({mv}{r}/{tl}{r},\"\")")
        add_col("X5_SalesToTotalAssets_formula", lambda r: f"=IFERROR({sales}{r}/{ta}{r},\"\")")

        x1c = col_letter.get("X1_WorkingCapitalToTotalAssets")
        x2c = col_letter.get("X2_RetainedEarningsToTotalAssets")
        x3c = col_letter.get("X3_EBITToTotalAssets")
        x4c = col_letter.get("X4_MarketValueToTotalLiabilities")
        x5c = col_letter.get("X5_SalesToTotalAssets")
        if x1c and x2c and x3c and x4c and x5c:
            add_col(
                "AltmanZScore_formula",
                lambda r: f"=1.2*{x1c}{r}+1.4*{x2c}{r}+3.3*{x3c}{r}+0.6*{x4c}{r}+1.0*{x5c}{r}",
            )

        add_col("FirmSize_LogTotalAssets_formula", lambda r: f"=IF({ta}{r}>0,LN({ta}{r}),\"\")")
        add_col("Leverage_formula", lambda r: f"=IFERROR({tl}{r}/{ta}{r},\"\")")
        add_col("ROA_formula", lambda r: f"=IFERROR({col_letter[REQUIRED_COLUMNS['net_profit']]}{r}/{ta}{r},\"\")")
        add_col("FixedAssetsRatio_formula", lambda r: f"=IFERROR({col_letter[REQUIRED_COLUMNS['fixed_assets']]}{r}/{ta}{r},\"\")")
        add_col("ROI_formula", lambda r: f"=IFERROR({roi_num}{r}/{fa}{r},\"\")")

    # Ocscore formula (keeps source values; adds a formula column)
    oc_cols = {
        "Size": "ocscore_Size",
        "TLTA": "ocscore_TLTA",
        "WCTA": "ocscore_WCTA",
        "CLCA": "ocscore_CLCA",
        "NITA": "ocscore_NITA",
        "FUTL": "ocscore_FUTL",
        "INTWO": "ocscore_INTWO",
        "CHIN": "ocscore_CHIN",
        "CPIN": "ocscore_CPIN",
    }
    if has_cols(*oc_cols.values()):
        size = col_letter[oc_cols["Size"]]
        tlta = col_letter[oc_cols["TLTA"]]
        wcta = col_letter[oc_cols["WCTA"]]
        clca = col_letter[oc_cols["CLCA"]]
        nita = col_letter[oc_cols["NITA"]]
        futl = col_letter[oc_cols["FUTL"]]
        intwo = col_letter[oc_cols["INTWO"]]
        chin = col_letter[oc_cols["CHIN"]]
        cpin = col_letter[oc_cols["CPIN"]]
        add_col(
            "ocscore_OScore_formula",
            lambda r: (
                f"=-1.32-(0.407*{size}{r})+(6.03*{tlta}{r})-(1.43*{wcta}{r})+(0.0757*{clca}{r})"
                f"-(2.37*{nita}{r})-(1.83*{futl}{r})+(0.285*{intwo}{r})-(1.72*{chin}{r})-(0.521*{cpin}{r})"
            ),
        )

    return df_out


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

    # Normalize key columns on merged side
    if "Symbol" in merged_df.columns:
        merged_df["Symbol"] = normalize_symbol(merged_df["Symbol"])
    if "Date" in merged_df.columns:
        merged_df["Date"] = pd.to_numeric(merged_df["Date"], errors="coerce")

    # Attach ocscore data after merged file is already collapsed to company-year
    oc_df = load_ocscore(base_dir)
    if oc_df is not None:
        oc_df.attrs["_source_path"] = next((p for p in [base_dir / "data" / "filtered" / "ocscore_filtered.xlsx", base_dir / "ocscore.xlsx"] if p.exists()), None)
        oc_df = normalize_ocscore(oc_df)
        # Drop any existing ocscore_* columns so we can refresh from source
        existing_oc = [c for c in merged_df.columns if c.startswith("ocscore_")]
        if existing_oc:
            merged_df = merged_df.drop(columns=existing_oc)
        merged_df = merged_df.merge(oc_df, how="left", left_on=["Symbol", "Date"], right_on=["Symbol", "Date"])

    result = compute_metrics(merged_df)

    # Build a unified DataFrame with metrics for downstream use (CSV or Excel)
    merged_with = merged_df.copy()
    if "AltmanZ_Band" in merged_with.columns:
        merged_with = merged_with.drop(columns=["AltmanZ_Band"])
    for col in result.columns:
        if col in {"Symbol", "Date"}:
            continue
        merged_with[col] = result[col]

    # Append formula columns alongside values so Excel can recalc when opened
    merged_with = add_inline_formula_columns(merged_with)

    if not args.no_update_merged:
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

    # No separate Excel emitted; merged CSV remains the single output


if __name__ == "__main__":
    main()
