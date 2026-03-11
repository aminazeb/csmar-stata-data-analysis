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


def normalize_symbol(series: pd.Series) -> pd.Series:
    out = series.astype(str).str.strip()
    return out.str.replace(r"\.0$", "", regex=True)


def load_ocscore(data_dir: Path) -> Optional[DataFrame]:
    candidates = [
        data_dir / "filtered" / "ocscore_filtered.xlsx",
        data_dir / "filtered" / "ocscore_filtered.csv",
        data_dir / "ocscore.xlsx",
        data_dir / "ocscore.csv",
    ]
    for path in candidates:
        if path.exists():
            if path.suffix.lower() == ".csv":
                return pd.read_csv(path)
            return pd.read_excel(path, header=None if path.name == "ocscore.xlsx" else 0)
    return None


def excel_col_letter(idx: int) -> str:
    """1-based column index to Excel column letters."""
    name = ""
    while idx > 0:
        idx, rem = divmod(idx - 1, 26)
        name = chr(65 + rem) + name
    return name


def normalize_ocscore(df: DataFrame) -> DataFrame:
    # Detect header row if raw ocscore.xlsx (headerless)
    if "Symbol" not in df.columns:
        # Try to find row containing Symbol marker
        symbol_rows = [i for i, row in df.iterrows() if (row == "Symbol").any()]
        header_idx = symbol_rows[0] if symbol_rows else 0
        df = pd.read_excel(df.attrs.get("_source_path"), header=header_idx) if "_source_path" in df.attrs else df

    # Drop unnamed boilerplate columns
    keep_cols = [c for c in df.columns if not pd.isna(c) and not str(c).startswith("Unnamed")]
    df = df.loc[:, keep_cols].copy()

    if "Symbol" in df.columns:
        df["Symbol"] = normalize_symbol(df["Symbol"])
    if "Date" in df.columns:
        df["Date"] = pd.to_numeric(df["Date"], errors="coerce")

    # Collapse to one row per Symbol-year if needed
    if "Date" in df.columns:
        grouped = df.groupby(["Symbol", "Date"], as_index=False).first()
    else:
        grouped = df

    # Prefix non-key columns to avoid clashes
    key_cols = {"Symbol", "Date"}
    rename_map = {c: f"ocscore_{c}" for c in grouped.columns if c not in key_cols}
    grouped = grouped.rename(columns=rename_map)
    return grouped


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
    ):
        ca = col_letter[REQUIRED_COLUMNS["current_assets"]]
        cl = col_letter[REQUIRED_COLUMNS["current_liabilities"]]
        ta = col_letter[REQUIRED_COLUMNS["total_assets"]]
        re = col_letter[REQUIRED_COLUMNS["retained_earnings"]]
        ebit = col_letter[REQUIRED_COLUMNS["operating_profit"]]
        sales = col_letter[REQUIRED_COLUMNS["operating_revenue"]]
        mv = col_letter[REQUIRED_COLUMNS["market_value"]]
        tl = col_letter[REQUIRED_COLUMNS["total_liabilities"]]

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
                lambda r: f"=1.2*{x1c}{r}+1.4*{x2c}{r}+3.3*{x3c}{r}+0.6*{x4c}{r}+0.999*{x5c}{r}",
            )

        add_col("FirmSize_LogTotalAssets_formula", lambda r: f"=IF({ta}{r}>0,LN({ta}{r}),\"\")")
        add_col("Leverage_formula", lambda r: f"=IFERROR({tl}{r}/{ta}{r},\"\")")
        add_col("ROA_formula", lambda r: f"=IFERROR({col_letter[REQUIRED_COLUMNS['net_profit']]}{r}/{ta}{r},\"\")")
        add_col("FixedAssetsRatio_formula", lambda r: f"=IFERROR({col_letter[REQUIRED_COLUMNS['fixed_assets']]}{r}/{ta}{r},\"\")")

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
        oc_df.attrs["_source_path"] = next((p for p in [base_dir / "filtered" / "ocscore_filtered.xlsx", base_dir / "ocscore.xlsx"] if p.exists()), None)
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
