import argparse
from pathlib import Path
from typing import Dict, Optional, Sequence

import pandas as pd


# Utility helpers

def normalize_company_id(series: pd.Series) -> pd.Series:
    out = series.astype(str).str.strip()
    return out.str.replace(r"\.0$", "", regex=True)


def read_any(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    return pd.read_excel(path)


def ensure_col(df: pd.DataFrame, col: str) -> pd.DataFrame:
    if col not in df.columns:
        df[col] = None
    return df


def resolve_date_col(df: pd.DataFrame, declared: Optional[str]) -> Optional[str]:
    if declared and declared in df.columns:
        return declared
    for fallback in ("Accper", "Accper.1", "EndDate", "EndDate.1", "Enddate", "Reptdt"):
        if fallback in df.columns:
            return fallback
    return None


def coerce_numeric_columns(df: pd.DataFrame, group_cols: set) -> tuple[pd.DataFrame, Sequence[str]]:
    numeric_cols = []
    for col in df.columns:
        if col in group_cols:
            continue
        coerced = pd.to_numeric(df[col], errors="coerce")
        if coerced.notna().any():
            df[col] = coerced
            numeric_cols.append(col)
    return df, numeric_cols


def collapse_company_year(df: pd.DataFrame, company_col: str, date_col: str) -> pd.DataFrame:
    """Collapse to one row per company-year using mean for numeric columns and first for others."""
    temp = df.copy()
    dates = pd.to_datetime(temp[date_col], errors="coerce")
    temp = temp.loc[~dates.isna()].copy()
    if temp.empty:
        return temp
    temp[company_col] = normalize_company_id(temp[company_col])
    temp[date_col] = dates.dt.year.astype("Int64")

    group_cols = {company_col, date_col}
    temp, numeric_cols = coerce_numeric_columns(temp, group_cols)
    agg: Dict[str, str] = {col: "mean" for col in numeric_cols}
    other_cols = [c for c in temp.columns if c not in group_cols and c not in numeric_cols]
    agg.update({col: "first" for col in other_cols})

    collapsed = temp.groupby(list(group_cols), as_index=False).agg(agg)
    return collapsed


def build_spine(dfs: Sequence[pd.DataFrame], key_cols: Sequence[str]) -> pd.DataFrame:
    parts = []
    for df in dfs:
        sub = df[list(key_cols)].drop_duplicates()
        parts.append(sub)
    return pd.concat(parts, ignore_index=True).drop_duplicates()


def assert_source_columns_retained(
    merged: pd.DataFrame,
    source_key: str,
    source_df: pd.DataFrame,
    key_cols: set,
) -> None:
    """Fail fast if any non-key source columns were not carried into the merged frame."""

    # source_df is already renamed before this check, so compare directly.
    expected = {c for c in source_df.columns if c not in key_cols}
    missing = sorted(expected - set(merged.columns))
    if missing:
        raise RuntimeError(
            f"Merged output is missing {len(missing)} column(s) from {source_key}: {missing}"
        )


def merge_filtered(data_dir: Path, output_path: Path) -> None:
    # Prefer a filtered subfolder if present
    filtered_dir = data_dir / "filtered"
    source_dir = filtered_dir if filtered_dir.exists() else data_dir

    # Expected filtered files
    files = {
        "cg_co": source_dir / "CG_Co_filtered.xlsx",
        "cg_ybasic": source_dir / "CG_Ybasic_filtered.xlsx",
        "fs_combas": source_dir / "FS_Combas_filtered.xlsx",
        "fs_comins": source_dir / "FS_Comins_filtered.xlsx",
        "fs_comscfd": source_dir / "FS_Comscfd_filtered.xlsx",
        "fs_comscfi": source_dir / "FS_Comscfi_filtered.xlsx",
        "fn_fn046": source_dir / "FN_FN046_filtered.xlsx",
        "mc_degree": source_dir / "MC_DiverOperationsDegree_filtered.csv",
        "mc_pro": source_dir / "MC_DiverOperationsPro_filtered.csv",
        "bdt_fin": source_dir / "BDT_FinDistMertonDD_filtered.xlsx",
        "ofdi_finindex": source_dir / "OFDI_FININDEX_filtered.xlsx",
        "ifs_emp": source_dir / "IFS_IndRegMSELE_filtered.xlsx",
    }

    # Load available files
    dfs: Dict[str, pd.DataFrame] = {}
    for key, path in files.items():
        if path.exists():
            df = read_any(path)
            dfs[key] = df
        else:
            alt = list(data_dir.glob(path.name.replace(".xlsx", ".csv")))
            if alt:
                df = read_any(alt[0])
                dfs[key] = df
            elif key == "ifs_emp":
                # Fall back to unfiltered industry employees file if filtered not present
                raw = data_dir / "IFS_IndRegMSELE.xlsx"
                if raw.exists():
                    dfs[key] = read_any(raw)

    if not dfs:
        raise FileNotFoundError(f"No filtered files found in {source_dir}")

    # Determine date columns per source (fallbacks if renamed)
    date_map: Dict[str, Optional[str]] = {
        "fs_combas": "Accper",
        "fs_comins": "Accper",
        "fs_comscfd": "Accper",
        "fs_comscfi": "Accper",
        "fn_fn046": "Accper",
        "mc_degree": "EndDate",
        "mc_pro": "EndDate",
        "bdt_fin": "Enddate",
        "ofdi_finindex": "EndDate",
        "cg_ybasic": "Reptdt",
        "cg_co": None,
        "ifs_emp": None,
    }

    # Normalize company id and collapse to one row per company-year where a date is present
    resolved_date_cols: Dict[str, Optional[str]] = {}
    for key, df in dfs.items():
        if "Stkcd" in df.columns:
            df["Symbol"] = normalize_company_id(df["Stkcd"])
        elif "Symbol" in df.columns:
            df["Symbol"] = normalize_company_id(df["Symbol"])

        declared_col = date_map.get(key)
        date_col = resolve_date_col(df, declared_col)
        resolved_date_cols[key] = date_col

        if date_col:
            dfs[key] = collapse_company_year(df, "Symbol", date_col)

    # Build spine from dated sources (skip industry-level employees which lack Symbol)
    dated_keys = [k for k, col in resolved_date_cols.items() if col and k in dfs and k != "ifs_emp"]
    spine_sources = []
    for k in dated_keys:
        date_col = resolved_date_cols[k]
        df = dfs[k]
        needed = ["Symbol", date_col]
        missing = [c for c in needed if c not in df.columns]
        if missing:
            raise KeyError(f"Missing required column(s) {missing} in {k}")
        spine_sources.append(df[["Symbol", date_col]].rename(columns={date_col: "Date"}))
    spine = build_spine(spine_sources, ["Symbol", "Date"]) if spine_sources else pd.DataFrame(columns=["Symbol", "Date"])

    # Start merged with spine
    merged = spine.copy()

    # Join each source
    # Industry employees handled separately later
    for key, df in list(dfs.items()):
        if key == "ifs_emp":
            continue
        date_col = resolved_date_cols.get(key)
        left_on = ["Symbol", "Date"] if date_col else ["Symbol"]
        right_on = ["Symbol", "Date"] if date_col else ["Symbol"]
        temp = df.copy()
        if date_col:
            temp = temp.rename(columns={date_col: "Date"})
        # Avoid column clashes: prefix all non-key columns with source key
        key_cols = set(right_on)
        rename_cols = {c: f"{key}_{c}" for c in temp.columns if c not in key_cols}
        temp = temp.rename(columns=rename_cols)
        merged = merged.merge(temp, how="left", left_on=left_on, right_on=right_on)
        assert_source_columns_retained(merged, key, temp, key_cols)

    # Optionally enrich with industry-level employees using industry code and year
    if "ifs_emp" in dfs:
        ind_df = dfs["ifs_emp"].copy()
        if "IndustryCode" in ind_df.columns and "SgnYear" in ind_df.columns:
            merged["__Year"] = pd.to_datetime(merged["Date"], errors="coerce").dt.year.astype("Int64")
            ind_df["SgnYear"] = pd.to_numeric(ind_df["SgnYear"], errors="coerce").astype("Int64")
            ind_df = ind_df.rename(columns={"LegalEntityNum": "ifs_LegalEntityNum", "EmployeeNum": "ifs_EmployeeNum"})
            merged = merged.merge(
                ind_df,
                how="left",
                left_on=["__Year", "cg_co_Nnindcd"],
                right_on=["SgnYear", "IndustryCode"],
            )
            merged = merged.drop(columns=[c for c in ["__Year", "SgnYear", "IndustryCode"] if c in merged.columns])

    # Assign a unique serial per company to make counts explicit
    unique_symbols = sorted(merged["Symbol"].dropna().unique())
    company_id_map = {sym: idx + 1 for idx, sym in enumerate(unique_symbols)}
    merged["serial_number"] = merged["Symbol"].map(company_id_map)
    # Ensure serial_number is the first column
    ordered_cols = ["serial_number"] + [c for c in merged.columns if c != "serial_number"]
    merged = merged[ordered_cols]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(output_path, index=False)
    print(f"Merged file written to {output_path} (rows={len(merged)})")


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Outer-merge filtered datasets without dropping rows.")
    parser.add_argument("--data-dir", type=Path, default=Path.cwd(), help="Directory containing filtered outputs.")
    parser.add_argument("--output", type=Path, default=None, help="Output file (default: <data-dir>/filtered/merged_filtered.csv)")
    args = parser.parse_args(argv)

    data_dir = args.data_dir.resolve()
    output = args.output or (data_dir / "filtered" / "merged_filtered.csv")
    merge_filtered(data_dir, output)


if __name__ == "__main__":
    main()
