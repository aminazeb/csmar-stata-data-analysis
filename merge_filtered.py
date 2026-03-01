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


def build_spine(dfs: Sequence[pd.DataFrame], key_cols: Sequence[str]) -> pd.DataFrame:
    parts = []
    for df in dfs:
        sub = df[list(key_cols)].drop_duplicates()
        parts.append(sub)
    return pd.concat(parts, ignore_index=True).drop_duplicates()


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

    # Normalize company id
    for key, df in dfs.items():
        if "Stkcd" in df.columns:
            df["Symbol"] = normalize_company_id(df["Stkcd"])
        elif "Symbol" in df.columns:
            df["Symbol"] = normalize_company_id(df["Symbol"])

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

    # Build spine from dated sources (skip industry-level employees which lack Symbol)
    dated_keys = [k for k, col in date_map.items() if col and k in dfs and k != "ifs_emp"]
    spine_sources = []
    for k in dated_keys:
        declared_col = date_map[k]
        df = dfs[k]
        actual_col = declared_col
        if actual_col not in df.columns:
            for fallback in ("Accper", "Accper.1", "EndDate", "EndDate.1"):
                if fallback in df.columns:
                    actual_col = fallback
                    break
        needed = ["Symbol", actual_col]
        missing = [c for c in needed if c not in df.columns]
        if missing:
            raise KeyError(f"Missing required column(s) {missing} in {k}")
        spine_sources.append(df[["Symbol", actual_col]].rename(columns={actual_col: "Date"}))
    spine = build_spine(spine_sources, ["Symbol", "Date"]) if spine_sources else pd.DataFrame(columns=["Symbol", "Date"])

    # Start merged with spine
    merged = spine.copy()

    # Join each source
    # Industry employees handled separately later
    for key, df in list(dfs.items()):
        if key == "ifs_emp":
            continue
        declared_col = date_map.get(key)
        date_col = declared_col
        if declared_col and declared_col not in df.columns:
            for fallback in ("Accper", "Accper.1", "EndDate", "EndDate.1"):
                if fallback in df.columns:
                    date_col = fallback
                    break
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
