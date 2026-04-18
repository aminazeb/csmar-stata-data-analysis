import argparse
from pathlib import Path
from typing import Dict, Optional, Sequence, Tuple
from itertools import combinations

import pandas as pd


def load_merged(data_dir: Path) -> Path:
    candidates: Sequence[Path] = [data_dir / "filtered" / "merged_filtered.csv", data_dir / "merged_filtered.csv"]
    for p in candidates:
        if p.exists():
            return p
    raise FileNotFoundError("merged_filtered.csv not found in data-dir or data-dir/filtered")


def detect_id_col(df: pd.DataFrame) -> Optional[str]:
    for c in ("Symbol", "Stkcd", "Stkcd.1"):
        if c in df.columns:
            return c
    return None


def detect_date_col(df: pd.DataFrame) -> Optional[str]:
    for c in ("Date", "Accper", "EndDate", "Enddate", "Reptdt"):
        if c in df.columns:
            return c
    return None


def summarize_years(df: pd.DataFrame, date_col: str) -> Optional[Tuple[int, int, int]]:
    series = df[date_col]

    # First try to interpret as numeric year (to avoid 1970 default when storing year ints)
    years_num = pd.to_numeric(series, errors="coerce")
    mask_year = (years_num >= 1900) & (years_num <= 2100)
    if mask_year.any() and mask_year.sum() >= 0.5 * mask_year.count():
        years = years_num[mask_year].dropna().astype(int)
        if years.empty:
            return None
        return years.min(), years.max(), years.nunique()

    # Fallback to datetime parsing
    dates = pd.to_datetime(series, errors="coerce")
    years = dates.dt.year.dropna().astype(int)
    if years.empty:
        return None
    return years.min(), years.max(), years.nunique()


def normalize_symbol(series: pd.Series) -> pd.Series:
    out = series.astype(str).str.strip()
    return out.replace({"nan": pd.NA}).str.replace(r"\.0$", "", regex=True)


def normalize_year_series(series: pd.Series) -> pd.Series:
    years_num = pd.to_numeric(series, errors="coerce")
    mask_year = (years_num >= 1900) & (years_num <= 2100)
    if mask_year.any() and mask_year.sum() >= 0.5 * mask_year.count():
        return years_num.where(mask_year).astype("Int64")
    dates = pd.to_datetime(series, errors="coerce")
    return dates.dt.year.astype("Int64")


def get_symbol_years_map(df: pd.DataFrame) -> Dict[str, set[int]]:
    id_col = detect_id_col(df)
    date_col = detect_date_col(df)
    if not id_col or not date_col:
        return {}

    temp = df[[id_col, date_col]].copy()
    temp["__symbol"] = normalize_symbol(temp[id_col])
    temp["__year"] = normalize_year_series(temp[date_col])
    temp = temp.dropna(subset=["__symbol", "__year"]).copy()
    temp["__year"] = temp["__year"].astype(int)

    grouped = temp.groupby("__symbol")["__year"].apply(lambda s: set(sorted(s.unique())))
    return grouped.to_dict()


def summarize_classified_diversification(classified_dir: Path) -> Dict[str, Dict[str, object]]:
    files = {
        "parent_product_diversification": classified_dir / "parent_product_diversification.csv",
        "consolidated_product_diversification": classified_dir / "consolidated_product_diversification.csv",
        "parent_sales_diversification": classified_dir / "parent_sales_diversification.csv",
        "consolidated_sales_diversification": classified_dir / "consolidated_sales_diversification.csv",
    }

    summary: Dict[str, Dict[str, object]] = {}
    for key, path in files.items():
        if not path.exists():
            summary[key] = {"exists": False, "path": path}
            continue
        df = pd.read_csv(path, low_memory=False)
        sym_years = get_symbol_years_map(df)
        summary[key] = {
            "exists": True,
            "path": path,
            "rows": len(df),
            "unique_companies": len(sym_years),
            "symbol_years": sym_years,
        }
    return summary


def summarize_filtered_sources(filtered_dir: Path) -> Dict[str, Dict[str, object]]:
    files = {
        "cg_co": filtered_dir / "CG_Co_filtered.xlsx",
        "cg_ybasic": filtered_dir / "CG_Ybasic_filtered.xlsx",
        "fs_combas": filtered_dir / "FS_Combas_filtered.xlsx",
        "fs_comins": filtered_dir / "FS_Comins_filtered.xlsx",
        "fs_comscfd": filtered_dir / "FS_Comscfd_filtered.xlsx",
        "fs_comscfi": filtered_dir / "FS_Comscfi_filtered.xlsx",
        "fn_fn046": filtered_dir / "FN_FN046_filtered.xlsx",
        "mc_degree": filtered_dir / "MC_DiverOperationsDegree_filtered.csv",
        "mc_pro": filtered_dir / "MC_DiverOperationsPro_filtered.csv",
        "bdt_fin": filtered_dir / "BDT_FinDistMertonDD_filtered.xlsx",
        "ofdi_finindex": filtered_dir / "OFDI_FININDEX_filtered.xlsx",
        "ifs_emp": filtered_dir / "IFS_IndRegMSELE_filtered.xlsx",
        "ocscore": filtered_dir / "ocscore_filtered.xlsx",
    }
    friendly = {
        "cg_co": "CG_Co (company metadata)",
        "cg_ybasic": "CG_Ybasic (employees)",
        "fs_combas": "FS_Combas (balance sheet)",
        "fs_comins": "FS_Comins (income statement)",
        "fs_comscfd": "FS_Comscfd (cash flow)",
        "fs_comscfi": "FS_Comscfi (depreciation)",
        "fn_fn046": "FN_FN046 (equity changes)",
        "mc_degree": "MC_DiverOperationsDegree (diversification)",
        "mc_pro": "MC_DiverOperationsPro (product operations)",
        "bdt_fin": "BDT_FinDistMertonDD (market value/distress)",
        "ofdi_finindex": "OFDI_FININDEX (Tobin Q style)",
        "ifs_emp": "IFS_IndRegMSELE (industry employees)",
        "ocscore": "ocscore (O-score inputs)",
    }
    summary: Dict[str, Dict[str, object]] = {}
    for key, path in files.items():
        if not path.exists():
            summary[key] = {"exists": False, "label": friendly.get(key, key)}
            continue
        df = pd.read_excel(path) if path.suffix.lower() != ".csv" else pd.read_csv(path)
        info: Dict[str, object] = {"exists": True, "rows": len(df), "label": friendly.get(key, key)}
        id_col = detect_id_col(df)
        if id_col:
            info["unique_ids"] = df[id_col].astype(str).str.strip().str.replace(r"\.0$", "", regex=True).nunique()
        date_col = detect_date_col(df)
        if date_col:
            yr = summarize_years(df, date_col)
            if yr:
                info["year_span"] = yr
        summary[key] = info
    return summary


def build_report(data_dir: Path) -> str:
    filtered_dir = data_dir / "filtered"
    merged_path = load_merged(data_dir)
    merged_df = pd.read_csv(merged_path)

    lines = []
    lines.append("Filters applied")
    lines.append("---------------")
    lines.append("- Year-end filter: applied to all dated firm-level datasets except IFS_IndRegMSELE")
    lines.append(
        "- Coverage intersection: min-years=3 using CG_Co, CG_Ybasic, FS_Combas, FS_Comins, MC_*, BDT_FinDistMertonDD; excluded from coverage calc but still trimmed to the common companies, year-filtered when dated, and required to meet min-years: FS_Comscfd, FS_Comscfi, FN_FN046, OFDI_FININDEX; IFS_IndRegMSELE excluded and filtered by years only"
    )
    lines.append("- ocscore passthrough (no coverage/year filter); merged later in analytics on Symbol+Date with ocscore_* prefixes")
    lines.append("- Parent-only by default; consolidated included when --allow-consolidated")
    lines.append("- Merged file collapsed to one row per company-year (numeric columns averaged); Date is the year; serial_number is the first column (sequential per Symbol)")
    lines.append("")

    lines.append("Filtered source counts")
    lines.append("----------------------")
    filtered_summary = summarize_filtered_sources(filtered_dir)
    for key, info in sorted(filtered_summary.items()):
        label = info.get("label") or key
        if not info.get("exists"):
            lines.append(f"- {label}: missing")
            continue
        row_txt = f"rows={info.get('rows', 'n/a')}"
        id_txt = f", unique_ids={info.get('unique_ids', 'n/a')}" if "unique_ids" in info else ""
        yr = info.get("year_span")
        yr_txt = f", years={yr[0]}-{yr[1]} ({yr[2]} uniq)" if yr else ""
        lines.append(f"- {label}: {row_txt}{id_txt}{yr_txt}")
    lines.append("")

    lines.append("Merged file summary")
    lines.append("-------------------")
    lines.append(f"Rows: {len(merged_df):,}")
    if "Symbol" in merged_df.columns:
        norm_symbol = normalize_symbol(merged_df["Symbol"])
        lines.append(f"Unique companies (Symbol): {norm_symbol.dropna().nunique():,} (from merged file)")
    if "Date" in merged_df.columns:
        years_numeric = pd.to_numeric(merged_df["Date"], errors="coerce")
        mask_year = (years_numeric >= 1900) & (years_numeric <= 2100)
        if mask_year.any() and mask_year.sum() >= 0.5 * mask_year.count():
            yrs = years_numeric[mask_year].dropna().astype(int)
        else:
            dates = pd.to_datetime(merged_df["Date"], errors="coerce")
            yrs = dates.dt.year.dropna().astype(int)
        if not yrs.empty:
            lines.append(f"Year span: {yrs.min()} - {yrs.max()} ({yrs.nunique()} unique years)")
    oc_cols = [c for c in merged_df.columns if c.startswith("ocscore_")]
    if oc_cols:
        filled = merged_df.get("ocscore_OScore").notna().sum() if "ocscore_OScore" in merged_df.columns else None
        fill_txt = f"; rows with O-score value: {filled:,}" if filled is not None else ""
        lines.append(f"O-score columns present: {len(oc_cols)}{fill_txt}")
    for state_col in ("mc_pro_StateTypeCode", "mc_degree_StateTypeCode", "StateTypeCode"):
        if state_col in merged_df.columns:
            counts = merged_df[state_col].astype(str).value_counts(dropna=True)
            summary = ", ".join([f"{k}:{v}" for k, v in counts.items()])
            lines.append(
                "Statement type from MC data (StateTypeCode: 1=Consolidated, 2=Parent); rows by code: "
                + summary
            )
            break

    classified_dir = filtered_dir / "classified"
    lines.append("")
    lines.append("Classified diversification company summary")
    lines.append("----------------------------------------")
    classified = summarize_classified_diversification(classified_dir)

    for key, info in classified.items():
        label = key.replace("_", " ")
        if not info.get("exists"):
            lines.append(f"- {label}: missing")
            continue
        lines.append(
            f"- {label}: rows={info['rows']}, unique_companies={info['unique_companies']}"
        )

    # Compare overlaps between each product-vs-sales pair for same statement type.
    pair_keys = [
        ("parent_product_diversification", "parent_sales_diversification"),
        ("consolidated_product_diversification", "consolidated_sales_diversification"),
    ]
    for left_key, right_key in pair_keys:
        left = classified.get(left_key, {})
        right = classified.get(right_key, {})
        if not (left.get("exists") and right.get("exists")):
            continue

        left_map = left.get("symbol_years", {})
        right_map = right.get("symbol_years", {})
        overlap = sorted(set(left_map.keys()) & set(right_map.keys()))
        lines.append("")
        lines.append(
            f"Overlap: {left_key} vs {right_key} -> overlapping_companies={len(overlap)}"
        )
        if not overlap:
            continue
        lines.append("company, years_in_left_file, years_in_right_file")
        for sym in overlap:
            left_years = sorted(left_map.get(sym, set()))
            right_years = sorted(right_map.get(sym, set()))
            left_txt = ",".join(str(y) for y in left_years) if left_years else "none"
            right_txt = ",".join(str(y) for y in right_years) if right_years else "none"
            lines.append(f"- {sym}: {left_txt} | {right_txt}")

    # Also provide pairwise overlap across all diversification files.
    available = [k for k, v in classified.items() if v.get("exists")]
    for left_key, right_key in combinations(sorted(available), 2):
        left_map = classified[left_key].get("symbol_years", {})
        right_map = classified[right_key].get("symbol_years", {})
        overlap_count = len(set(left_map.keys()) & set(right_map.keys()))
        lines.append(f"Pair overlap companies: {left_key} vs {right_key} = {overlap_count}")

    return "\n".join(lines)


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Produce summary stats and filter report for merged_filtered.csv")
    parser.add_argument("--data-dir", type=Path, default=Path.cwd(), help="Base data directory (expects filtered/merged_filtered.csv)")
    parser.add_argument("--output", type=Path, default=None, help="Optional path to write the report (default: docs/report_summary.txt)")
    args = parser.parse_args(argv)

    data_dir = args.data_dir.resolve()
    report = build_report(data_dir)
    print(report)

    if args.output:
        out_path = args.output.resolve()
    else:
        repo_root = Path(__file__).resolve().parent
        default_dir = repo_root / "docs"
        default_dir.mkdir(parents=True, exist_ok=True)
        out_path = default_dir / "report_summary.txt"
    out_path.write_text(report)
    print(f"Report written to {out_path}")


if __name__ == "__main__":
    main()
