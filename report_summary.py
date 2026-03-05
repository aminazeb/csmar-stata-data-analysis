import argparse
from pathlib import Path
from typing import Dict, Optional, Sequence, Tuple

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
    }
    summary: Dict[str, Dict[str, object]] = {}
    for key, path in files.items():
        if not path.exists():
            summary[key] = {"exists": False}
            continue
        df = pd.read_excel(path) if path.suffix.lower() != ".csv" else pd.read_csv(path)
        info: Dict[str, object] = {"exists": True, "rows": len(df)}
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
        "- Coverage intersection: min-years=3 using CG_Co, CG_Ybasic, FS_Combas, FS_Comins, MC_*, BDT_FinDistMertonDD; excluded from coverage calc but still trimmed to the common companies (year-filtered when dated): FS_Comscfd, FS_Comscfi, FN_FN046, OFDI_FININDEX; IFS_IndRegMSELE excluded and filtered by years only"
    )
    lines.append("- Parent-only by default; consolidated included when --allow-consolidated")
    lines.append("- Merged file collapsed to one row per company-year (numeric columns averaged); Date is the year; serial_number is the first column (sequential per Symbol)")
    lines.append("")

    lines.append("Filtered source counts")
    lines.append("----------------------")
    filtered_summary = summarize_filtered_sources(filtered_dir)
    for key, info in sorted(filtered_summary.items()):
        if not info.get("exists"):
            lines.append(f"- {key}: missing")
            continue
        row_txt = f"rows={info.get('rows', 'n/a')}"
        id_txt = f", unique_ids={info.get('unique_ids', 'n/a')}" if "unique_ids" in info else ""
        yr = info.get("year_span")
        yr_txt = f", years={yr[0]}-{yr[1]} ({yr[2]} uniq)" if yr else ""
        lines.append(f"- {key}: {row_txt}{id_txt}{yr_txt}")
    lines.append("")

    lines.append("Merged file summary")
    lines.append("-------------------")
    lines.append(f"Rows: {len(merged_df):,}")
    if "Symbol" in merged_df.columns:
        lines.append(f"Unique companies (Symbol): {merged_df['Symbol'].nunique():,}")
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
    for state_col in ("mc_pro_StateTypeCode", "mc_degree_StateTypeCode", "StateTypeCode"):
        if state_col in merged_df.columns:
            counts = merged_df[state_col].astype(str).value_counts(dropna=True)
            summary = ", ".join([f"{k}:{v}" for k, v in counts.items()])
            lines.append(f"StateTypeCode counts ({state_col}): {summary}")
            break
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
