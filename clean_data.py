import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Set

import pandas as pd


@dataclass
class DatasetConfig:
    key: str
    stem: str
    expected_exts: Sequence[str]
    company_cols: Sequence[str]
    date_cols: Sequence[str]
    filter_col: Optional[str] = None
    filter_keep: Optional[Sequence[str]] = None


DATASETS: List[DatasetConfig] = [
    DatasetConfig(
        key="cg_co",
        stem="CG_Co",
        expected_exts=(".xlsx", ".xls"),
        company_cols=("Stkcd",),
        date_cols=(),  # metadata; no per-year filter
    ),
    DatasetConfig(
        key="fs_combas",
        stem="FS_Combas",
        expected_exts=(".xlsx", ".xls"),
        company_cols=("Stkcd",),
        date_cols=("Accper",),
        filter_col="Typrep",
        filter_keep=("B",),  # B = Parent Statements
    ),
    DatasetConfig(
        key="fs_comins",
        stem="FS_Comins",
        expected_exts=(".xlsx", ".xls"),
        company_cols=("Stkcd",),
        date_cols=("Accper",),
        filter_col="Typrep",
        filter_keep=("B",),  # B = Parent Statements
    ),
    DatasetConfig(
        key="mc_diversified_degree",
        stem="MC_DiverOperationsDegree",
        expected_exts=(".csv", ".xlsx", ".xls"),
        company_cols=("Symbol",),
        date_cols=("EndDate",),
        filter_col="StateTypeCode",
        filter_keep=("2", 2),  # 2 = Parent Company accounting statements
    ),
    DatasetConfig(
        key="mc_diversified_product",
        stem="MC_DiverOperationsPro",
        expected_exts=(".csv", ".xlsx", ".xls"),
        company_cols=("Symbol",),
        date_cols=("EndDate",),
        filter_col="StateTypeCode",
        filter_keep=("2", 2),
    ),
]


def find_input_file(cfg: DatasetConfig, data_dir: Path) -> Path:
    for ext in cfg.expected_exts:
        candidate = data_dir / f"{cfg.stem}{ext}"
        if candidate.exists():
            return candidate
    for ext in cfg.expected_exts:
        matches = sorted(data_dir.glob(f"{cfg.stem}*{ext}"))
        if matches:
            return matches[0]
    raise FileNotFoundError(
        f"No file found for {cfg.stem} with extensions {cfg.expected_exts} in {data_dir}"
    )


def pick_first_existing(df: pd.DataFrame, columns: Sequence[str]) -> str:
    for col in columns:
        if col in df.columns:
            return col
    raise KeyError(f"None of the expected columns are present: {columns}")


def normalize_company_id(series: pd.Series) -> pd.Series:
    # Convert to string, strip spaces, and drop a trailing ".0" from Excel-like numerics.
    out = series.astype(str).str.strip()
    out = out.str.replace(r"\.0$", "", regex=True)
    return out


def read_dataset(path: Path) -> pd.DataFrame:
    if path.suffix.lower() in {".csv"}:
        try:
            return pd.read_csv(path)
        except pd.errors.ParserError:
            # Retry with the python engine and skipping bad lines to handle irregular CSV rows.
            return pd.read_csv(path, engine="python", on_bad_lines="skip")
    return pd.read_excel(path)


def apply_row_filter(df: pd.DataFrame, cfg: DatasetConfig, include_consolidated: bool = False) -> pd.DataFrame:
    if not cfg.filter_col:
        return df
    if cfg.filter_col not in df.columns:
        raise KeyError(f"Expected filter column '{cfg.filter_col}' not found in {cfg.stem}")
    keep_values = {str(v) for v in cfg.filter_keep} if cfg.filter_keep else set()
    if include_consolidated:
        if cfg.key.startswith("mc_"):
            keep_values.update({"1"})
        if cfg.key in {"fs_combas", "fs_comins"}:
            keep_values.update({"A"})
    filtered = df[df[cfg.filter_col].astype(str).isin(keep_values)]
    return filtered


def filter_year_end(df: pd.DataFrame, date_col: Optional[str]) -> pd.DataFrame:
    if not date_col:
        return df
    dates = pd.to_datetime(df[date_col], errors="coerce")
    mask = (dates.dt.month == 12) & (dates.dt.day == 31)
    filtered = df.loc[mask].copy()
    return filtered


def normalize_year(df: pd.DataFrame, date_col: str) -> pd.Series:
    raw = df[date_col]
    # Try strict ISO format first, then fall back to flexible parsing to reduce warnings.
    dates = pd.to_datetime(raw, format="%Y-%m-%d", errors="coerce")
    if dates.isna().all():
        dates = pd.to_datetime(raw, errors="coerce")
    return dates.dt.year


def companies_with_full_years(
    df: pd.DataFrame,
    company_col: str,
    year_col: Optional[str],
    target_years: Set[int],
    min_years: int,
) -> Set[str]:
    temp = df.copy()
    temp[company_col] = normalize_company_id(temp[company_col])
    if year_col is None:
        return set(temp[company_col].dropna().unique())
    temp["__year"] = normalize_year(temp, year_col)
    temp = temp.dropna(subset=[company_col, "__year"])
    year_sets = temp.groupby(company_col)["__year"].apply(lambda s: set(s) & target_years)
    return {company for company, years in year_sets.items() if len(years) >= min_years}


def filter_for_companies_and_years(
    df: pd.DataFrame,
    company_col: str,
    year_col: Optional[str],
    keep_companies: Set[str],
    target_years: Set[int],
) -> pd.DataFrame:
    out = df.copy()
    out[company_col] = normalize_company_id(out[company_col])
    mask = out[company_col].isin(keep_companies)
    if year_col:
        out["__year"] = normalize_year(out, year_col)
        mask &= out["__year"].isin(target_years)
        out = out.drop(columns=["__year"])
    return out.loc[mask].reset_index(drop=True)


def save_dataset(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix.lower() == ".csv":
        df.to_csv(path, index=False)
    else:
        df.to_excel(path, index=False)


def process(
    data_dir: Path,
    output_dir: Path,
    target_years: Set[int],
    min_years: int,
    include_consolidated: bool = False,
    debug: bool = False,
) -> None:
    loaded = {}
    metadata = {}
    for cfg in DATASETS:
        path = find_input_file(cfg, data_dir)
        df = read_dataset(path)
        df = apply_row_filter(df, cfg, include_consolidated=include_consolidated)
        df = filter_year_end(df, cfg.date_cols[0] if cfg.date_cols else None)
        company_col = pick_first_existing(df, cfg.company_cols)
        year_col = pick_first_existing(df, cfg.date_cols) if cfg.date_cols else None
        loaded[cfg.key] = (cfg, path, df, company_col, year_col)
        coverage = companies_with_full_years(df, company_col, year_col, target_years, min_years)
        years_present = None
        if year_col:
            years_series = normalize_year(df, year_col).dropna().astype(int)
            years_present = (years_series.min(), years_series.max(), sorted(years_series.unique())[:10])
        metadata[cfg.key] = {
            "path": path,
            "company_col": company_col,
            "year_col": year_col,
            "coverage": coverage,
            "row_count": len(df),
            "unique_companies": df[company_col].nunique(),
            "years_present": years_present,
        }

    common_companies = set.intersection(*(meta["coverage"] for meta in metadata.values()))
    print(
        f"Companies with coverage across all datasets (>= {min_years} of {sorted(target_years)}): "
        f"{len(common_companies)}"
    )
    for cfg_key, meta in metadata.items():
        yr_info = ""
        if meta["years_present"]:
            yr_min, yr_max, yr_samples = meta["years_present"]
            yr_info = f" years min/max: {yr_min}-{yr_max}"
        print(
            f"- {cfg_key}: rows={meta['row_count']}, unique companies={meta['unique_companies']}, "
            f"companies meeting threshold={len(meta['coverage'])}.{yr_info}"
        )

    if not common_companies:
        raise RuntimeError(
            "No companies meet the coverage requirement across all datasets and target years. "
            "Check counts above; likely some files lack the full year range or have mismatched company codes."
        )

    for cfg_key, (cfg, path, df, company_col, year_col) in loaded.items():
        filtered = filter_for_companies_and_years(df, company_col, year_col, common_companies, target_years)
        output_path = output_dir / f"{path.stem}_filtered{path.suffix}"
        save_dataset(filtered, output_path)
        print(f"Saved filtered {cfg_key} -> {output_path} (rows={len(filtered)})")


def parse_years(values: Iterable[str]) -> Set[int]:
    years = {int(v) for v in values}
    return years


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Filter companies present in all datasets for the target years.")
    parser.add_argument("--data-dir", type=Path, default=Path.cwd(), help="Directory containing the source Excel/CSV files.")
    parser.add_argument(
        "--output-dir", type=Path, default=None, help="Directory to write filtered outputs (default: <data-dir>/filtered)."
    )
    parser.add_argument("--debug", action="store_true", help="Print extra diagnostics about company/year coverage.")
    parser.add_argument(
        "--allow-consolidated",
        action="store_true",
        help="Also keep consolidated statements (StateTypeCode=1 for MC_*, Typrep=A for FS_*). Default keeps parent only.",
    )
    parser.add_argument(
        "--min-years",
        type=int,
        default=3,
        help="Minimum number of target years a company must appear in (default: 3).",
    )
    parser.add_argument(
        "--years",
        nargs="+",
        default=["2018", "2019", "2020", "2021", "2022", "2023", "2024"],
        help="Target years to keep (e.g. --years 2018 2019 2020 2021 2022 2023 2024).",
    )
    args = parser.parse_args(argv)

    target_years = parse_years(args.years)
    data_dir = args.data_dir.resolve()
    output_dir = (args.output_dir or (data_dir / "filtered")).resolve()

    process(
        data_dir=data_dir,
        output_dir=output_dir,
        target_years=target_years,
        min_years=args.min_years,
        include_consolidated=args.allow_consolidated,
        debug=args.debug,
    )


if __name__ == "__main__":
    main()
