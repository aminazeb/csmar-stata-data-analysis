import argparse
from pathlib import Path
from typing import Dict, Optional, Sequence, Tuple

import pandas as pd


PRODUCT_COLUMNS: Sequence[str] = (
    "Symbol",
    "EndDate",
    "StatementType",
    "ProductName_EN",
    "Currency",
    "SaleRevenue",
    "SaleRevenueRatio",
    "OperatingCost",
    "OperatingCostRatio",
    "OperatingProfit",
    "OperatingProfitRatio",
    "OperatingMarginRatio",
    "SaleRevenueGrowth",
    "OperatingCostGrowth",
    "OperatingProfitGrowth",
    "OperatingMarginGrowth",
    "ShortName_EN",
    "IndustryCodeC",
    "IndustryCodeD",
    "IndustryCodeB",
    "IndustryCodeA",
    "Stktype",
    "ListedDate",
)

DIV_COLUMNS: Sequence[str] = (
    "Symbol",
    "EndDate",
    "StatementType",
    "ProductName_EN",
    "Currency",
    "SaleRevenue",
    "SaleRevenueRatio",
    "OperatingCost",
    "OperatingCostRatio",
    "OperatingProfit",
    "OperatingProfitRatio",
    "OperatingMarginRatio",
    "SaleRevenueGrowth",
    "OperatingCostGrowth",
    "OperatingProfitGrowth",
    "OperatingMarginGrowth",
    "IsDiversifiedOperations",
    "MainBusinessInvolvedF",
    "MainBusinessInvolvedS",
    "IncomeHHI",
    "IncomeEntropyIndex",
    "ClassificationStandard",
    "ShortName_EN",
    "IndustryCodeC",
    "IndustryCodeD",
    "IndustryCodeB",
    "IndustryCodeA",
    "Stktype",
    "ListedDate",
)


OUTPUT_SOURCE_CANDIDATES = {
    "ProductName_EN": ["mc_pro_ProductName_EN", "ProductName_EN"],
    "Currency": ["mc_pro_Currency", "Currency"],
    "SaleRevenue": ["mc_pro_SaleRevenue", "SaleRevenue"],
    "SaleRevenueRatio": ["mc_pro_SaleRevenueRatio", "SaleRevenueRatio"],
    "OperatingCost": ["mc_pro_OperatingCost", "OperatingCost"],
    "OperatingCostRatio": ["mc_pro_OperatingCostRatio", "OperatingCostRatio"],
    "OperatingProfit": ["mc_pro_OperatingProfit", "OperatingProfit"],
    "OperatingProfitRatio": ["mc_pro_OperatingProfitRatio", "OperatingProfitRatio"],
    "OperatingMarginRatio": ["mc_pro_OperatingMarginRatio", "OperatingMarginRatio"],
    "SaleRevenueGrowth": ["mc_pro_SaleRevenueGrowth", "SaleRevenueGrowth"],
    "OperatingCostGrowth": ["mc_pro_OperatingCostGrowth", "OperatingCostGrowth"],
    "OperatingProfitGrowth": ["mc_pro_OperatingProfitGrowth", "OperatingProfitGrowth"],
    "OperatingMarginGrowth": ["mc_pro_OperatingMarginGrowth", "OperatingMarginGrowth"],
    "IsDiversifiedOperations": ["mc_degree_IsDiversifiedOperations", "IsDiversifiedOperations"],
    "MainBusinessInvolvedF": ["mc_degree_MainBusinessInvolvedF", "MainBusinessInvolvedF"],
    "MainBusinessInvolvedS": ["mc_degree_MainBusinessInvolvedS", "MainBusinessInvolvedS"],
    "IncomeHHI": ["mc_degree_IncomeHHI", "IncomeHHI"],
    "IncomeEntropyIndex": ["mc_degree_IncomeEntropyIndex", "IncomeEntropyIndex"],
}

SALES_CLASS_VALUES = {"1", "2", "4"}
PRODUCT_CLASS_VALUES = {"3"}


def pick_first(df: pd.DataFrame, candidates: Sequence[str], default: Optional[str] = None) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return default


def ensure_cols(df: pd.DataFrame, cols: Sequence[str]) -> pd.DataFrame:
    for c in cols:
        if c not in df.columns:
            df[c] = None
    return df


def normalize_code(series: pd.Series) -> pd.Series:
    return series.astype(str).str.replace(r"\.0$", "", regex=True)


def normalize_classification_standard(series: pd.Series) -> pd.Series:
    out = series.astype("string").str.replace(r"\.0$", "", regex=True)
    out = out.fillna("2")
    out = out.replace({"<NA>": "2", "nan": "2", "None": "2", "": "2"})
    return out.astype(str)


def normalize_state_type_code(series: pd.Series) -> pd.Series:
    out = series.astype("string").str.replace(r"\.0$", "", regex=True)
    out = out.replace({"<NA>": pd.NA, "nan": pd.NA, "None": pd.NA, "": pd.NA})
    return out


def resolve_state_type_code(df: pd.DataFrame, source_col: str) -> pd.Series:
    out = normalize_state_type_code(df[source_col])
    key_cols = ["Symbol", "EndDate"]
    if not all(c in df.columns for c in key_cols):
        return out.fillna("2").astype(str)

    temp = df[key_cols].copy()
    temp["__state"] = out
    has_parent = temp.groupby(key_cols)["__state"].transform(lambda s: (s == "2").any())

    # Only infer missing state as parent when no parent statement exists for that company-year.
    inferred = pd.Series(pd.NA, index=out.index, dtype="string")
    inferred[~has_parent] = "2"
    return out.where(out.notna(), inferred).astype("string")


def load_merged(data_dir: Path) -> pd.DataFrame:
    candidates = [data_dir / "data" / "filtered" / "merged_filtered.csv", data_dir / "filtered" / "merged_filtered.csv", data_dir / "merged_filtered.csv"]
    for p in candidates:
        if p.exists():
            return pd.read_csv(p)
    raise FileNotFoundError("merged_filtered.csv not found in data-dir, data-dir/data/filtered, or data-dir/filtered")


def parse_years(values: Optional[Sequence[str]]) -> Optional[set[int]]:
    if not values:
        return None
    return {int(v) for v in values}


def filter_by_years(df: pd.DataFrame, years: Optional[set[int]]) -> pd.DataFrame:
    if not years:
        return df
    date_col = pick_first(df, ["Date", "EndDate", "Accper"])
    if date_col is None:
        return df
    out = df.copy()
    numeric_years = pd.to_numeric(out[date_col], errors="coerce")
    if numeric_years.notna().any():
        yr = numeric_years.astype("Int64")
    else:
        parsed = pd.to_datetime(out[date_col], errors="coerce")
        yr = parsed.dt.year
    return out[yr.isin(years)].copy()


def add_statement_and_filter(df: pd.DataFrame, state_col: str, statement_type: str) -> pd.DataFrame:
    code = {"parent": "2", "consolidated": "1"}[statement_type]
    codes = normalize_code(df[state_col])
    out = df[codes == code].copy()  # Preserve all merged columns
    out["StatementType"] = "Parent" if statement_type == "parent" else "Consolidated"
    return out


def extract_metadata(df: pd.DataFrame) -> Dict[str, pd.Series]:
    meta = {}
    meta["ShortName_EN"] = df.get(pick_first(df, ["mc_pro_ShortName_EN", "mc_degree_ShortName_EN", "cg_co_Stknme_en"]))
    meta["IndustryCodeC"] = df.get(pick_first(df, ["cg_co_Nnindcd", "cg_co_IndustryCodeC"]))
    meta["IndustryCodeD"] = df.get("cg_co_IndustryCodeD")
    meta["IndustryCodeB"] = df.get("cg_co_Nindcd")
    meta["IndustryCodeA"] = df.get("cg_co_Indcd")
    meta["Stktype"] = df.get("cg_co_Stktype")
    meta["ListedDate"] = df.get("cg_co_ListedDate")
    return meta


def populate_output_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for target, candidates in OUTPUT_SOURCE_CANDIDATES.items():
        source = pick_first(out, candidates)
        if source is not None:
            out[target] = out[source]
    return out


def backfill_company_year_fields(df: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    out = df.copy()
    key_cols = ["Symbol", "EndDate"]
    if not all(c in out.columns for c in key_cols):
        return out

    for col in columns:
        if col in out.columns:
            # Fill blanks from sibling rows of the same company-year when data exists there.
            out[col] = out.groupby(key_cols)[col].transform(lambda s: s.ffill().bfill())
    return out


def build_product_outputs(df: pd.DataFrame, output_dir: Path) -> Tuple[int, int]:
    meta = extract_metadata(df)
    date_col = pick_first(df, ["Date", "EndDate", "Accper"])
    if date_col is None:
        raise KeyError("No date column (Date/EndDate/Accper) found in merged file")

    state_col = pick_first(df, ["mc_pro_StateTypeCode", "StateTypeCode"])
    if state_col is None:
        raise KeyError("StateTypeCode column not found for product data")

    working = df.copy()
    if "EndDate" not in working.columns:
        working["EndDate"] = working[date_col]

    working = populate_output_columns(working)
    working = backfill_company_year_fields(working, PRODUCT_COLUMNS)
    working["StateTypeCode"] = resolve_state_type_code(working, state_col) if state_col in working.columns else "2"
    for k, v in meta.items():
        working[k] = v

    working = ensure_cols(working, PRODUCT_COLUMNS)

    counts = {}
    for st_type, fname in (("parent", "parent_product.csv"), ("consolidated", "consolidated_product.csv")):
        subset = add_statement_and_filter(working, "StateTypeCode", st_type)
        subset = ensure_cols(subset, PRODUCT_COLUMNS)
        out_path = output_dir / fname
        out_path.parent.mkdir(parents=True, exist_ok=True)
        subset.to_csv(out_path, index=False)
        counts[st_type] = len(subset)
    return counts["parent"], counts["consolidated"]


def build_div_outputs(df: pd.DataFrame, output_dir: Path) -> Tuple[int, int, int, int]:
    meta = extract_metadata(df)
    date_col = pick_first(df, ["Date", "EndDate", "Accper"])
    if date_col is None:
        raise KeyError("No date column (Date/EndDate/Accper) found in merged file")
    state_col = pick_first(df, ["mc_degree_StateTypeCode", "StateTypeCode"])
    class_col = pick_first(df, ["mc_degree_ClassificationStandard", "ClassificationStandard"])
    if state_col is None or class_col is None:
        raise KeyError("Missing StateTypeCode or ClassificationStandard for diversification data")

    working = df.copy()
    if "EndDate" not in working.columns:
        working["EndDate"] = working[date_col]

    working = populate_output_columns(working)
    working = backfill_company_year_fields(working, DIV_COLUMNS)
    working["ClassificationStandard"] = normalize_classification_standard(working[class_col]) if class_col in working.columns else "2"
    working["StateTypeCode"] = resolve_state_type_code(working, state_col) if state_col in working.columns else "2"
    working["ClassificationStandard"] = working["ClassificationStandard"].replace({"1": "2", "4": "2"})
    for k, v in meta.items():
        working[k] = v

    working = ensure_cols(working, DIV_COLUMNS)

    counts = {}
    for class_value, tag in (("2", "sales"), ("3", "product")):
        class_df = working[working["ClassificationStandard"] == class_value].copy()
        for st_type, fname in (
            ("parent", f"parent_{tag}_diversification.csv"),
            ("consolidated", f"consolidated_{tag}_diversification.csv"),
        ):
            subset = add_statement_and_filter(class_df, "StateTypeCode", st_type)
            subset = ensure_cols(subset, DIV_COLUMNS)
            out_path = output_dir / fname
            out_path.parent.mkdir(parents=True, exist_ok=True)
            subset.to_csv(out_path, index=False)
            counts[(tag, st_type)] = len(subset)

    return (
        counts.get(("product", "parent"), 0),
        counts.get(("product", "consolidated"), 0),
        counts.get(("sales", "parent"), 0),
        counts.get(("sales", "consolidated"), 0),
    )


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Generate classification outputs from merged_filtered.csv (all columns preserved, including normalized metrics)")
    parser.add_argument("--data-dir", type=Path, default=Path.cwd(), help="Base data directory (looks for data/filtered/merged_filtered.csv or filtered/merged_filtered.csv)")
    parser.add_argument("--output-dir", type=Path, default=None, help="Directory to write outputs (default: <data-dir>/data/filtered/classified or <data-dir>/filtered/classified)")
    parser.add_argument(
        "--years",
        nargs="+",
        default=None,
        help="Optional years to keep before classification (e.g. --years 2018 2019 2020 2021 2022 2023 2024).",
    )
    args = parser.parse_args(argv)

    base_dir = args.data_dir.resolve()
    # Infer output dir based on where merged_filtered.csv was found
    if args.output_dir:
        output_dir = args.output_dir.resolve()
    else:
        # Check where merged file exists to determine output location
        if (base_dir / "data" / "filtered" / "merged_filtered.csv").exists():
            output_dir = (base_dir / "data" / "filtered" / "classified").resolve()
        else:
            output_dir = (base_dir / "filtered" / "classified").resolve()

    merged = load_merged(base_dir)
    target_years = parse_years(args.years)
    merged = filter_by_years(merged, target_years)

    parent_prod, cons_prod = build_product_outputs(merged, output_dir)
    parent_prod_div, cons_prod_div, parent_sales_div, cons_sales_div = build_div_outputs(merged, output_dir)

    print("Source used:", base_dir)
    print("Outputs written to", output_dir)
    print(f"parent_product.csv rows: {parent_prod}")
    print(f"consolidated_product.csv rows: {cons_prod}")
    print(f"parent_product_diversification.csv rows: {parent_prod_div}")
    print(f"consolidated_product_diversification.csv rows: {cons_prod_div}")
    print(f"parent_sales_diversification.csv rows: {parent_sales_div}")
    print(f"consolidated_sales_diversification.csv rows: {cons_sales_div}")


if __name__ == "__main__":
    main()
