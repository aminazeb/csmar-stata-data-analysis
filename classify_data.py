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


def load_merged(data_dir: Path) -> pd.DataFrame:
    candidates = [data_dir / "filtered" / "merged_filtered.csv", data_dir / "merged_filtered.csv"]
    for p in candidates:
        if p.exists():
            return pd.read_csv(p)
    raise FileNotFoundError("merged_filtered.csv not found in data-dir or data-dir/filtered")


def add_statement_and_filter(df: pd.DataFrame, state_col: str, statement_type: str) -> pd.DataFrame:
    code = {"parent": "2", "consolidated": "1"}[statement_type]
    codes = normalize_code(df[state_col])
    out = df[codes == code].copy()
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


def build_product_outputs(df: pd.DataFrame, output_dir: Path) -> Tuple[int, int]:
    meta = extract_metadata(df)
    date_col = pick_first(df, ["Date", "EndDate", "Accper"])
    if date_col is None:
        raise KeyError("No date column (Date/EndDate/Accper) found in merged file")

    base = pd.DataFrame()
    base["Symbol"] = df["Symbol"]
    base["EndDate"] = df[date_col]
    field_map = {
        "ProductName_EN": "mc_pro_ProductName_EN",
        "Currency": "mc_pro_Currency",
        "SaleRevenue": "mc_pro_SaleRevenue",
        "SaleRevenueRatio": "mc_pro_SaleRevenueRatio",
        "OperatingCost": "mc_pro_OperatingCost",
        "OperatingCostRatio": "mc_pro_OperatingCostRatio",
        "OperatingProfit": "mc_pro_OperatingProfit",
        "OperatingProfitRatio": "mc_pro_OperatingProfitRatio",
        "OperatingMarginRatio": "mc_pro_OperatingMarginRatio",
        "SaleRevenueGrowth": "mc_pro_SaleRevenueGrowth",
        "OperatingCostGrowth": "mc_pro_OperatingCostGrowth",
        "OperatingProfitGrowth": "mc_pro_OperatingProfitGrowth",
        "OperatingMarginGrowth": "mc_pro_OperatingMarginGrowth",
    }
    for out_col, src_col in field_map.items():
        base[out_col] = df[src_col] if src_col in df.columns else None

    for k, v in meta.items():
        base[k] = v

    state_col = pick_first(df, ["mc_pro_StateTypeCode", "StateTypeCode"])
    if state_col is None:
        raise KeyError("StateTypeCode column not found for product data")
    base["StateTypeCode"] = normalize_code(df[state_col])

    counts = {}
    for st_type, fname in (("parent", "parent_product.csv"), ("consolidated", "consolidated_product.csv")):
        subset = add_statement_and_filter(base, "StateTypeCode", st_type)
        subset = ensure_cols(subset, PRODUCT_COLUMNS)
        subset = subset.loc[:, PRODUCT_COLUMNS]
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

    base = pd.DataFrame()
    base["Symbol"] = df["Symbol"]
    base["EndDate"] = df[date_col]

    # Bring product-level fields if present (some degree files may not have them; defaults to None)
    product_fields = {
        "ProductName_EN": pick_first(df, ["mc_degree_ProductName_EN", "mc_pro_ProductName_EN"]),
        "Currency": pick_first(df, ["mc_degree_Currency", "mc_pro_Currency"]),
        "SaleRevenue": pick_first(df, ["mc_degree_SaleRevenue", "mc_pro_SaleRevenue"]),
        "SaleRevenueRatio": pick_first(df, ["mc_degree_SaleRevenueRatio", "mc_pro_SaleRevenueRatio"]),
        "OperatingCost": pick_first(df, ["mc_degree_OperatingCost", "mc_pro_OperatingCost"]),
        "OperatingCostRatio": pick_first(df, ["mc_degree_OperatingCostRatio", "mc_pro_OperatingCostRatio"]),
        "OperatingProfit": pick_first(df, ["mc_degree_OperatingProfit", "mc_pro_OperatingProfit"]),
        "OperatingProfitRatio": pick_first(df, ["mc_degree_OperatingProfitRatio", "mc_pro_OperatingProfitRatio"]),
        "OperatingMarginRatio": pick_first(df, ["mc_degree_OperatingMarginRatio", "mc_pro_OperatingMarginRatio"]),
        "SaleRevenueGrowth": pick_first(df, ["mc_degree_SaleRevenueGrowth", "mc_pro_SaleRevenueGrowth"]),
        "OperatingCostGrowth": pick_first(df, ["mc_degree_OperatingCostGrowth", "mc_pro_OperatingCostGrowth"]),
        "OperatingProfitGrowth": pick_first(df, ["mc_degree_OperatingProfitGrowth", "mc_pro_OperatingProfitGrowth"]),
        "OperatingMarginGrowth": pick_first(df, ["mc_degree_OperatingMarginGrowth", "mc_pro_OperatingMarginGrowth"]),
    }
    for out_col, src_col in product_fields.items():
        base[out_col] = df[src_col] if src_col else None

    base["IsDiversifiedOperations"] = df.get(pick_first(df, ["mc_degree_IsDiversifiedOperations"]))
    base["MainBusinessInvolvedF"] = df.get(pick_first(df, ["mc_degree_MainBusinessInvolvedF"]))
    base["MainBusinessInvolvedS"] = df.get(pick_first(df, ["mc_degree_MainBusinessInvolvedS"]))
    base["IncomeHHI"] = df.get(pick_first(df, ["mc_degree_IncomeHHI"]))
    base["IncomeEntropyIndex"] = df.get(pick_first(df, ["mc_degree_IncomeEntropyIndex"]))
    base["ClassificationStandard"] = normalize_code(df[class_col])
    base["StateTypeCode"] = normalize_code(df[state_col])

    for k, v in meta.items():
        base[k] = v

    counts = {}
    for class_value, tag in (("2", "sales"), ("3", "product")):
        class_df = base[base["ClassificationStandard"] == class_value].copy()
        for st_type, fname in (
            ("parent", f"parent_{tag}_diversification.csv"),
            ("consolidated", f"consolidated_{tag}_diversification.csv"),
        ):
            subset = add_statement_and_filter(class_df, "StateTypeCode", st_type)
            subset = ensure_cols(subset, DIV_COLUMNS)
            subset = subset.loc[:, DIV_COLUMNS]
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
    parser = argparse.ArgumentParser(description="Generate classification outputs from merged_filtered.csv")
    parser.add_argument("--data-dir", type=Path, default=Path.cwd(), help="Base data directory (looks for filtered/merged_filtered.csv)")
    parser.add_argument("--output-dir", type=Path, default=None, help="Directory to write outputs (default: <data-dir>/filtered/classified)")
    args = parser.parse_args(argv)

    base_dir = args.data_dir.resolve()
    output_dir = (args.output_dir or (base_dir / "filtered" / "classified")).resolve()

    merged = load_merged(base_dir)

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
