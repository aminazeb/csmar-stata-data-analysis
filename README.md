# Data Filtering Helper

This project filters five datasets to keep companies present across them with sufficient year coverage (Dec 31 rows), then merges and classifies them into parent/consolidated sales and product diversification outputs with company metadata.

## Files expected (in --data-dir)

- CG_Co.(xlsx|xls)
- FS_Combas.(xlsx|xls)
- FS_Comins.(xlsx|xls)
- MC_DiverOperationsDegree.(csv|xlsx|xls)
- MC_DiverOperationsPro.(csv|xlsx|xls)

## What clean_data.py does

1. Load each file (CSV or Excel).
2. Filter to parent statements only:
   - FS_Combas, FS_Comins: `Typrep = B`.
   - MC\_\*: `StateTypeCode = 2`.
3. Keep only year-end rows (month=12, day=31) for dated files.
4. Normalize company IDs (strip, drop trailing `.0`).
5. For each dated file, keep companies that appear in at least `min_years` of the target years.
6. Intersect companies across all datasets (CG_Co only checks presence, no dates).
7. Write filtered outputs to `filtered/` under the data dir.

## Run commands

From the repo root:

```bash
# Default: years 2018-2024, min 3 years coverage, parent-only, year-end only
/Users/air/Documents/statadata/.venv/bin/python clean_data.py --data-dir /Users/air/Documents/statadata/data --debug
```

To generate the four classified files (sales and product diversification, parent and consolidated):

```bash
/Users/air/Documents/statadata/.venv/bin/python classify_data.py --data-dir /Users/air/Documents/statadata/data
```

Recommended sequence (clean → merge → classify):

```bash
# 1) Filter raw sources (parent-only)
/Users/air/Documents/statadata/.venv/bin/python clean_data.py --data-dir /Users/air/Documents/statadata/data

#    If you want consolidated too, add: --allow-consolidated

# 2) Merge filtered files into one wide file
/Users/air/Documents/statadata/.venv/bin/python merge_filtered.py --data-dir /Users/air/Documents/statadata/data

# 3) Classify from the merged file into product and diversification outputs
/Users/air/Documents/statadata/.venv/bin/python classify_data.py --data-dir /Users/air/Documents/statadata/data
```

What each step produces:

- Step 1 (clean_data.py): filtered source files in `<data-dir>/filtered`, applying year-end (Dec 31), year coverage, and (by default) parent-only; `--allow-consolidated` keeps consolidated too.
- Step 2 (merge_filtered.py): a wide outer-join `merged_filtered.csv` in `<data-dir>/filtered`, retaining all rows from each filtered source.
- Step 3 (classify_data.py): classified outputs in `<data-dir>/filtered/classified`:
  - parent_product_diversification.csv / consolidated_product_diversification.csv (ClassificationStandard=3 + diversification metrics)
  - parent_sales_diversification.csv / consolidated_sales_diversification.csv (ClassificationStandard=2 + diversification metrics)

Options (clean_data.py):

- `--data-dir DIR` : folder containing the five source files (default: cwd).
- `--output-dir DIR` : where to write filtered outputs (default: <data-dir>/filtered).
- `--years Y1 Y2 ...` : target years (default: 2018 2019 2020 2021 2022 2023 2024).
- `--min-years N` : minimum count of target years required per company per dated file (default: 3).
- `--allow-consolidated` : also keep consolidated statements (MC StateTypeCode=1, FS Typrep=A). Default keeps parent only.
- `--debug` : print coverage stats per dataset and intersection size.

Options (merge_filtered.py):

- `--data-dir DIR` : base directory; script looks in `<data-dir>/filtered` first. Output defaults to `<data-dir>/filtered/merged_filtered.csv`.

Options (classify_data.py):

- `--data-dir DIR` : base directory; script looks for `filtered/merged_filtered.csv` (falls back to `merged_filtered.csv` in base). Output defaults to `<data-dir>/filtered/classified`.

## Outputs

Written to `filtered/` (or your `--output-dir`), one file per source, suffixed `_filtered` and same extension.

The classify step writes to `<data-dir>/filtered/classified` by default:

- parent_product_diversification.csv (ClassificationStandard=3)
- consolidated_product_diversification.csv (ClassificationStandard=3)
- parent_sales_diversification.csv (ClassificationStandard=2)
- consolidated_sales_diversification.csv (ClassificationStandard=2)

Each classified file includes:

- Symbol (normalized company ID) and EndDate (Dec 31 only)
- StatementType: Parent or Consolidated (from StateTypeCode 2 or 1)
- Product-level fields from MC_DiverOperationsPro: ProductName_EN, Currency, revenue/cost/profit, ratios, and growth metrics
- Diversification fields from MC_DiverOperationsDegree (scope depends on ClassificationStandard): IsDiversifiedOperations, MainBusinessInvolvedF/MainBusinessInvolvedS, IncomeHHI, IncomeEntropyIndex, ClassificationStandard
- Company metadata from CG_Co: ShortName_EN, IndustryCodeC (2012 CSRC)

## Notes

- Year filtering uses the first date column per file (Accper for FS*\*, EndDate for MC*\*).
- Company join: FS*\* `Stkcd` matches MC*\* `Symbol`; CG_Co uses `Stkcd` for presence only.
- Classification uses the same ID normalization, filters Dec 31 rows, and splits by StateTypeCode (2 parent, 1 consolidated). Company metadata (ShortName_EN, IndustryCodeC) is joined when available.
- CSVs with bad rows are retried with python engine and `on_bad_lines="skip"`.
- Only Dec 31 rows are kept for dated files; change `filter_year_end` if your fiscal year-end differs.
