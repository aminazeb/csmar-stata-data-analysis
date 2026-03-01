# Data Filtering Helper

This project filters multiple datasets to keep companies present across them with sufficient year coverage (Dec 31 rows for firm-level data), merges them into one wide file, computes Altman Z and related financial ratios, and classifies into parent/consolidated sales and product diversification outputs with company metadata.

## Files expected (in --data-dir)

- CG_Co.(xlsx|xls)
- CG_Ybasic.(xlsx|xls) — employees per firm
- FS_Combas.(xlsx|xls)
- FS_Comins.(xlsx|xls)
- FS_Comscfd.(xlsx|xls)
- FS_Comscfi.(xlsx|xls)
- FN_FN046.(xlsx|xls)
- MC_DiverOperationsDegree.(csv|xlsx|xls)
- MC_DiverOperationsPro.(csv|xlsx|xls)
- BDT_FinDistMertonDD.(xlsx|xls)
- OFDI_FININDEX.(xlsx|xls)
- IFS_IndRegMSELE.(xlsx|xls) — industry-level employees (joins by industry code + year; excluded from coverage intersection)

## What clean_data.py does

1. Load each file (CSV or Excel).
2. Filter to parent statements by default:

- FS_Combas, FS_Comins: `Typrep = B` (add consolidated with `--allow-consolidated`).
- MC\_\*: `StateTypeCode = 2` (add consolidated with `--allow-consolidated`).

3. Keep only year-end rows (Dec 31) for dated firm-level files; industry-level IFS data keeps all years (only filtered by target years).
4. Normalize company IDs (strip, drop trailing `.0`).
5. For coverage-participating dated files, keep companies that appear in at least `min_years` of the target years (default 3). Coverage calculation uses only CG*Co, CG_Ybasic, FS_Combas, FS_Comins, MC*\*, and BDT_FinDistMertonDD.
6. Intersect companies across those coverage-participating datasets. Firm-level files excluded from the coverage calculation (FS_Comscfd, FS_Comscfi, FN_FN046, OFDI_FININDEX) are still trimmed to that common company set and filtered by target years when dated; IFS_IndRegMSELE is excluded from coverage and filtered by target years only.
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

Recommended sequence (clean → merge → metrics → classify → summary):

```bash
# 1) Filter raw sources (parent-only)
/Users/air/Documents/statadata/.venv/bin/python clean_data.py --data-dir /Users/air/Documents/statadata/data

#    If you want consolidated too, add: --allow-consolidated

# 2) Merge filtered files into one wide file
/Users/air/Documents/statadata/.venv/bin/python merge_filtered.py --data-dir /Users/air/Documents/statadata/data

# 3) Compute Altman Z + derived metrics and append to merged
/Users/air/Documents/statadata/.venv/bin/python apply_analytics.py --data-dir /Users/air/Documents/statadata/data

# 4) Classify from the merged file into product and diversification outputs
/Users/air/Documents/statadata/.venv/bin/python classify_data.py --data-dir /Users/air/Documents/statadata/data

# 5) Generate a summary report of filters and counts (writes to docs/report_summary.txt by default)
/Users/air/Documents/statadata/.venv/bin/python report_summary.py --data-dir /Users/air/Documents/statadata/data
```

What each step produces:

- Step 1 (clean_data.py): filtered source files in `<data-dir>/filtered`, applying year-end (Dec 31) where applicable, year coverage, and parent-only by default; `--allow-consolidated` keeps consolidated too. IFS is filtered only by target years.
- Step 2 (merge*filtered.py): a wide outer-join `merged_filtered.csv` in `<data-dir>/filtered`, retaining all rows from each filtered source; CG_Ybasic fields are prefixed `cg_ybasic*`, BDT `bdt*fin*`, OFDI `ofdi*finindex*`, industry employees join by year + industry code with `ifs_EmployeeNum`/`ifs_LegalEntityNum`.
- Step 3 (apply_analytics.py): appends AltmanZScore, X1–X5 components, FirmSize_LogTotalAssets, Leverage, ROA, FixedAssetsRatio, SalesGrowth into merged_filtered.csv. Add `--output PATH` only if you also want a standalone metrics CSV.
- Step 4 (classify_data.py): classified outputs in `<data-dir>/filtered/classified`:
  - parent_product_diversification.csv / consolidated_product_diversification.csv (ClassificationStandard=3 + diversification metrics)
  - parent_sales_diversification.csv / consolidated_sales_diversification.csv (ClassificationStandard=2 + diversification metrics)
- Step 5 (report_summary.py): summary of filters, per-source counts, merged stats written to `docs/report_summary.txt` by default (or custom `--output`).

Options (clean_data.py):

- `--data-dir DIR` : folder containing the source files (default: cwd).
- `--output-dir DIR` : where to write filtered outputs (default: <data-dir>/filtered).
- `--years Y1 Y2 ...` : target years (default: 2018 2019 2020 2021 2022 2023 2024).
- `--min-years N` : minimum count of target years required per company per dated file (default: 3). Coverage uses CG*Co, CG_Ybasic, FS_Combas, FS_Comins, MC*\*, BDT_FinDistMertonDD; FS_Comscfd, FS_Comscfi, FN_FN046, OFDI_FININDEX are excluded from coverage calc but still trimmed to the common companies (year-filtered when dated); IFS_IndRegMSELE is excluded and filtered by target years only.
- `--allow-consolidated` : also keep consolidated statements (MC StateTypeCode=1, FS Typrep=A). Default keeps parent only.
- `--debug` : print coverage stats per dataset and intersection size.

Options (merge_filtered.py):

- `--data-dir DIR` : base directory; script looks in `<data-dir>/filtered` first. Output defaults to `<data-dir>/filtered/merged_filtered.csv`.

Options (classify_data.py):

- `--data-dir DIR` : base directory; script looks for `filtered/merged_filtered.csv` (falls back to `merged_filtered.csv` in base). Output defaults to `<data-dir>/filtered/classified`.

Options (apply_analytics.py):

- `--data-dir DIR` : base directory; script looks for `filtered/merged_filtered.csv` (falls back to `merged_filtered.csv` in base).
- `--output PATH` : optional separate Altman/analytics CSV; omit to only update merged_filtered.csv.
- `--no-update-merged` : skip writing derived columns back into merged_filtered.csv.

Options (report_summary.py):

- `--data-dir DIR` : base directory; script looks for `filtered/merged_filtered.csv` (falls back to `merged_filtered.csv` in base).
- `--output PATH` : optional path to write the report; defaults to `docs/report_summary.txt` alongside the scripts.

Options (report_summary.py):

- `--data-dir DIR` : base directory; script looks for `filtered/merged_filtered.csv` (falls back to `merged_filtered.csv` in base).
- `--output PATH` : optional path to write the report; defaults to `docs/report_summary.txt` alongside the scripts.

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
- Additional merged columns (e.g., employees from CG_Ybasic, distress/market value from BDT, TobinQ from OFDI, industry employees from IFS) remain available in merged_filtered.csv and the Altman metrics output.

## Notes

- Year filtering uses the first date column per file (Accper for FS*\*, EndDate for MC*\*).
- Company join: FS*\* `Stkcd` matches MC*\* `Symbol`; CG_Co uses `Stkcd` for presence only.
- Classification uses the same ID normalization, filters Dec 31 rows, and splits by StateTypeCode (2 parent, 1 consolidated). Company metadata (ShortName_EN, IndustryCodeC) is joined when available.
- Altman/metrics use Dec 31 rows and rely on FS/MC/BDT/OFDI fields; outputs are appended to merged (a separate CSV is only written if `--output` is provided).
- CSVs with bad rows are retried with python engine and `on_bad_lines="skip"`.
- Only Dec 31 rows are kept for dated files; change `filter_year_end` if your fiscal year-end differs.
