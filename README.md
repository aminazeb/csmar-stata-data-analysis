# Data Filtering Helper

This project filters five datasets to keep companies present across them with sufficient year coverage. It targets parent-company statements and year-end (Dec 31) rows only.

## Files expected (in --data-dir)

- CG_Co.(xlsx|xls)
- FS_Combas.(xlsx|xls)
- FS_Comins.(xlsx|xls)
- MC_DiverOperationsDegree.(csv|xlsx|xls)
- MC_DiverOperationsPro.(csv|xlsx|xls)

## What the script does

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

Options:

- `--data-dir DIR` : folder containing the five source files (default: cwd).
- `--output-dir DIR` : where to write filtered outputs (default: <data-dir>/filtered).
- `--years Y1 Y2 ...` : target years (default: 2018 2019 2020 2021 2022 2023 2024).
- `--min-years N` : minimum count of target years required per company per dated file (default: 3).
- `--debug` : print coverage stats per dataset and intersection size.

## Outputs

Written to `filtered/` (or your `--output-dir`), one file per source, suffixed `_filtered` and same extension.

## Notes

- Year filtering uses the first date column per file (Accper for FS*\*, EndDate for MC*\*).
- Company join: FS*\* `Stkcd` matches MC*\* `Symbol`; CG_Co uses `Stkcd` for presence only.
- CSVs with bad rows are retried with python engine and `on_bad_lines="skip"`.
- Only Dec 31 rows are kept for dated files; change `filter_year_end` if your fiscal year-end differs.
