# QuantLab Data Pipeline

Lean data ingestion and access layer for equity datasets (WRDS + FRED). Builds S&P 500 datasets locally (including delisted names) with clean Parquet tables and a simple handler.

## What’s inside
- Unified `DataHandler` interface for downstream consumers.
- Local Parquet storage adapter (`LocalParquetDataHandler`).
- WRDS-backed ingestion (`wrds_ingestion`) for S&P 500 (prints step-by-step progress with timings):
  - CRSP DSF prices/returns (daily with `shrout`, `cfacpr` → `adj_close`), MSF monthly prices/returns.
  - S&P500 membership from `crsp.dsp500list` (permno-based, includes delisted names).
  - Delist adjustments from `crsp.StkDelists` (`delret`/`dlret` if present; returns adjusted via `(1+ret)*(1+delret)-1`; skips with warning if missing).
  - Compustat fundamentals via CCM link: revenue/sales/net income/total assets/common equity/preferred stock/long-term debt/cash flow from ops/capex/R&D (renamed by `config/wrds_field_map.yml`).
  - IPO date enrichment from `comp_global_daily.g_company` when available.
  - Analyst recommendations from I/B/E/S (summary consensus + analyst-level history on 1-5 scale; consensus via `tr_ibes.recdsum`, history via `tr_ibes.recddet`/`det_rec`; mapped to CRSP with CUSIP → `ncusip` → `permno`).
  - Fama-French factors: `ff_all.fivefactors_daily` (MKT, SMB, HML, RMW, CMA, RF) plus MOM from `ff_all.factors_daily` when available.
  - Benchmark: CRSP S&P500 index (`crsp.dsp500`).
  - Macro: FRED API (CPI, unemployment, industrial production).
  - Dividends: `crsp.msedist` with simple `dividend_yield` (divamt/price on pay date).
- YAML config loader and field mapping (`config/wrds_field_map.yml`).

## Repository layout
```
quantlab_data_pipeline/
  config/
    wrds_credentials.yml         # WRDS creds (gitignored; use example)
    fred_credentials.yml         # FRED API key (gitignored; use example)
    wrds_field_map.yml           # Friendly field renames (e.g., fundamentals)
  src/data_pipeline/
    ingestion/wrds_ingestion.py  # Main ingestion entrypoint (WRDS + FRED)
    storage/parquet.py           # LocalParquetDataHandler
    interfaces.py, config.py, __init__.py
  notebooks/
```

**Data root**: outputs are written outside the repo by default under a sibling `../quantlab_data/` folder (or whatever `QUANTLAB_DATA_ROOT` points to). That directory will contain `data_processed/`, `data_meta/`, `data_raw/`, and `reference/`. Keep datasets out of the code repo. Set a custom location via `export QUANTLAB_DATA_ROOT=/path/to/quantlab_data`.

**Data root resolution order**
1) Env var `QUANTLAB_DATA_ROOT`
2) Sibling `../quantlab_data` next to this repo (detected via `pyproject.toml`)
3) Fallback: `./quantlab_data` under the current working directory

**Outputs under the data root**
- `data_processed/`: canonical, cleaned parquet tables (prices, returns, fundamentals, factors, macro, benchmarks, analyst data, dividends)
- `data_meta/`: metadata (`assets_master.parquet`, `universe_sp500.parquet`, `trading_calendar.parquet`, `data_sources.yml`, `field_manifest.*`)
- `data_raw/`: optional raw snapshots when `--save-raw` is set
- `reference/`: copy of `field_manifest.csv` for easy browsing
- `logs/`: per-run ingestion logs (`wrds_ingestion_<timestamp>.log`)

Key datasets written under `data_processed/` (in the shared data root):
- `prices_daily.parquet`, `returns_daily.parquet` (CRSP DSF, with `shrout`; delist-adjusted if delret present)
- `returns_monthly.parquet` (CRSP MSF, delist-adjusted if delret present)
- `fundamentals_quarterly.parquet` (Compustat via CCM link, renamed per `wrds_field_map.yml`)
- `analyst_consensus.parquet` (I/B/E/S recommendations consensus; uses `tr_ibes.recdsum` when available, mapped to CRSP via CUSIP→`ncusip`→`permno`, includes buy/hold/sell %, mean/median/stdev and counts)
- `analyst_ratings_history.parquet` (I/B/E/S analyst-level recommendation history; uses `tr_ibes.recddet` first, then `det_rec` variants if present; mapped to CRSP via CUSIP)
- `macro_timeseries.parquet` (FRED API)
- `style_factor_returns.parquet` (FF 5-factor + MOM when available)
- `benchmarks.parquet` (CRSP S&P500)
- `dividends_monthly.parquet` (CRSP MSEDIST with simple dividend_yield)
- Metadata under `data_meta/`: `assets_master.parquet`, `universe_sp500.parquet`, `trading_calendar.parquet`
- Manifests:
  - [`config/wrds_field_map.yml`](config/wrds_field_map.yml) (wrds field mapping reference)
  - [`reference/field_manifest.csv`](reference/field_manifest.csv) (column-level manifest for each dataset)
- Raw snapshots under `data_raw/` (located in the data root) when `--save-raw`: prices (daily/monthly), delist tables, membership, assets, fundamentals, factors, macro, benchmark, dividends.

## Usage
1) Install dependencies
```bash
conda env create -f environment.yml
conda activate quantlab-data-pipeline
```
   (Or with pip: `pip install -e .[dev]`)

2) Ingest S&P500 data from WRDS (CRSP/Compustat/Fama-French/FRED)
```bash
python -m data_pipeline.ingestion.wrds_ingestion --start 2000-01-01 --end 2025-01-01 --save-raw
# optional: override data root (otherwise uses $QUANTLAB_DATA_ROOT or ../quantlab_data next to the repo)
# python -m data_pipeline.ingestion.wrds_ingestion --root /Users/edl/Documents/dev/quantlab/quantlab_data
```
- Logs: each run writes to `<data_root>/logs/wrds_ingestion_<timestamp>.log` and a run manifest to `<data_root>/data_meta/data_sources.yml` with dataset paths/sources.
  - Defaults: `--start`=`2000-01-01`, `--end`=`2025-01-01`, `--save-raw` off.
  - `data_sources.yml` captures run timestamp, params, and source/location for each dataset (processed + optional raw).
  - `field_manifest.yml/csv` lists columns per dataset and is mirrored to `<data_root>/reference/field_manifest.csv`.

- The ingest script now logs step-by-step progress with timings (e.g., `[3/16] Build assets master ... ✔`), plus a final summary with total runtime.

- WRDS auth options:
  - Preferred: `.pgpass` entry for `wrds-pgdata.wharton.upenn.edu:9737` (see WRDS docs).
  - Alternative: create `config/wrds_credentials.yml` (gitignored) from `config/wrds_credentials.example.yml`.
  - Macro requires FRED API key in `config/fred_credentials.yml` or env `FRED_API_KEY`.
  - Raw snapshots (when `--save-raw`) go to `data_raw/` in the data root: prices_raw, prices_monthly_raw, delist tables, membership_raw, assets_master_raw (with ipo_date), fundamentals_raw, style_factors_raw, macro_raw, benchmark_raw, dividends_monthly_raw.

3) Load data via the handler
```python
from data_pipeline import LocalParquetDataHandler

handler = LocalParquetDataHandler()  # defaults to $QUANTLAB_DATA_ROOT or ../quantlab_data
prices = handler.get_prices(["AAPL", "MSFT"], start_date="2021-01-01", end_date="2021-03-31")
macro = handler.get_macro("2020-01-01", "2020-12-31")
```

Example outputs (after ingest):
```python
handler.get_prices(["AAPL"], "2020-01-02", "2020-01-06").head()
#         date  asset_id ticker   open    high     low   close  adj_close     volume
# 0 2020-01-02     14593   AAPL  ...    ...     ...     ...     ...         ...

handler.get_fundamentals(["AAPL"], "2019-01-01", "2021-12-31").head()
#   report_date  asset_id  revenue  net_income  total_assets  total_debt_long_term  cash_flow_from_operations
#   ...

handler.get_style_factor_returns("2020-01-01", "2020-01-05").head()
#         date factor_name     ret
# 0 2020-01-02        MKT   0.00xx
# 1 2020-01-02        SMB   0.00xx
# ...

handler.get_fundamentals(["AAPL"]).head()
#   report_date  asset_id  revenue  sales  net_income  total_assets  common_equity  preferred_stock  total_debt_long_term  cash_flow_from_operations  capital_expenditures  research_and_development
#   ...

handler.get_macro("2020-01-01", "2020-03-01").head()
#         date series_name   value
# 0 2020-01-01    CPIAUCSL  2.59e+02
# ...

import pandas as pd
from data_pipeline import default_data_root
pd.read_parquet(default_data_root() / "data_processed" / "dividends_monthly.parquet").head()
#         date  asset_id  divamt  dividend_yield  ...
```

## Best practices & ops
- Keep the data root outside code repos (set `QUANTLAB_DATA_ROOT`) and avoid checking parquet/logs into git.
- Treat each ingest log + `data_sources.yml`/`field_manifest.*` as provenance for that run; archive them with the data snapshots if you copy/move data.
- Rotate/prune old files under `<data_root>/logs/` if you run ingests frequently.
- Prefer `LocalParquetDataHandler()` with no args so it follows the shared data root; override `processed_dir`/`meta_dir` only if your layout differs.
- Set WRDS auth once (via `.pgpass` or `config/wrds_credentials.yml`) and FRED API via env `FRED_API_KEY` to avoid embedding secrets in notebooks/scripts.

## Tests
```bash
pytest
```
