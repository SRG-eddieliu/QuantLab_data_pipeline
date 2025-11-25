# QuantLab Data Pipeline

Lean data ingestion and access layer for equity datasets (WRDS + FRED). Builds S&P 500 datasets locally (including delisted names) and serves them via a simple handler.

## What’s inside
- Unified `DataHandler` interface for downstream consumers.
- Local Parquet storage adapter (`LocalParquetDataHandler`).
- WRDS-backed ingestion (`wrds_ingestion`) for S&P 500 data (CRSP prices/returns/membership, Compustat fundamentals, FF 5-factor + optional MOM, CRSP S&P500 benchmark).
- FRED API macro ingestion (CPI, UNRATE, INDPRO).
- YAML config loader and field mapping (`config/wrds_field_map.yml`).

## Repository layout
```
QuantLab_data_pipeline/
  config/                # YAML configs (optional)
  data_raw/              # Vendor/API dumps (not tracked)
  data_processed/        # Normalized Parquet tables
  data_meta/             # Metadata (assets, calendars, universe)
  src/
    data_pipeline/
      __init__.py
      config.py
      interfaces.py
      ingestion/
        wrds_ingestion.py
      storage/
        parquet.py
  notebooks/
  tests/
```

Key datasets written under `data_processed/`:
- `prices_daily.parquet`, `returns_daily.parquet`
- `fundamentals_quarterly.parquet`
- `macro_timeseries.parquet`
- `style_factor_returns.parquet`
- `benchmarks.parquet`
- Metadata under `data_meta/`: `assets_master.parquet`, `universe_sp500.parquet`, `trading_calendar.parquet`
- Field manifest under `data_meta/`: `field_manifest.yml` / `field_manifest.csv` (fields, sources, paths).
- Optional raw snapshots under `data_raw/` when `--save-raw` is passed.

## Usage
1) Install dependencies
```bash
conda env create -f environment.yml
conda activate quantlab-data-pipeline
```
   (Or with pip: `pip install -e .[dev]`)

2) Ingest S&P500 data from WRDS (CRSP/Compustat/Fama-French/FRED)
```bash
python -m src.data_pipeline.ingestion.wrds_ingestion --root . --start 2000-01-01 --end 2025-01-01 --save-raw
```

- WRDS auth options:
  - Preferred: `.pgpass` entry for `wrds-pgdata.wharton.upenn.edu:9737` (see WRDS docs).
  - Alternative: create `config/wrds_credentials.yml` (gitignored) from `config/wrds_credentials.example.yml`.
  - Macro requires FRED API key in `config/fred_credentials.yml` or env `FRED_API_KEY`.

3) Load data via the handler
```python
from src.data_pipeline.storage import LocalParquetDataHandler

handler = LocalParquetDataHandler(data_root=".")
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
```

## Tests
```bash
pytest
```
