# QuantLab Data Pipeline

Lean data ingestion and access layer for equity datasets. This repository now focuses solely on building and serving clean, reusable data tables (no research/backtesting code).

## What’s inside
- Unified `DataHandler` interface for downstream consumers.
- Local Parquet storage adapter (`LocalParquetDataHandler`).
- Example ingestion script to bootstrap synthetic or downloaded datasets.
- YAML config loader for pipeline settings.

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
        example_data.py
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

## Usage
1) Install dependencies
```bash
pip install -e .[dev]
```

2) Ingest example data (WRDS → yfinance → synthetic fallback)
```bash
python -m src.data_pipeline.ingestion.example_data --root . --start 2000-01-01 --end 2025-01-01
```

3) Load data via the handler
```python
from src.data_pipeline.storage import LocalParquetDataHandler

handler = LocalParquetDataHandler(data_root=".")
prices = handler.get_prices(["AAPL", "MSFT"], start_date="2021-01-01", end_date="2021-03-31")
macro = handler.get_macro("2020-01-01", "2020-12-31")
```

## Tests
```bash
pytest
```
