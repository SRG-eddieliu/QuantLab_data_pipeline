"""
Quick demo script to validate the LocalParquetDataHandler.

Run after ingesting data:
    python -m src.data_pipeline.ingest_example_data
"""

from pathlib import Path

from src.data_pipeline.local_parquet_handler import LocalParquetDataHandler


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    handler = LocalParquetDataHandler(project_root)

    print("Universe on 2020-01-02:")
    print(handler.get_universe("2020-01-02").head())

    print("\nPrices: AAPL, MSFT from 2020-01-01 to 2020-02-01:")
    print(handler.get_prices(["AAPL", "MSFT"], "2020-01-01", "2020-02-01").head())

    print("\nReturns: AAPL, MSFT from 2020-01-01 to 2020-02-01:")
    print(handler.get_returns(["AAPL", "MSFT"], "2020-01-01", "2020-02-01").head())


if __name__ == "__main__":
    main()
