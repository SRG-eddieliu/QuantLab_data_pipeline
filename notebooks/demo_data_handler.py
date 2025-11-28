"""
Quick demo script to validate the LocalParquetDataHandler.

Run after ingesting data:
    python -m data_pipeline.ingestion.wrds_ingestion
"""

from data_pipeline import LocalParquetDataHandler, default_data_root


def main() -> None:
    handler = LocalParquetDataHandler(default_data_root())

    print("Universe on 2020-01-02:")
    print(handler.get_universe("2020-01-02").head())

    print("\nPrices: AAPL, MSFT from 2020-01-01 to 2020-02-01:")
    print(handler.get_prices(["AAPL", "MSFT"], "2020-01-01", "2020-02-01").head())

    print("\nReturns: AAPL, MSFT from 2020-01-01 to 2020-02-01:")
    print(handler.get_returns(["AAPL", "MSFT"], "2020-01-01", "2020-02-01").head())


if __name__ == "__main__":
    main()
