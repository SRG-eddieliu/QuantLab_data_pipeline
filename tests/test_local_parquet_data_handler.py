from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.data_pipeline.local_parquet_handler import LocalParquetDataHandler


def _write(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def _build_fixture(tmp_path: Path) -> None:
    processed = tmp_path / "data_processed"
    meta = tmp_path / "data_meta"

    assets_master = pd.DataFrame(
        {
            "asset_id": [1, 2],
            "ticker": ["AAA", "BBB"],
            "sector": ["Tech", "Tech"],
            "industry": ["Software", "Software"],
            "currency": ["USD", "USD"],
            "first_date": pd.to_datetime(["2020-01-01", "2020-01-01"]),
            "last_date": pd.to_datetime(["2020-01-05", "2020-01-05"]),
        }
    )
    _write(assets_master, meta / "assets_master.parquet")

    calendar = pd.DataFrame({"date": pd.date_range("2020-01-01", periods=5, freq="D"), "is_trading_day": True})
    _write(calendar, meta / "trading_calendar.parquet")

    universe = pd.DataFrame(
        {
            "date": pd.date_range("2020-01-01", periods=5, freq="D").repeat(2),
            "asset_id": [1, 2] * 5,
            "in_universe": True,
        }
    )
    _write(universe, meta / "universe_sp500.parquet")

    dates = pd.date_range("2020-01-01", periods=5, freq="D")
    prices = pd.DataFrame(
        {
            "date": list(dates) * 2,
            "asset_id": [1] * 5 + [2] * 5,
            "ticker": ["AAA"] * 5 + ["BBB"] * 5,
            "open": range(10, 20),
            "high": range(11, 21),
            "low": range(9, 19),
            "close": range(10, 20),
            "adj_close": range(10, 20),
            "volume": 1_000_000,
        }
    )
    _write(prices, processed / "prices_daily.parquet")

    returns = prices.copy()
    returns["ret_1d"] = returns.groupby("ticker")["adj_close"].pct_change()
    _write(returns[["date", "asset_id", "ticker", "ret_1d"]].dropna(), processed / "returns_daily.parquet")

    membership = pd.DataFrame(
        {"date": list(dates) * 2, "asset_id": [1, 2] * 5, "in_sp500": True}
    )
    _write(membership, processed / "sp500_membership.parquet")

    fundamentals = pd.DataFrame(
        {
            "report_date": pd.to_datetime(["2020-03-31", "2020-03-31"]),
            "asset_id": [1, 2],
            "book_value": [100, 200],
            "net_income": [10, 20],
            "total_assets": [500, 600],
            "total_debt": [200, 250],
            "cfo": [12, 22],
        }
    )
    _write(fundamentals, processed / "fundamentals_quarterly.parquet")

    macro = pd.DataFrame(
        {"date": dates, "series_name": ["CPI"] * len(dates), "value": range(len(dates))}
    )
    _write(macro, processed / "macro_timeseries.parquet")

    style = pd.DataFrame({"date": dates, "factor_name": ["MKT"] * len(dates), "ret": 0.001})
    _write(style, processed / "style_factor_returns.parquet")

    bench = pd.DataFrame(
        {"date": dates, "benchmark_name": "^GSPC", "level": range(100, 105), "ret": 0.001}
    )
    _write(bench, processed / "benchmarks.parquet")

    risk_free = pd.DataFrame({"date": dates, "rf": 0.0001})
    _write(risk_free, processed / "risk_free.parquet")


def test_get_prices_filters(tmp_path: Path) -> None:
    _build_fixture(tmp_path)
    handler = LocalParquetDataHandler(tmp_path)

    df = handler.get_prices(["AAA"], start_date="2020-01-02", end_date="2020-01-03", fields=["close", "volume"])
    assert not df.empty
    assert set(df["ticker"].unique()) == {"AAA"}
    assert df["date"].min() >= pd.to_datetime("2020-01-02")
    assert set(df.columns) == {"date", "asset_id", "ticker", "close", "volume"}


def test_get_returns(tmp_path: Path) -> None:
    _build_fixture(tmp_path)
    handler = LocalParquetDataHandler(tmp_path)
    df = handler.get_returns(["BBB"], start_date="2020-01-02", end_date="2020-01-05")
    assert set(df["asset_id"].unique()) == {2}
    assert df["date"].max() <= pd.to_datetime("2020-01-05")


def test_get_universe(tmp_path: Path) -> None:
    _build_fixture(tmp_path)
    handler = LocalParquetDataHandler(tmp_path)
    df = handler.get_universe("2020-01-03")
    assert len(df) == 2
    assert df["date"].nunique() == 1


def test_get_fundamentals(tmp_path: Path) -> None:
    _build_fixture(tmp_path)
    handler = LocalParquetDataHandler(tmp_path)
    df = handler.get_fundamentals(["AAA"], start_date="2020-01-01", end_date="2020-12-31")
    assert set(df["asset_id"].unique()) == {1}
    assert {"book_value", "net_income", "total_assets", "total_debt", "cfo"}.issubset(df.columns)


def test_get_macro_and_factors(tmp_path: Path) -> None:
    _build_fixture(tmp_path)
    handler = LocalParquetDataHandler(tmp_path)
    macro = handler.get_macro("2020-01-01", "2020-01-03")
    factors = handler.get_style_factor_returns("2020-01-01", "2020-01-03")
    assert not macro.empty and not factors.empty
    assert macro["series_name"].unique().tolist() == ["CPI"]
    assert factors["factor_name"].unique().tolist() == ["MKT"]


def test_get_benchmark(tmp_path: Path) -> None:
    _build_fixture(tmp_path)
    handler = LocalParquetDataHandler(tmp_path)
    df = handler.get_benchmark_returns("^GSPC", "2020-01-02", "2020-01-04")
    assert not df.empty
    assert df["benchmark_name"].unique().tolist() == ["^GSPC"]
