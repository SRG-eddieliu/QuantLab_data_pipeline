from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable, Tuple

import numpy as np
import pandas as pd
import yfinance as yf


TICKERS = ["AAPL", "MSFT", "AMZN", "META", "TSLA"]
DEFAULT_START = "2000-01-01"
DEFAULT_END = "2025-01-01"


def ensure_dirs(paths: Iterable[Path]) -> None:
    for p in paths:
        p.mkdir(parents=True, exist_ok=True)


def _download_prices(tickers: list[str], start: str, end: str, prefer_wrds: bool = True) -> pd.DataFrame:
    """
    Download OHLCV (prefer WRDS CRSP; fallback to yfinance; then synthetic).
    """
    if prefer_wrds:
        try:
            df_wrds = _download_prices_wrds(tickers, start, end)
            if df_wrds is not None and not df_wrds.empty:
                return df_wrds
        except Exception as exc:  # pragma: no cover - WRDS not available in tests
            print(f"[WARN] WRDS download failed ({exc}); falling back to yfinance.", file=sys.stderr)

    try:
        raw = yf.download(
            tickers=" ".join(tickers),
            start=start,
            end=end,
            auto_adjust=False,
            progress=False,
            threads=True,
        )
        if raw.empty:
            raise ValueError("Received empty data from yfinance")
        if isinstance(raw.columns, pd.MultiIndex):
            df = raw.stack(level=1).reset_index()
            df = df.rename(
                columns={
                    "Date": "date",
                    "level_1": "ticker",
                    "Open": "open",
                    "High": "high",
                    "Low": "low",
                    "Close": "close",
                    "Adj Close": "adj_close",
                    "Volume": "volume",
                }
            )
        else:
            # Single ticker case
            df = raw.reset_index()
            df["ticker"] = tickers[0]
            df = df.rename(
                columns={
                    "Date": "date",
                    "Open": "open",
                    "High": "high",
                    "Low": "low",
                    "Close": "close",
                    "Adj Close": "adj_close",
                    "Volume": "volume",
                }
            )
        df["date"] = pd.to_datetime(df["date"])
        df = df.dropna(subset=["adj_close"])
        return df
    except Exception as exc:  # noqa: BLE001 - we want to surface any failure
        print(f"[WARN] yfinance download failed ({exc}); falling back to synthetic prices.", file=sys.stderr)
        return _synthetic_prices(tickers, start, end)


def _download_prices_wrds(tickers: list[str], start: str, end: str) -> pd.DataFrame | None:
    """
    Attempt to pull prices from WRDS CRSP daily stock file.
    Requires: pip install wrds, credentials configured (preferrably ~/.pgpass entry
    for host wrds-pgdata.wharton.upenn.edu:9737; otherwise WRDS_USERNAME/WRDS_PASSWORD env).
    """
    try:
        import wrds  # type: ignore
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError("wrds package not installed; install via `pip install wrds`.") from exc

    db = wrds.Connection()
    tickers_sql = ",".join(f"'{t}'" for t in tickers)
    query = f"""
        select a.date,
               b.ticker,
               a.permno as asset_id,
               a.openprc as open,
               a.askhi as high,
               a.bidlo as low,
               a.prc as close,
               a.prc * a.cfacpr as adj_close,
               a.vol as volume
        from crsp.dsf a
        join crsp.dsenames b
          on a.permno = b.permno
         and b.ticker in ({tickers_sql})
         and b.namedt <= a.date
         and a.date <= b.nameendt
        where a.date between '{start}' and '{end}'
          and b.ticker in ({tickers_sql})
    """
    df = db.raw_sql(query, date_cols=["date"])
    db.close()
    if df.empty:
        raise ValueError("WRDS returned empty price data")
    return df


def _synthetic_prices(tickers: list[str], start: str, end: str) -> pd.DataFrame:
    dates = pd.bdate_range(start=start, end=end)
    rng = np.random.default_rng(seed=42)
    frames = []
    for ticker in tickers:
        rets = rng.normal(loc=0.0005, scale=0.02, size=len(dates))
        adj_close = 100 * (1 + pd.Series(rets, index=dates)).cumprod()
        close = adj_close * (1 + rng.normal(0, 0.0002, size=len(dates)))
        frames.append(
            pd.DataFrame(
                {
                    "date": dates,
                    "ticker": ticker,
                    "open": close * (1 - 0.001),
                    "high": close * (1 + 0.002),
                    "low": close * (1 - 0.002),
                    "close": close,
                    "adj_close": adj_close,
                    "volume": rng.integers(1e6, 5e6, size=len(dates)),
                }
            )
        )
    return pd.concat(frames, ignore_index=True)


def _build_assets_master(prices: pd.DataFrame) -> pd.DataFrame:
    grouped = prices.groupby("ticker")["date"]
    asset_ids = {ticker: idx + 1 for idx, ticker in enumerate(sorted(prices["ticker"].unique()))}
    records = []
    for ticker, dates in grouped:
        records.append(
            {
                "asset_id": asset_ids[ticker],
                "ticker": ticker,
                "sector": "Unknown",
                "industry": "Unknown",
                "currency": "USD",
                "first_date": dates.min(),
                "last_date": dates.max(),
            }
        )
    return pd.DataFrame(records)


def _attach_asset_ids(df: pd.DataFrame, assets_master: pd.DataFrame) -> pd.DataFrame:
    mapping = dict(zip(assets_master["ticker"], assets_master["asset_id"]))
    df["asset_id"] = df["ticker"].map(mapping)
    return df


def _build_returns(prices: pd.DataFrame) -> pd.DataFrame:
    df = prices.sort_values(["ticker", "date"]).copy()
    df["ret_1d"] = df.groupby("ticker")["adj_close"].pct_change()
    return df[["date", "asset_id", "ticker", "ret_1d"]].dropna()


def _build_sp500_membership(calendar: pd.DataFrame, assets_master: pd.DataFrame) -> pd.DataFrame:
    idx = pd.MultiIndex.from_product(
        [calendar["date"], assets_master["asset_id"]],
        names=["date", "asset_id"],
    )
    df = idx.to_frame(index=False)
    df["in_sp500"] = True
    return df


def _build_universe(calendar: pd.DataFrame, assets_master: pd.DataFrame) -> pd.DataFrame:
    df = _build_sp500_membership(calendar, assets_master)
    df = df.rename(columns={"in_sp500": "in_universe"})
    return df


def _build_trading_calendar(start: str, end: str) -> pd.DataFrame:
    dates = pd.bdate_range(start=start, end=end)
    return pd.DataFrame({"date": dates, "is_trading_day": True})


def _build_fundamentals(assets_master: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    dates = pd.date_range(start=start, end=end, freq="Q")
    rng = np.random.default_rng(seed=7)
    rows = []
    for asset_id in assets_master["asset_id"]:
        for d in dates:
            rows.append(
                {
                    "report_date": d,
                    "asset_id": asset_id,
                    "book_value": rng.normal(10_000, 1_000),
                    "net_income": rng.normal(1_000, 200),
                    "total_assets": rng.normal(50_000, 5_000),
                    "total_debt": rng.normal(20_000, 2_000),
                    "cfo": rng.normal(1_200, 150),
                }
            )
    return pd.DataFrame(rows)


def _build_macro_series(start: str, end: str) -> pd.DataFrame:
    dates = pd.bdate_range(start=start, end=end, freq="BM")
    rng = np.random.default_rng(seed=11)
    series_names = ["CPI", "UNRATE", "INDPRO"]
    rows = []
    for name in series_names:
        values = rng.normal(loc=0.0, scale=1.0, size=len(dates)).cumsum()
        rows.append(pd.DataFrame({"date": dates, "series_name": name, "value": values}))
    return pd.concat(rows, ignore_index=True)


def _build_risk_free(calendar: pd.DataFrame, annual_rate: float = 0.02) -> pd.DataFrame:
    daily_rate = annual_rate / 252
    return pd.DataFrame({"date": calendar["date"], "rf": daily_rate})


def _build_style_factor_returns(calendar: pd.DataFrame) -> pd.DataFrame:
    factors = ["MKT", "SMB", "HML", "MOM", "QUAL", "SIZE"]
    rng = np.random.default_rng(seed=21)
    rows = []
    for factor in factors:
        rets = rng.normal(loc=0.0003, scale=0.01, size=len(calendar))
        rows.append(pd.DataFrame({"date": calendar["date"], "factor_name": factor, "ret": rets}))
    return pd.concat(rows, ignore_index=True)


def _build_benchmark(start: str, end: str) -> pd.DataFrame:
    try:
        bench_raw = yf.download("^GSPC", start=start, end=end, auto_adjust=False, progress=False, threads=True)
        if bench_raw.empty:
            raise ValueError("Empty benchmark data")
        bench = bench_raw.reset_index().rename(
            columns={"Date": "date", "Adj Close": "adj_close", "Close": "close", "Open": "open"}
        )
        bench["ret"] = bench["adj_close"].pct_change()
        bench["benchmark_name"] = "^GSPC"
        bench["level"] = bench["adj_close"]
        return bench[["date", "benchmark_name", "level", "ret"]].dropna()
    except Exception as exc:  # noqa: BLE001
        print(f"[WARN] Benchmark download failed ({exc}); falling back to synthetic benchmark.", file=sys.stderr)
        dates = pd.bdate_range(start=start, end=end)
        rng = np.random.default_rng(seed=99)
        rets = rng.normal(loc=0.0004, scale=0.012, size=len(dates))
        levels = 100 * (1 + pd.Series(rets, index=dates)).cumprod()
        return pd.DataFrame(
            {"date": dates, "benchmark_name": "^GSPC", "level": levels.values, "ret": rets},
        )


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    print(f"Wrote {len(df):,} rows to {path}")


def ingest(root: Path, start: str, end: str, source: str = "auto") -> None:
    processed_path = root / "data_processed"
    meta_path = root / "data_meta"
    ensure_dirs([processed_path, meta_path])

    prefer_wrds = source in ("auto", "wrds")
    prices = _download_prices(TICKERS, start, end, prefer_wrds=prefer_wrds)
    assets_master = _build_assets_master(prices)
    prices = _attach_asset_ids(prices, assets_master)
    trading_calendar = _build_trading_calendar(start, end)

    returns = _build_returns(prices)
    membership = _build_sp500_membership(trading_calendar, assets_master)
    universe = _build_universe(trading_calendar, assets_master)
    fundamentals = _build_fundamentals(assets_master, start, end)
    macro = _build_macro_series(start, end)
    risk_free = _build_risk_free(trading_calendar)
    style_factors = _build_style_factor_returns(trading_calendar)
    benchmark = _build_benchmark(start, end)

    write_parquet(prices, processed_path / "prices_daily.parquet")
    write_parquet(returns, processed_path / "returns_daily.parquet")
    write_parquet(membership, processed_path / "sp500_membership.parquet")
    write_parquet(fundamentals, processed_path / "fundamentals_quarterly.parquet")
    write_parquet(macro, processed_path / "macro_timeseries.parquet")
    write_parquet(risk_free, processed_path / "risk_free.parquet")
    write_parquet(style_factors, processed_path / "style_factor_returns.parquet")
    write_parquet(benchmark, processed_path / "benchmarks.parquet")

    write_parquet(assets_master, meta_path / "assets_master.parquet")
    write_parquet(universe, meta_path / "universe_sp500.parquet")
    write_parquet(trading_calendar, meta_path / "trading_calendar.parquet")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest example data into local Parquet files.")
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parents[2],
        help="Project root containing data_processed/ and data_meta/ directories.",
    )
    parser.add_argument("--start", type=str, default=DEFAULT_START, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=DEFAULT_END, help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "--source",
        type=str,
        choices=["auto", "wrds", "yfinance"],
        default="auto",
        help="Preferred data source: wrds (CRSP) or yfinance; auto tries WRDS then falls back.",
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = parse_args()
    ingest(args.root, args.start, args.end, args.source)
