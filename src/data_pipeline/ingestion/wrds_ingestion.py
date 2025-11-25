from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Tuple
import os

import pandas as pd
import requests
import yaml


DEFAULT_START = "2000-01-01"
DEFAULT_END = "2025-01-01"


def ensure_dirs(paths: Iterable[Path]) -> None:
    for p in paths:
        p.mkdir(parents=True, exist_ok=True)


def _load_wrds_credentials(credentials_path: Path | None = None) -> Tuple[str | None, str | None]:
    """
    Load WRDS credentials from a YAML file if present.

    Expected schema:
    username: YOUR_USERNAME
    password: YOUR_PASSWORD
    """
    if credentials_path is None:
        credentials_path = Path(__file__).resolve().parents[3] / "config" / "wrds_credentials.yml"
    if not credentials_path.exists():
        return None, None
    data = yaml.safe_load(credentials_path.read_text()) or {}
    return data.get("username"), data.get("password")


def _load_field_mapping(section: str) -> dict[str, str]:
    """
    Load WRDS → friendly field mappings from config/wrds_field_map.yml.
    """
    mapping_path = Path(__file__).resolve().parents[3] / "config" / "wrds_field_map.yml"
    if not mapping_path.exists():
        return {}
    data = yaml.safe_load(mapping_path.read_text()) or {}
    return data.get(section, {})


def _load_fred_api_key(credentials_path: Path | None = None) -> str | None:
    """
    Load FRED API key from env or config/fred_credentials.yml.
    """
    env_key = os.environ.get("FRED_API_KEY")
    if env_key:
        return env_key
    if credentials_path is None:
        credentials_path = Path(__file__).resolve().parents[3] / "config" / "fred_credentials.yml"
    if not credentials_path.exists():
        return None
    data = yaml.safe_load(credentials_path.read_text()) or {}
    return data.get("api_key")


def _wrds_connection(credentials_path: Path | None = None):
    try:
        import wrds  # type: ignore
    except ImportError as exc:
        raise RuntimeError("wrds package not installed; install via conda/pip.") from exc
    username, password = _load_wrds_credentials(credentials_path)
    return wrds.Connection(wrds_username=username, wrds_password=password)


def _build_sp500_universe_wrds(db, start: str, end: str) -> pd.DataFrame:
    query = f"""
        select permno,
               start as start_date,
               ending as end_date
        from crsp.dsp500list
        where start <= '{end}' and ending >= '{start}'
    """
    return db.raw_sql(query, date_cols=["start_date", "end_date"])


def _build_assets_master_wrds(db, universe: pd.DataFrame) -> pd.DataFrame:
    permnos = "','".join(str(p) for p in universe["permno"].unique())
    query = f"""
        select distinct permno as asset_id,
                        ticker,
                        namedt as first_date,
                        nameendt as last_date
        from crsp.dsenames
        where permno in ('{permnos}')
    """
    return db.raw_sql(query, date_cols=["first_date", "last_date"])


def _build_trading_calendar(start: str, end: str) -> pd.DataFrame:
    dates = pd.bdate_range(start=start, end=end)
    return pd.DataFrame({"date": dates, "is_trading_day": True})


def _build_universe_daily(universe: pd.DataFrame, calendar: pd.DataFrame) -> pd.DataFrame:
    records = []
    for _, row in universe.iterrows():
        mask = (calendar["date"] >= row["start_date"]) & (calendar["date"] <= row["end_date"])
        dates = calendar.loc[mask, "date"]
        records.append(pd.DataFrame({"date": dates, "asset_id": row["permno"], "in_sp500": True}))
    return pd.concat(records, ignore_index=True)


def _download_prices_wrds_full(db, universe: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    permnos = "','".join(str(p) for p in universe["permno"].unique())
    query = f"""
        select d.date,
               d.permno,
               d.openprc as open,
               d.askhi as high,
               d.bidlo as low,
               d.prc as close,
               d.cfacpr,
               d.ret,
               d.vol as volume
        from crsp.dsf d
        where d.permno in ('{permnos}')
          and d.date between '{start}' and '{end}'
    """
    df = db.raw_sql(query, date_cols=["date"])
    df = df.rename(columns={"permno": "asset_id"})
    df["adj_close"] = df["close"] * df["cfacpr"]
    return df


def _attach_tickers(prices: pd.DataFrame, assets_master: pd.DataFrame) -> pd.DataFrame:
    mapping = dict(zip(assets_master["asset_id"], assets_master["ticker"]))
    prices["ticker"] = prices["asset_id"].map(mapping)
    return prices


def _build_returns_from_crsp(prices: pd.DataFrame) -> pd.DataFrame:
    df = prices[["date", "asset_id", "ticker", "ret"]].copy()
    df["ret_1d"] = df["ret"]
    return df[["date", "asset_id", "ticker", "ret_1d"]]


def _build_fundamentals_wrds(db, universe: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    permnos = "','".join(str(p) for p in universe["permno"].unique())
    link_sql = f"""
        select gvkey, lpermno as permno, linkdt, linkenddt
        from crsp.ccmxpf_linktable
        where lpermno in ('{permnos}')
          and linktype in ('LU','LC')
          and linkprim in ('P','C')
          and (linkdt <= '{end}' or linkdt is null)
    """
    links = db.raw_sql(link_sql, date_cols=["linkdt", "linkenddt"])
    gvkeys = "','".join(links["gvkey"].unique())
    funda_sql = f"""
        select gvkey, datadate,
               revt, ni, at, dltt, oancf
        from comp.funda
        where gvkey in ('{gvkeys}')
          and indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C'
          and datadate between '{start}' and '{end}'
    """
    funda = db.raw_sql(funda_sql, date_cols=["datadate"])
    merged = funda.merge(links, on="gvkey", how="left")
    merged = merged[
        (merged["datadate"] >= merged["linkdt"])
        & ((merged["linkenddt"].isna()) | (merged["datadate"] <= merged["linkenddt"]))
    ]
    merged = merged.rename(columns={"datadate": "report_date", "permno": "asset_id"})
    fundamentals = merged[["report_date", "asset_id", "revt", "ni", "at", "dltt", "oancf"]]
    # Apply friendly naming if available
    mapping = _load_field_mapping("fundamentals")
    fundamentals = fundamentals.rename(columns={k: v for k, v in mapping.items() if k in fundamentals.columns})
    return fundamentals


def _build_ff_factors_and_rf(db, start: str, end: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    # Pull FF 5-factor data (incl. rmw/cma) from ff_all.fivefactors_daily; add umd from ff_all.factors_daily if present.
    base_cols = ["date", "mktrf", "smb", "hml", "rmw", "cma", "rf"]
    sql = f"select {', '.join(base_cols)} from ff_all.fivefactors_daily where date between '{start}' and '{end}'"
    try:
        ff = db.raw_sql(sql, date_cols=["date"])
    except Exception as exc:
        print(
            f"[WARN] ff_all.fivefactors_daily unavailable ({exc}); retrying with core FF factors only.",
            file=sys.stderr,
        )
        core_sql = f"select date, mktrf, smb, hml, rf from ff_all.factors_daily where date between '{start}' and '{end}'"
        ff = db.raw_sql(core_sql, date_cols=["date"])
        for col in ["rmw", "cma"]:
            ff[col] = None
        ff_raw = ff.copy()
    else:
        ff_raw = ff.copy()

    # Add momentum (umd) from ff_all.factors_daily if available
    try:
        mom = db.raw_sql(
            f"select date, umd from ff_all.factors_daily where date between '{start}' and '{end}'",
            date_cols=["date"],
        )
        mom["umd"] = mom["umd"] / 100.0
        ff = ff.merge(mom, on="date", how="left")
        ff_raw = ff_raw.merge(mom, on="date", how="left")
    except Exception as exc:
        print(f"[WARN] ff_all.factors_daily missing umd ({exc}); continuing without MOM.", file=sys.stderr)
        ff["umd"] = None
        ff_raw["umd"] = None

    pct_cols = [c for c in ff.columns if c != "date"]
    ff[pct_cols] = ff[pct_cols] / 100.0
    factor_map = {
        "mktrf": "MKT",
        "smb": "SMB",
        "hml": "HML",
        "rmw": "RMW",
        "cma": "CMA",
        "umd": "MOM",
    }
    factors_long = []
    for raw, name in factor_map.items():
        if raw in ff.columns:
            tmp = ff[["date", raw]].rename(columns={raw: "ret"})
            tmp["factor_name"] = name
            factors_long.append(tmp.dropna())
    factors = pd.concat(factors_long, ignore_index=True) if factors_long else pd.DataFrame(columns=["date", "factor_name", "ret"])
    rf = ff[["date", "rf"]].rename(columns={"rf": "rf"})
    return factors, rf, ff_raw


def _build_macro_wrds(db, start: str, end: str) -> pd.DataFrame:
    """
    Pull macro series from FRED HTTP API (fallback if WRDS FRED mirror absent).
    """
    api_key = _load_fred_api_key()
    series = ["CPIAUCSL", "UNRATE", "INDPRO"]
    frames = []
    for series_id in series:
        params = {
            "series_id": series_id,
            "observation_start": start,
            "observation_end": end,
            "file_type": "json",
        }
        if api_key:
            params["api_key"] = api_key
        url = "https://api.stlouisfed.org/fred/series/observations"
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json().get("observations", [])
        rows = []
        for obs in data:
            val = obs.get("value")
            try:
                val_num = float(val)
            except (TypeError, ValueError):
                continue
            rows.append({"date": obs.get("date"), "series_name": series_id, "value": val_num})
        if rows:
            frames.append(pd.DataFrame(rows))
    if not frames:
        print("[WARN] FRED API returned no data; macro_timeseries will be empty.", file=sys.stderr)
        return pd.DataFrame(columns=["date", "series_name", "value"])
    df = pd.concat(frames, ignore_index=True)
    df["date"] = pd.to_datetime(df["date"])
    return df


def _build_benchmark_wrds(db, start: str, end: str) -> pd.DataFrame:
    # Try common column names; some schemas use caldt instead of date.
    for date_col in ("date", "caldt"):
        try:
            sql = f"""
                select {date_col}, vwretd as ret
                from crsp.dsp500
                where {date_col} between '{start}' and '{end}'
            """
            bench = db.raw_sql(sql, date_cols=[date_col])
            bench = bench.rename(columns={date_col: "date"})
            bench["benchmark_name"] = "^GSPC"
            bench["level"] = (1 + bench["ret"]).cumprod() * 100
            return bench[["date", "benchmark_name", "level", "ret"]]
        except Exception as exc:
            last_exc = exc
            continue
    raise RuntimeError(f"Failed to load benchmark from crsp.dsp500: {last_exc}")


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    print(f"Wrote {len(df):,} rows to {path}")


def _collect_columns(path: Path) -> list[str]:
    try:
        import pyarrow.parquet as pq  # type: ignore

        return pq.read_schema(path).names
    except Exception:
        try:
            return list(pd.read_parquet(path, nrows=0).columns)
        except Exception as exc:  # pragma: no cover - diagnostics only
            return [f"<failed to read cols: {exc}>"]


def ingest(root: Path, start: str, end: str, save_raw: bool = False) -> None:
    processed_path = root / "data_processed"
    meta_path = root / "data_meta"
    raw_path = root / "data_raw"
    ensure_dirs([processed_path, meta_path, raw_path])

    db = _wrds_connection()

    universe = _build_sp500_universe_wrds(db, start, end)
    assets_master = _build_assets_master_wrds(db, universe)
    calendar = _build_trading_calendar(start, end)
    membership = _build_universe_daily(universe, calendar)

    prices = _download_prices_wrds_full(db, universe, start, end)
    prices = _attach_tickers(prices, assets_master)
    returns = _build_returns_from_crsp(prices)

    fundamentals = _build_fundamentals_wrds(db, universe, start, end)
    style_factors, risk_free, ff_raw = _build_ff_factors_and_rf(db, start, end)
    macro = _build_macro_wrds(db, start, end)
    benchmark = _build_benchmark_wrds(db, start, end)

    if save_raw:
        write_parquet(prices, raw_path / "prices_raw.parquet")
        write_parquet(universe, raw_path / "sp500_membership_raw.parquet")
        write_parquet(assets_master, raw_path / "assets_master_raw.parquet")
        write_parquet(fundamentals, raw_path / "fundamentals_raw.parquet")
        write_parquet(ff_raw, raw_path / "style_factors_raw.parquet")
        write_parquet(macro, raw_path / "macro_raw.parquet")
        write_parquet(benchmark, raw_path / "benchmark_raw.parquet")

    write_parquet(prices, processed_path / "prices_daily.parquet")
    write_parquet(returns, processed_path / "returns_daily.parquet")
    write_parquet(membership, processed_path / "sp500_membership.parquet")
    write_parquet(fundamentals, processed_path / "fundamentals_quarterly.parquet")
    write_parquet(macro, processed_path / "macro_timeseries.parquet")
    write_parquet(risk_free, processed_path / "risk_free.parquet")
    write_parquet(style_factors, processed_path / "style_factor_returns.parquet")
    write_parquet(benchmark, processed_path / "benchmarks.parquet")

    write_parquet(assets_master, meta_path / "assets_master.parquet")
    write_parquet(membership.rename(columns={"in_sp500": "in_universe"}), meta_path / "universe_sp500.parquet")
    write_parquet(calendar, meta_path / "trading_calendar.parquet")

    log_path = meta_path / "data_sources.yml"
    log = {
        "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
        "params": {"start": start, "end": end, "source": "wrds", "save_raw": save_raw},
        "datasets": {
            "prices_daily": {"source": "wrds_crsp_dsf", "path": str(processed_path / "prices_daily.parquet")},
            "returns_daily": {"source": "wrds_crsp_dsf_ret", "path": str(processed_path / "returns_daily.parquet")},
            "fundamentals_quarterly": {"source": "wrds_comp_funda", "path": str(processed_path / "fundamentals_quarterly.parquet")},
            "macro_timeseries": {"source": "fred_api", "path": str(processed_path / "macro_timeseries.parquet")},
            "risk_free": {"source": "wrds_ff_factors_daily_rf", "path": str(processed_path / "risk_free.parquet")},
            "style_factor_returns": {"source": "wrds_ff_all_factors_daily", "path": str(processed_path / "style_factor_returns.parquet")},
            "benchmarks": {"source": "wrds_crsp_dsp500", "path": str(processed_path / "benchmarks.parquet")},
            "sp500_membership": {"source": "wrds_crsp_dsp500list", "path": str(processed_path / "sp500_membership.parquet")},
            "assets_master": {"source": "wrds_crsp_dsenames", "path": str(meta_path / "assets_master.parquet")},
            "universe_sp500": {"source": "wrds_crsp_dsp500list", "path": str(meta_path / "universe_sp500.parquet")},
            "trading_calendar": {"source": "business_day_generated", "path": str(meta_path / "trading_calendar.parquet")},
            "raw": {
                "prices_raw": str(raw_path / "prices_raw.parquet") if save_raw else None,
                "sp500_membership_raw": str(raw_path / "sp500_membership_raw.parquet") if save_raw else None,
                "assets_master_raw": str(raw_path / "assets_master_raw.parquet") if save_raw else None,
                "fundamentals_raw": str(raw_path / "fundamentals_raw.parquet") if save_raw else None,
                "style_factors_raw": str(raw_path / "style_factors_raw.parquet") if save_raw else None,
                "macro_raw": str(raw_path / "macro_raw.parquet") if save_raw else None,
                "benchmark_raw": str(raw_path / "benchmark_raw.parquet") if save_raw else None,
            },
        },
    }
    with log_path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(log, f)
    print(f"Wrote ingestion log to {log_path}")

    # Write a field manifest (processed + raw if present)
    manifest = []
    datasets = log.get("datasets", {})
    for name, info in datasets.items():
        if name == "raw":
            for raw_name, raw_path in (info or {}).items():
                if not raw_path:
                    continue
                path = Path(raw_path)
                cols = _collect_columns(path) if path.exists() else []
                manifest.append(
                    {
                        "dataset": raw_name,
                        "type": "raw",
                        "source": "raw_snapshot",
                        "path": str(path),
                        "columns": cols,
                    }
                )
            continue
        path = Path(info.get("path", ""))
        cols = _collect_columns(path) if path.exists() else []
        manifest.append(
            {
                "dataset": name,
                "type": "processed",
                "source": info.get("source"),
                "path": str(path),
                "columns": cols,
            }
        )

    manifest_path_yml = meta_path / "field_manifest.yml"
    manifest_path_csv = meta_path / "field_manifest.csv"
    with manifest_path_yml.open("w", encoding="utf-8") as f:
        yaml.safe_dump(manifest, f)
    pd.DataFrame(manifest).to_csv(manifest_path_csv, index=False)
    print(f"Wrote field manifest to {manifest_path_yml} and {manifest_path_csv}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest S&P500 data from WRDS into local Parquet files.")
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[3], help="Project root.")
    parser.add_argument("--start", type=str, default=DEFAULT_START, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=DEFAULT_END, help="End date (YYYY-MM-DD)")
    parser.add_argument("--save-raw", action="store_true", help="Also write raw WRDS price panel to data_raw/.")
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = parse_args()
    ingest(args.root, args.start, args.end, save_raw=args.save_raw)
