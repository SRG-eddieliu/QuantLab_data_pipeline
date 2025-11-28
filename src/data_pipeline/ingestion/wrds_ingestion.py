from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Tuple
import os
import time

import pandas as pd
import requests
import yaml

from ..config import default_data_root


DEFAULT_START = "2000-01-01"
DEFAULT_END = "2025-01-01"
logger = logging.getLogger(__name__)


def _sql_list(values: Iterable[object]) -> str:
    """
    Safely format a list of values for SQL IN clauses by escaping single quotes.
    """
    return "','".join(str(v).replace("'", "''") for v in values)


def ensure_dirs(paths: Iterable[Path]) -> None:
    for p in paths:
        p.mkdir(parents=True, exist_ok=True)


def _configure_logging(root: Path) -> Path:
    """
    Configure console + file logging under the shared data root.
    """
    logger.handlers.clear()
    logger.setLevel(logging.INFO)
    log_dir = root / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    log_path = log_dir / f"wrds_ingestion_{ts}.log"
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    logger.propagate = False
    return log_path


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
    permnos = _sql_list(universe["permno"].unique())
    query = f"""
        select distinct permno as asset_id,
                        ticker,
                        namedt as first_date,
                        nameendt as last_date
        from crsp.dsenames
        where permno in ('{permnos}')
    """
    return db.raw_sql(query, date_cols=["first_date", "last_date"])


def _load_ipo_dates(db, universe: pd.DataFrame) -> pd.DataFrame:
    """
    Pull IPO dates from Compustat global company file and map to permno via CCM link.
    """
    permnos = _sql_list(universe["permno"].unique())
    query = f"""
        select distinct l.lpermno as asset_id,
                        g.ipodate
        from crsp.ccmxpf_linktable l
        join comp_global_daily.g_company g
          on l.gvkey = g.gvkey
        where l.lpermno in ('{permnos}')
          and l.linktype in ('LU','LC')
          and l.linkprim in ('P','C')
    """
    try:
        df = db.raw_sql(query, date_cols=["ipodate"])
        return df
    except Exception as exc:  # pragma: no cover - optional table
        logger.warning("comp_global_daily.g_company IPO dates unavailable (%s); skipping IPO enrichment.", exc)
        return pd.DataFrame(columns=["asset_id", "ipodate"])


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
    permnos = _sql_list(universe["permno"].unique())
    query = f"""
        select d.date,
               d.permno,
               d.openprc as open,
               d.askhi as high,
               d.bidlo as low,
               d.prc as close,
               d.cfacpr,
               d.ret,
               d.shrout,
               d.vol as volume
        from crsp.dsf d
        where d.permno in ('{permnos}')
          and d.date between '{start}' and '{end}'
    """
    df = db.raw_sql(query, date_cols=["date"])
    df = df.rename(columns={"permno": "asset_id"})
    df["adj_close"] = df["close"] * df["cfacpr"]
    return df


def _download_monthly_wrds(db, universe: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    permnos = _sql_list(universe["permno"].unique())
    query = f"""
        select m.date,
               m.permno,
               m.prc as close,
               m.ret,
               m.vol as volume,
               m.shrout
        from crsp.msf m
        where m.permno in ('{permnos}')
          and m.date between '{start}' and '{end}'
    """
    df = db.raw_sql(query, date_cols=["date"])
    df = df.rename(columns={"permno": "asset_id"})
    return df


def _load_dividends_monthly(db, universe: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    permnos = _sql_list(universe["permno"].unique())
    query = f"""
        select permno as asset_id,
               distcd,
               divamt,
               facpr,
               facshr,
               paydt as date
        from crsp.msedist
        where permno in ('{permnos}') and paydt between '{start}' and '{end}'
    """
    try:
        df = db.raw_sql(query, date_cols=["date"])
        return df
    except Exception as exc:
        logger.warning("crsp.msedist not available (%s); skipping listed dividends.", exc)
        return pd.DataFrame(columns=["asset_id", "distcd", "divamt", "facpr", "facshr", "date"])


def _first_non_null(series: pd.Series):
    non_null = series.dropna()
    return non_null.iloc[0] if not non_null.empty else None


def _dedupe_assets_master(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    grouped = []
    for asset_id, g in df.groupby("asset_id"):
        g_sorted = g.sort_values("last_date")
        ld_series = g["last_date"].dropna()
        grouped.append(
            {
                "asset_id": asset_id,
                "ticker": _first_non_null(g_sorted["ticker"][::-1]),
                "first_date": g["first_date"].min(),
                "last_date": ld_series.max() if not ld_series.empty else None,
                "ipodate": g["ipodate"].dropna().min() if "ipodate" in g.columns else None,
            }
        )
    grouped = pd.DataFrame(grouped)
    return grouped


def _dedupe_consensus(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    agg_map = {
        "ticker": _first_non_null,
        "mean_rating": _first_non_null,
        "median_rating": _first_non_null,
        "stdev_rating": _first_non_null,
        "num_analysts": _first_non_null,
        "buy_percent": _first_non_null,
        "hold_percent": _first_non_null,
        "sell_percent": _first_non_null,
        "num_up": _first_non_null,
        "num_down": _first_non_null,
        "usfirm": _first_non_null,
        "ibes_official_ticker": _first_non_null,
        "ibes_cusip": _first_non_null,
        "company_name": _first_non_null,
    }
    deduped = df.groupby(["date", "asset_id"], as_index=False).agg(agg_map)
    return deduped


def _dedupe_ratings_history(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    agg_map = {
        "ticker": _first_non_null,
        "rating": _first_non_null,
        "action_code": _first_non_null,
        "rating_text": _first_non_null,
        "statistic_date": _first_non_null,
    }
    deduped = df.groupby(["date", "asset_id", "analyst_id"], as_index=False).agg(agg_map)
    return deduped


def _clean_dividends(dividends: pd.DataFrame, prices_daily: pd.DataFrame) -> pd.DataFrame:
    if dividends.empty:
        return dividends
    merged = dividends.copy()
    prices_cols = ["asset_id", "date", "close"]
    if prices_daily is not None and set(prices_cols).issubset(prices_daily.columns):
        price_lookup = prices_daily[prices_cols].rename(columns={"close": "close_daily"})
        merged = merged.merge(price_lookup, on=["asset_id", "date"], how="left")

    # Normalize close column names and prefer daily over monthly
    if "close_x" in merged.columns or "close_y" in merged.columns:
        merged["close"] = merged.get("close_y").combine_first(merged.get("close_x"))
        merged = merged.drop(columns=[c for c in ["close_x", "close_y"] if c in merged.columns])
    if "close_daily" in merged.columns:
        merged["close"] = merged.get("close_daily").combine_first(merged.get("close"))
        merged = merged.drop(columns=["close_daily"])
    if "close" not in merged.columns:
        merged["close"] = None

    def _agg(group: pd.DataFrame) -> pd.Series:
        base = group.iloc[0].copy()
        base["divamt"] = group["divamt"].sum(skipna=True)
        base["distcd"] = _first_non_null(group["distcd"])
        base["facpr"] = _first_non_null(group["facpr"])
        base["facshr"] = _first_non_null(group["facshr"])
        base["close"] = _first_non_null(group["close"])
        base["dividend_yield"] = base["divamt"] / base["close"] if pd.notna(base.get("close")) else None
        return base

    cleaned = merged.groupby(["asset_id", "date"], as_index=False).apply(_agg).reset_index(drop=True)
    return cleaned


def _load_dlret_daily(db, universe: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    cols = [
        "asset_id",
        "date",
        "deldtprc",
        "deldtprcflg",
        "delactiontype",
        "delstatustype",
        "delreasontype",
        "delpaymenttype",
        "delpermno",
        "delpermco",
        "delret",
        "delretmisstype",
        "delnextdt",
        "delnextprc",
        "delnextprcflg",
        "delamtdt",
        "deldivamt",
        "deldistype",
        "deldlydt",
    ]
    permnos = _sql_list(universe["permno"].unique())
    query = f"""
        select permno as asset_id,
               delistingdt as date,
               deldtprc,
               deldtprcflg,
               delactiontype,
               delstatustype,
               delreasontype,
               delpaymenttype,
               delpermno,
               delpermco,
               delret,
               delretmisstype,
               delnextdt,
               delnextprc,
               delnextprcflg,
               delamtdt,
               deldivamt,
               deldistype,
               deldlydt
        from crsp.StkDelists
        where permno in ('{permnos}') and delistingdt between '{start}' and '{end}'
    """
    try:
        df = db.raw_sql(query, date_cols=["date"])
        for col in cols:
            if col not in df.columns:
                df[col] = None
        return df[cols]
    except Exception as exc:
        logger.warning("crsp.StkDelists not available (%s); skipping daily delist returns.", exc)
        return pd.DataFrame(columns=cols)


def _load_dlret_monthly(db, universe: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    cols = [
        "asset_id",
        "date",
        "deldtprc",
        "deldtprcflg",
        "delactiontype",
        "delstatustype",
        "delreasontype",
        "delpaymenttype",
        "delpermno",
        "delpermco",
        "delret",
        "delretmisstype",
        "delnextdt",
        "delnextprc",
        "delnextprcflg",
        "delamtdt",
        "deldivamt",
        "deldistype",
        "deldlydt",
    ]
    permnos = _sql_list(universe["permno"].unique())
    query = f"""
        select permno as asset_id,
               delistingdt as date,
               deldtprc,
               deldtprcflg,
               delactiontype,
               delstatustype,
               delreasontype,
               delpaymenttype,
               delpermno,
               delpermco,
               delret,
               delretmisstype,
               delnextdt,
               delnextprc,
               delnextprcflg,
               delamtdt,
               deldivamt,
               deldistype,
               deldlydt
        from crsp.StkDelists
        where permno in ('{permnos}') and delistingdt between '{start}' and '{end}'
    """
    try:
        df = db.raw_sql(query, date_cols=["date"])
        for col in cols:
            if col not in df.columns:
                df[col] = None
        return df[cols]
    except Exception as exc:
        logger.warning("crsp.StkDelists not available (%s); skipping monthly delist returns.", exc)
        return pd.DataFrame(columns=cols)


def _attach_tickers(prices: pd.DataFrame, assets_master: pd.DataFrame) -> pd.DataFrame:
    mapping = dict(zip(assets_master["asset_id"], assets_master["ticker"]))
    prices["ticker"] = prices["asset_id"].map(mapping)
    return prices


def _build_returns_from_crsp(prices: pd.DataFrame) -> pd.DataFrame:
    df = prices[["date", "asset_id", "ticker", "ret"]].copy()
    df["ret_1d"] = df["ret"]
    return df[["date", "asset_id", "ticker", "ret_1d"]]


def _apply_delist_returns(returns: pd.DataFrame, dlret: pd.DataFrame) -> pd.DataFrame:
    if dlret.empty:
        return returns
    if "dlret" not in dlret.columns and "delret" in dlret.columns:
        dlret = dlret.rename(columns={"delret": "dlret"})
    if "dlret" not in dlret.columns:
        dlret["dlret"] = 0.0
    merged = returns.merge(dlret[["asset_id", "date", "dlret"]], on=["asset_id", "date"], how="left")
    merged["dlret"] = merged["dlret"].fillna(0.0)
    merged["ret_1d"] = (1 + merged["ret_1d"]) * (1 + merged["dlret"]) - 1
    return merged.drop(columns=["dlret"])


def _build_monthly_returns_from_crsp(prices_m: pd.DataFrame, dlret_m: pd.DataFrame) -> pd.DataFrame:
    df = prices_m.rename(columns={"close": "price"})[["date", "asset_id", "ret", "price", "volume", "shrout"]].copy()
    df["ret_1m"] = df["ret"]
    if not dlret_m.empty:
        # Some WRDS schemas may name the field delret instead of dlret
        if "dlret" not in dlret_m.columns and "delret" in dlret_m.columns:
            dlret_m = dlret_m.rename(columns={"delret": "dlret"})
        if "dlret" not in dlret_m.columns:
            dlret_m["dlret"] = 0.0
        df = df.merge(dlret_m[["asset_id", "date", "dlret"]], on=["asset_id", "date"], how="left")
        df["dlret"] = df["dlret"].fillna(0.0)
        df["ret_1m"] = (1 + df["ret_1m"]) * (1 + df["dlret"]) - 1
        df = df.drop(columns=["dlret"])
    return df


def _build_fundamentals_wrds(db, universe: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    permnos = _sql_list(universe["permno"].unique())
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
    full_sql = f"""
        select gvkey, datadate,
               revt, sale, ni, at, ceq, dltt, pstk, oancf, capx, xrd
        from comp.funda
        where gvkey in ('{gvkeys}')
          and indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C'
          and datadate between '{start}' and '{end}'
    """
    try:
        funda = db.raw_sql(full_sql, date_cols=["datadate"])
    except Exception as exc:
        logger.warning("comp.funda missing some requested fields (%s); falling back to core set.", exc)
        core_sql = f"""
            select gvkey, datadate,
                   revt, ni, at, dltt, oancf
            from comp.funda
            where gvkey in ('{gvkeys}')
              and indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C'
              and datadate between '{start}' and '{end}'
        """
        funda = db.raw_sql(core_sql, date_cols=["datadate"])
    merged = funda.merge(links, on="gvkey", how="left")
    merged = merged[
        (merged["datadate"] >= merged["linkdt"])
        & ((merged["linkenddt"].isna()) | (merged["datadate"] <= merged["linkenddt"]))
    ]
    merged = merged.rename(columns={"datadate": "report_date", "permno": "asset_id"})
    cols = [
        "report_date",
        "asset_id",
        "revt",
        "sale",
        "ni",
        "at",
        "ceq",
        "dltt",
        "pstk",
        "oancf",
        "capx",
        "xrd",
    ]
    missing_cols = [c for c in cols if c not in merged.columns]
    if missing_cols:
        for col in missing_cols:
            merged[col] = None
    fundamentals = merged[[c for c in cols if c in merged.columns]]
    # Apply friendly naming if available
    mapping = _load_field_mapping("fundamentals")
    fundamentals = fundamentals.rename(columns={k: v for k, v in mapping.items() if k in fundamentals.columns})
    return fundamentals


def _load_idxref_mapping(db, universe: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    """
    Map IBES to CRSP via CUSIP overlap only (tr_ibes.id cusip -> CRSP dsenames ncusip -> permno).

    Sources: tr_ibes.id (sdates only; end date assumed open). iclink/idxref are not used per request.
    """
    permnos = _sql_list(universe["permno"].unique())
    base_columns = ["asset_id", "ticker", "ibtic", "cname", "start_date", "end_date"]

    def _normalize_cusip(series: pd.Series) -> pd.Series:
        return series.astype(str).str.replace(r"[^A-Za-z0-9]", "", regex=True).str[:8]

    # Primary: CUSIP mapping
    def _load_idsum(query: str) -> pd.DataFrame:
        try:
            return db.raw_sql(query, date_cols=["start_date", "end_date"])
        except Exception as exc:
            logger.warning("tr_ibes.id query failed (%s);", exc)
            return pd.DataFrame()

    idsum_queries = [
        # tr_ibes.id with only sdates; assume open-ended coverage
        f"""
        select ticker, cusip, cname, sdates as start_date, null as end_date
        from tr_ibes.id
        where sdates <= '{end}'
        """,
    ]
    ibes_ids = pd.DataFrame()
    for q in idsum_queries:
        ibes_ids = _load_idsum(q)
        if not ibes_ids.empty:
            break
    if ibes_ids.empty:
        logger.warning("tr_ibes.id unavailable for CUSIP mapping after retries; analyst datasets will be empty.")
        return pd.DataFrame(columns=base_columns)

    ibes_ids["end_date"] = ibes_ids["end_date"].fillna(pd.Timestamp.max)
    ibes_ids["cusip8"] = _normalize_cusip(ibes_ids["cusip"])
    try:
        crsp_names = db.raw_sql(
            f"""
            select permno as asset_id, ncusip, namedt as start_date, nameendt as end_date
            from crsp.dsenames
            where permno in ('{permnos}')
              and ncusip is not null
              and namedt <= '{end}'
              and (nameendt is null or nameendt >= '{start}')
            """,
            date_cols=["start_date", "end_date"],
        )
    except Exception as exc:
        logger.warning("crsp.dsenames unavailable for CUSIP mapping (%s); analyst datasets will be empty.", exc)
        return pd.DataFrame(columns=base_columns)
    if crsp_names.empty:
        logger.warning("crsp.dsenames returned no rows for CUSIP mapping; analyst datasets will be empty.")
        return pd.DataFrame(columns=base_columns)

    crsp_names["end_date"] = crsp_names["end_date"].fillna(pd.Timestamp.max)
    crsp_names["cusip8"] = _normalize_cusip(crsp_names["ncusip"])

    merged_cusip = ibes_ids.merge(crsp_names, on="cusip8", how="inner", suffixes=("_ibes", "_crsp"))
    merged_cusip["start_date_final"] = merged_cusip[["start_date_ibes", "start_date_crsp"]].max(axis=1)
    merged_cusip["end_date_final"] = merged_cusip[["end_date_ibes", "end_date_crsp"]].min(axis=1)
    merged_cusip = merged_cusip[
        (merged_cusip["start_date_final"] <= pd.to_datetime(end))
        & (merged_cusip["end_date_final"] >= pd.to_datetime(start))
    ]
    if merged_cusip.empty:
        logger.warning("CUSIP mapping produced no matches after date filter; analyst datasets will be empty.")
        return pd.DataFrame(columns=base_columns)

    mapped = pd.DataFrame(
        {
            "asset_id": merged_cusip["asset_id"],
            "ticker": merged_cusip["ticker"],
            "ibtic": None,
            "cname": merged_cusip["cname"],
            "start_date": merged_cusip["start_date_final"],
            "end_date": merged_cusip["end_date_final"],
        }
    ).drop_duplicates(subset=["asset_id", "ticker", "start_date", "end_date"])
    return mapped


def _build_analyst_consensus_wrds(db, idxref: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    """
    Pull I/B/E/S consensus recommendations (summary) and map to permno.

    Industry convention: ratings are on a 1-5 scale (1=Strong Buy, 5=Sell).
    """
    if idxref.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "asset_id",
                "ticker",
                "mean_rating",
                "median_rating",
                "stdev_rating",
                "num_analysts",
                "rating_high",
                "rating_low",
                "num_buy",
                "num_hold",
                "num_sell",
            ]
        )
    tickers = _sql_list(idxref["ticker"].unique())
    # Use tr_ibes.recdsum only; rec_summary tables are not available in this environment.
    candidates = [("tr_ibes", "recdsum", "statpers")]
    try:
        available = db.list_tables(candidates[0][0])
    except Exception:
        available = []
    schema, table, date_col = candidates[0]
    if table not in available:
        logger.warning("%s.%s not available; consensus dataset will be empty.", schema, table)
        return pd.DataFrame(
            columns=[
                "date",
                "asset_id",
                "ticker",
                "mean_rating",
                "median_rating",
                "stdev_rating",
                "num_analysts",
                "buy_percent",
                "hold_percent",
                "sell_percent",
                "num_up",
                "num_down",
                "usfirm",
                "ibes_official_ticker",
                "ibes_cusip",
                "company_name",
            ]
        )
    sql = f"""
        select statpers,
               ticker,
               oftic,
               cusip,
               cname,
               buypct,
               holdpct,
               sellpct,
               meanrec,
               medrec,
               stdev,
               numup,
               numdown,
               numrec,
               usfirm
        from {schema}.{table}
        where ticker in ('{tickers}')
          and {date_col} between '{start}' and '{end}'
    """
    try:
        recs = db.raw_sql(sql, date_cols=[date_col])
    except Exception as exc:
        logger.warning("%s.%s query failed (%s); consensus dataset will be empty.", schema, table, exc)
        return pd.DataFrame(
            columns=[
                "date",
                "asset_id",
                "ticker",
                "mean_rating",
                "median_rating",
                "stdev_rating",
                "num_analysts",
                "buy_percent",
                "hold_percent",
                "sell_percent",
                "num_up",
                "num_down",
                "usfirm",
                "ibes_official_ticker",
                "ibes_cusip",
                "company_name",
            ]
        )
    recs[date_col] = pd.to_datetime(recs[date_col])
    recs = recs.merge(idxref, on="ticker", how="left")
    date_field = "statpers" if "statpers" in recs.columns else "date"
    if date_field in recs.columns:
        recs = recs[(recs[date_field] >= recs["start_date"]) & (recs[date_field] <= recs["end_date"])]
    rename_map = {
        "statpers": "date",
        "meanrec": "mean_rating",
        "medrec": "median_rating",
        "stdev": "stdev_rating",
        "numrec": "num_analysts",
        "buypct": "buy_percent",
        "holdpct": "hold_percent",
        "sellpct": "sell_percent",
        "numup": "num_up",
        "numdown": "num_down",
        "oftic": "ibes_official_ticker",
        "cusip": "ibes_cusip",
        "cname": "company_name",
    }
    recs = recs.rename(columns={k: v for k, v in rename_map.items() if k in recs.columns})
    # Ensure all expected columns exist for downstream consumers
    expected_cols = [
        "date",
        "asset_id",
        "ticker",
        "mean_rating",
        "median_rating",
        "stdev_rating",
        "num_analysts",
        "buy_percent",
        "hold_percent",
        "sell_percent",
        "num_up",
        "num_down",
        "usfirm",
        "ibes_official_ticker",
        "ibes_cusip",
        "company_name",
    ]
    for col in expected_cols:
        if col not in recs.columns:
            recs[col] = None
    ordered_cols = [
        "date",
        "asset_id",
        "ticker",
        "mean_rating",
        "median_rating",
        "stdev_rating",
        "num_analysts",
        "buy_percent",
        "hold_percent",
        "sell_percent",
        "num_up",
        "num_down",
        "usfirm",
        "ibes_official_ticker",
        "ibes_cusip",
        "company_name",
    ]
    recs = recs[ordered_cols]
    recs = recs.dropna(subset=["date", "asset_id"])
    return recs


def _build_analyst_ratings_history_wrds(db, idxref: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    """
    Pull analyst-level recommendation history from I/B/E/S detail (point-in-time).
    """
    base_cols = [
        "date",
        "asset_id",
        "ticker",
        "analyst_id",
        "rating",
        "action_code",
        "rating_text",
        "statistic_date",
    ]
    if idxref.empty:
        return pd.DataFrame(columns=base_cols)
    tickers = _sql_list(idxref["ticker"].unique())
    candidates = [
        ("tr_ibes", "recddet"),  # Thomson/Refinitiv IBES detail
        ("tr_ibes", "det_rec"),
        ("ibes", "det_rec"),
        ("ibesus", "det_rec"),
    ]
    detail = pd.DataFrame()
    attempted = []
    for schema, table in candidates:
        try:
            available = db.list_tables(schema)
        except Exception:
            available = []
        if table not in available:
            attempted.append(f"{schema}.{table} (absent)")
            continue
        date_field = "statpers" if table == "det_rec" else "anndats"
        sql = f"""
            select *
            from {schema}.{table}
            where ticker in ('{tickers}')
              and {date_field} between '{start}' and '{end}'
        """
        try:
            detail = db.raw_sql(sql)
            attempted.append(f"{schema}.{table} (ok)")
            break
        except Exception as exc:
            attempted.append(f"{schema}.{table} (error: {exc})")
            detail = pd.DataFrame()
            continue
    if detail.empty:
        logger.warning("No det_rec table available; attempted: %s. Analyst rating history will be empty.", attempted)
        return pd.DataFrame(columns=base_cols)
    # Convert date-like columns conservatively
    for col in ["statpers", "anndats", "anndats_act", "actdats", "revdats"]:
        if col in detail.columns:
            detail[col] = pd.to_datetime(detail[col])
    detail = detail.merge(idxref, on="ticker", how="left")
    # Date range alignment uses statpers or anndats depending on table
    if "statpers" in detail.columns:
        detail = detail[(detail["statpers"] >= detail["start_date"]) & (detail["statpers"] <= detail["end_date"])]
    elif "anndats" in detail.columns:
        detail = detail[(detail["anndats"] >= detail["start_date"]) & (detail["anndats"] <= detail["end_date"])]
    # Select and normalize fields
    date_col = None
    for candidate in ("anndats_act", "anndats", "statpers", "actdats", "revdats"):
        if candidate in detail.columns:
            date_col = candidate
            break
    if date_col:
        detail["date"] = detail[date_col]
    detail["analyst_id"] = (
        detail["analys"]
        if "analys" in detail.columns
        else detail["amaskcd"]
        if "amaskcd" in detail.columns
        else None
    )
    if "ireccd" in detail.columns:
        detail["rating"] = detail["ireccd"]
    elif "rec" in detail.columns:
        detail["rating"] = detail["rec"]
    else:
        detail["rating"] = None
    detail["action_code"] = (
        detail["ereccd"] if "ereccd" in detail.columns else detail["actioncode"] if "actioncode" in detail.columns else None
    )
    detail["rating_text"] = (
        detail["itext"] if "itext" in detail.columns else detail["recdef"] if "recdef" in detail.columns else None
    )
    detail["statistic_date"] = (
        detail["statpers"]
        if "statpers" in detail.columns
        else detail["anndats"]
        if "anndats" in detail.columns
        else None
    )
    missing_cols = [c for c in base_cols if c not in detail.columns]
    for col in missing_cols:
        detail[col] = None
    detail = detail[base_cols].dropna(subset=["date", "asset_id"])
    return detail


def _build_ff_factors_and_rf(db, start: str, end: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    # Pull FF 5-factor data (incl. rmw/cma) from ff_all.fivefactors_daily; add umd from ff_all.factors_daily if present.
    base_cols = ["date", "mktrf", "smb", "hml", "rmw", "cma", "rf"]
    sql = f"select {', '.join(base_cols)} from ff_all.fivefactors_daily where date between '{start}' and '{end}'"
    try:
        ff = db.raw_sql(sql, date_cols=["date"])
    except Exception as exc:
        logger.warning("ff_all.fivefactors_daily unavailable (%s); retrying with core FF factors only.", exc)
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
        logger.warning("ff_all.factors_daily missing umd (%s); continuing without MOM.", exc)
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
        logger.warning("FRED API returned no data; macro_timeseries will be empty.")
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
    logger.info("Wrote %s rows to %s", len(df), path)


def _collect_columns(path: Path) -> list[str]:
    try:
        import pyarrow.parquet as pq  # type: ignore

        return pq.read_schema(path).names
    except Exception:
        try:
            return list(pd.read_parquet(path, nrows=0).columns)
        except Exception as exc:  # pragma: no cover - diagnostics only
            return [f"<failed to read cols: {exc}>"]


def ingest(root: Path | None, start: str, end: str, save_raw: bool = False) -> None:
    total_steps = 17
    steps_done: list[tuple[str, float]] = []

    def start_step(name: str) -> tuple[str, float]:
        logger.info("[%s/%s] %s ...", len(steps_done) + 1, total_steps, name)
        return name, time.time()

    def end_step(token: tuple[str, float]) -> None:
        name, t0 = token
        elapsed = time.time() - t0
        steps_done.append((name, elapsed))
        logger.info("  ✔ %s (%.1fs)", name, elapsed)

    resolved_root = Path(root).expanduser().resolve() if root is not None else default_data_root()
    log_path = _configure_logging(resolved_root)
    logger.info("Logging to %s", log_path)
    processed_path = resolved_root / "data_processed"
    meta_path = resolved_root / "data_meta"
    raw_path = resolved_root / "data_raw"
    reference_path = resolved_root / "reference"
    ensure_dirs([processed_path, meta_path, raw_path, reference_path])

    step = start_step("Connect to WRDS")
    db = _wrds_connection()
    end_step(step)

    step = start_step("Build SP500 universe")
    universe = _build_sp500_universe_wrds(db, start, end)
    end_step(step)

    step = start_step("Build assets master")
    assets_master = _build_assets_master_wrds(db, universe)
    ipo_dates = _load_ipo_dates(db, universe)
    if not ipo_dates.empty:
        assets_master = assets_master.merge(ipo_dates, on="asset_id", how="left")
    assets_master = _dedupe_assets_master(assets_master)
    end_step(step)

    step = start_step("Build trading calendar and membership")
    calendar = _build_trading_calendar(start, end)
    membership = _build_universe_daily(universe, calendar)
    end_step(step)

    step = start_step("Build IBES↔CRSP mapping (CUSIP)")
    idxref = _load_idxref_mapping(db, universe, start, end)
    end_step(step)

    step = start_step("Download daily prices/returns")
    prices = _download_prices_wrds_full(db, universe, start, end)
    prices = _attach_tickers(prices, assets_master)
    returns = _build_returns_from_crsp(prices)
    dlret_daily = _load_dlret_daily(db, universe, start, end)
    returns = _apply_delist_returns(returns, dlret_daily)
    end_step(step)

    step = start_step("Download fundamentals")
    fundamentals = _build_fundamentals_wrds(db, universe, start, end)
    end_step(step)

    step = start_step("Download analyst consensus")
    analyst_consensus = _build_analyst_consensus_wrds(db, idxref, start, end)
    analyst_consensus = _dedupe_consensus(analyst_consensus)
    end_step(step)

    step = start_step("Download analyst rating history")
    analyst_ratings = _build_analyst_ratings_history_wrds(db, idxref, start, end)
    analyst_ratings = _dedupe_ratings_history(analyst_ratings)
    end_step(step)

    step = start_step("Download style factors and risk-free")
    style_factors, risk_free, ff_raw = _build_ff_factors_and_rf(db, start, end)
    end_step(step)

    step = start_step("Download macro series")
    macro = _build_macro_wrds(db, start, end)
    end_step(step)

    step = start_step("Download benchmark")
    benchmark = _build_benchmark_wrds(db, start, end)
    end_step(step)

    step = start_step("Download monthly prices/returns")
    prices_monthly = _download_monthly_wrds(db, universe, start, end)
    dlret_monthly = _load_dlret_monthly(db, universe, start, end)
    returns_monthly = _build_monthly_returns_from_crsp(prices_monthly, dlret_monthly)
    end_step(step)

    step = start_step("Download dividends")
    dividends = _load_dividends_monthly(db, universe, start, end)
    if not dividends.empty:
        div_merge = dividends.merge(prices_monthly[["asset_id", "date", "close"]], on=["asset_id", "date"], how="left")
        dividends = _clean_dividends(div_merge, prices)
    end_step(step)

    step = start_step("Write raw snapshots" if save_raw else "Skip raw snapshots")
    if save_raw:
        write_parquet(prices, raw_path / "prices_raw.parquet")
        write_parquet(universe, raw_path / "sp500_membership_raw.parquet")
        write_parquet(assets_master, raw_path / "assets_master_raw.parquet")
        write_parquet(fundamentals, raw_path / "fundamentals_raw.parquet")
        write_parquet(idxref, raw_path / "ibes_idxref_raw.parquet")
        write_parquet(analyst_consensus, raw_path / "analyst_consensus_raw.parquet")
        write_parquet(analyst_ratings, raw_path / "analyst_ratings_history_raw.parquet")
        write_parquet(ff_raw, raw_path / "style_factors_raw.parquet")
        write_parquet(macro, raw_path / "macro_raw.parquet")
        write_parquet(benchmark, raw_path / "benchmark_raw.parquet")
        write_parquet(prices_monthly, raw_path / "prices_monthly_raw.parquet")
        write_parquet(dlret_daily, raw_path / "dlret_daily_raw.parquet")
        write_parquet(dlret_monthly, raw_path / "dlret_monthly_raw.parquet")
        write_parquet(dividends, raw_path / "dividends_monthly_raw.parquet")
    end_step(step)

    step = start_step("Write processed datasets")
    write_parquet(prices, processed_path / "prices_daily.parquet")
    write_parquet(returns, processed_path / "returns_daily.parquet")
    write_parquet(membership, processed_path / "sp500_membership.parquet")
    write_parquet(fundamentals, processed_path / "fundamentals_quarterly.parquet")
    write_parquet(analyst_consensus, processed_path / "analyst_consensus.parquet")
    write_parquet(analyst_ratings, processed_path / "analyst_ratings_history.parquet")
    write_parquet(macro, processed_path / "macro_timeseries.parquet")
    write_parquet(risk_free, processed_path / "risk_free.parquet")
    write_parquet(style_factors, processed_path / "style_factor_returns.parquet")
    write_parquet(benchmark, processed_path / "benchmarks.parquet")
    write_parquet(returns_monthly, processed_path / "returns_monthly.parquet")
    write_parquet(dividends, processed_path / "dividends_monthly.parquet")
    end_step(step)

    step = start_step("Write metadata and manifests")
    write_parquet(assets_master, meta_path / "assets_master.parquet")
    write_parquet(membership.rename(columns={"in_sp500": "in_universe"}), meta_path / "universe_sp500.parquet")
    write_parquet(calendar, meta_path / "trading_calendar.parquet")

    sources_log_path = meta_path / "data_sources.yml"
    log = {
        "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
        "params": {"start": start, "end": end, "source": "wrds", "save_raw": save_raw},
        "datasets": {
            "prices_daily": {"source": "wrds_crsp_dsf", "path": str(processed_path / "prices_daily.parquet")},
            "returns_daily": {"source": "wrds_crsp_dsf_ret", "path": str(processed_path / "returns_daily.parquet")},
            "returns_monthly": {"source": "wrds_crsp_msf_ret_dlret", "path": str(processed_path / "returns_monthly.parquet")},
            "dividends_monthly": {"source": "wrds_crsp_msedist", "path": str(processed_path / "dividends_monthly.parquet")},
            "fundamentals_quarterly": {"source": "wrds_comp_funda", "path": str(processed_path / "fundamentals_quarterly.parquet")},
            "analyst_consensus": {"source": "wrds_tr_ibes_recdsum", "path": str(processed_path / "analyst_consensus.parquet")},
            "analyst_ratings_history": {
                "source": "wrds_det_rec",
                "path": str(processed_path / "analyst_ratings_history.parquet"),
            },
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
                "ibes_idxref_raw": str(raw_path / "ibes_idxref_raw.parquet") if save_raw else None,
                "analyst_consensus_raw": str(raw_path / "analyst_consensus_raw.parquet") if save_raw else None,
                "analyst_ratings_history_raw": str(raw_path / "analyst_ratings_history_raw.parquet") if save_raw else None,
                "style_factors_raw": str(raw_path / "style_factors_raw.parquet") if save_raw else None,
                "macro_raw": str(raw_path / "macro_raw.parquet") if save_raw else None,
                "benchmark_raw": str(raw_path / "benchmark_raw.parquet") if save_raw else None,
                "prices_monthly_raw": str(raw_path / "prices_monthly_raw.parquet") if save_raw else None,
                "dlret_daily_raw": str(raw_path / "dlret_daily_raw.parquet") if save_raw else None,
                "dlret_monthly_raw": str(raw_path / "dlret_monthly_raw.parquet") if save_raw else None,
                "dividends_monthly_raw": str(raw_path / "dividends_monthly_raw.parquet") if save_raw else None,
            },
        },
    }
    with sources_log_path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(log, f)
    logger.info("Wrote ingestion log to %s", sources_log_path)

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
                cols = cols or ["<unknown>"]
                for col in cols:
                    manifest.append(
                        {
                            "dataset": raw_name,
                            "type": "raw",
                            "source": "raw_snapshot",
                            "path": str(path),
                            "column": col,
                        }
                    )
            continue
        path = Path(info.get("path", ""))
        cols = _collect_columns(path) if path.exists() else []
        cols = cols or ["<unknown>"]
        for col in cols:
            manifest.append(
                {
                    "dataset": name,
                    "type": "processed",
                    "source": info.get("source"),
                    "path": str(path),
                    "column": col,
                }
            )

    manifest_path_yml = meta_path / "field_manifest.yml"
    manifest_path_csv = meta_path / "field_manifest.csv"
    manifest_path_csv_reference = reference_path / "field_manifest.csv"
    with manifest_path_yml.open("w", encoding="utf-8") as f:
        yaml.safe_dump(manifest, f)
    df_manifest = pd.DataFrame(manifest)
    df_manifest.to_csv(manifest_path_csv, index=False)
    df_manifest.to_csv(manifest_path_csv_reference, index=False)
    logger.info("Wrote field manifest to %s, %s, and %s", manifest_path_yml, manifest_path_csv, manifest_path_csv_reference)
    end_step(step)

    total_elapsed = sum(t for _, t in steps_done)
    summary = ", ".join(f"{name} {t:.1f}s" for name, t in steps_done)
    logger.info("Done in %.1fs. Steps: %s", total_elapsed, summary)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest S&P500 data from WRDS into local Parquet files.")
    parser.add_argument(
        "--root",
        type=Path,
        default=default_data_root(),
        help="Data root (defaults to env QUANTLAB_DATA_ROOT or ../quantlab_data next to repo).",
    )
    parser.add_argument("--start", type=str, default=DEFAULT_START, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=DEFAULT_END, help="End date (YYYY-MM-DD)")
    parser.add_argument("--save-raw", action="store_true", help="Also write raw WRDS price panel to data_raw/.")
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = parse_args()
    ingest(args.root, args.start, args.end, save_raw=args.save_raw)
