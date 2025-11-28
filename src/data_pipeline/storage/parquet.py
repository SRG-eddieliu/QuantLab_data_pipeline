from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd
import yaml

from ..config import resolve_data_root
from ..interfaces import AssetLike, DataHandler, DateLike


class LocalParquetDataHandler(DataHandler):
    """
    Local parquet-backed implementation of DataHandler for on-disk datasets.

    Keeps downstream processing decoupled from the physical storage layout so
    the backend can be swapped without refactors.
    """

    def __init__(
        self,
        data_root: Path | str | None = None,
        processed_dir: str = "data_processed",
        meta_dir: str = "data_meta",
    ):
        root = resolve_data_root(data_root)
        super().__init__(root)
        self.processed_path = (root / processed_dir).resolve()
        self.meta_path = (root / meta_dir).resolve()
        self._assets_master_cache: Optional[pd.DataFrame] = None
        self._field_map = self._load_field_mapping()

    @staticmethod
    def _load_field_mapping() -> dict[str, dict[str, str]]:
        path = Path(__file__).resolve().parents[3] / "config" / "wrds_field_map.yml"
        if not path.exists():
            return {}
        data = yaml.safe_load(path.read_text()) or {}
        return {section: mapping or {} for section, mapping in data.items()}

    def _read_parquet(self, path: Path, parse_dates: Optional[list[str]] = None) -> pd.DataFrame:
        if not path.exists():
            raise FileNotFoundError(f"Missing dataset at {path}")
        df = pd.read_parquet(path)
        if parse_dates:
            for col in parse_dates:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col])
        return df

    def _assets_master(self) -> pd.DataFrame:
        if self._assets_master_cache is None:
            path = self.meta_path / "assets_master.parquet"
            self._assets_master_cache = self._read_parquet(path, parse_dates=["first_date", "last_date"])
        return self._assets_master_cache

    def _tickers_to_asset_ids(self, tickers: AssetLike | None) -> list[int]:
        if tickers is None:
            return []
        assets = self._assets_master()
        mapping = dict(zip(assets["ticker"], assets["asset_id"]))
        missing = [t for t in tickers if t not in mapping]
        if missing:
            raise ValueError(f"Tickers not found in assets_master: {missing}")
        return [mapping[t] for t in tickers]

    @staticmethod
    def _filter_dates(df: pd.DataFrame, start_date: DateLike | None, end_date: DateLike | None) -> pd.DataFrame:
        if "date" not in df.columns:
            return df
        if start_date:
            df = df[df["date"] >= pd.to_datetime(start_date)]
        if end_date:
            df = df[df["date"] <= pd.to_datetime(end_date)]
        return df

    @staticmethod
    def _filter_fields(df: pd.DataFrame, fields: Optional[list[str]], mandatory: list[str]) -> pd.DataFrame:
        if not fields:
            return df
        keep = list(dict.fromkeys(mandatory + fields))  # preserve order, ensure mandatory retained
        missing = [f for f in keep if f not in df.columns]
        if missing:
            raise ValueError(f"Requested fields missing from dataset: {missing}")
        return df[keep]

    def get_prices(
        self,
        tickers: AssetLike | None,
        start_date: DateLike | None = None,
        end_date: DateLike | None = None,
        fields: Optional[list[str]] = None,
    ) -> pd.DataFrame:
        df = self._read_parquet(self.processed_path / "prices_daily.parquet", parse_dates=["date"])
        asset_ids = self._tickers_to_asset_ids(tickers) if tickers else None
        if asset_ids:
            df = df[df["asset_id"].isin(asset_ids)]
        df = self._filter_dates(df, start_date, end_date)
        df = self._filter_fields(df, fields, mandatory=["date", "asset_id", "ticker"])
        return df.sort_values(["date", "asset_id"]).reset_index(drop=True)

    def get_returns(
        self,
        tickers: AssetLike | None,
        start_date: DateLike | None = None,
        end_date: DateLike | None = None,
    ) -> pd.DataFrame:
        df = self._read_parquet(self.processed_path / "returns_daily.parquet", parse_dates=["date"])
        asset_ids = self._tickers_to_asset_ids(tickers) if tickers else None
        if asset_ids:
            df = df[df["asset_id"].isin(asset_ids)]
        df = self._filter_dates(df, start_date, end_date)
        return df.sort_values(["date", "asset_id"]).reset_index(drop=True)

    def get_universe(self, date: DateLike | None = None) -> pd.DataFrame:
        df = self._read_parquet(self.meta_path / "universe_sp500.parquet", parse_dates=["date"])
        if date:
            df = df[df["date"] == pd.to_datetime(date)]
        return df.sort_values(["date", "asset_id"]).reset_index(drop=True)

    def get_fundamentals(
        self,
        tickers: AssetLike | None,
        start_date: DateLike | None = None,
        end_date: DateLike | None = None,
    ) -> pd.DataFrame:
        df = self._read_parquet(self.processed_path / "fundamentals_quarterly.parquet", parse_dates=["report_date"])
        asset_ids = self._tickers_to_asset_ids(tickers) if tickers else None
        if asset_ids:
            df = df[df["asset_id"].isin(asset_ids)]
        if start_date:
            df = df[df["report_date"] >= pd.to_datetime(start_date)]
        if end_date:
            df = df[df["report_date"] <= pd.to_datetime(end_date)]
        mapping = self._field_map.get("fundamentals", {})
        if mapping:
            df = df.rename(columns={k: v for k, v in mapping.items() if k in df.columns})
        return df.sort_values(["report_date", "asset_id"]).reset_index(drop=True)

    def get_analyst_consensus(
        self,
        tickers: AssetLike | None,
        start_date: DateLike | None = None,
        end_date: DateLike | None = None,
        fields: Optional[list[str]] = None,
    ) -> pd.DataFrame:
        df = self._read_parquet(self.processed_path / "analyst_consensus.parquet", parse_dates=["date"])
        asset_ids = self._tickers_to_asset_ids(tickers) if tickers else None
        if asset_ids:
            df = df[df["asset_id"].isin(asset_ids)]
        df = self._filter_dates(df, start_date, end_date)
        df = self._filter_fields(df, fields, mandatory=["date", "asset_id", "ticker"])
        return df.sort_values(["date", "asset_id"]).reset_index(drop=True)

    def get_analyst_ratings_history(
        self,
        tickers: AssetLike | None,
        start_date: DateLike | None = None,
        end_date: DateLike | None = None,
        fields: Optional[list[str]] = None,
    ) -> pd.DataFrame:
        df = self._read_parquet(self.processed_path / "analyst_ratings_history.parquet", parse_dates=["date"])
        asset_ids = self._tickers_to_asset_ids(tickers) if tickers else None
        if asset_ids:
            df = df[df["asset_id"].isin(asset_ids)]
        df = self._filter_dates(df, start_date, end_date)
        df = self._filter_fields(df, fields, mandatory=["date", "asset_id", "ticker"])
        return df.sort_values(["date", "asset_id"]).reset_index(drop=True)

    def get_macro(
        self,
        start_date: DateLike | None = None,
        end_date: DateLike | None = None,
    ) -> pd.DataFrame:
        df = self._read_parquet(self.processed_path / "macro_timeseries.parquet", parse_dates=["date"])
        df = self._filter_dates(df, start_date, end_date)
        return df.sort_values(["date", "series_name"]).reset_index(drop=True)

    def get_style_factor_returns(
        self,
        start_date: DateLike | None = None,
        end_date: DateLike | None = None,
    ) -> pd.DataFrame:
        df = self._read_parquet(self.processed_path / "style_factor_returns.parquet", parse_dates=["date"])
        df = self._filter_dates(df, start_date, end_date)
        return df.sort_values(["date", "factor_name"]).reset_index(drop=True)

    def get_benchmark_returns(
        self,
        benchmark: str,
        start_date: DateLike | None = None,
        end_date: DateLike | None = None,
    ) -> pd.DataFrame:
        df = self._read_parquet(self.processed_path / "benchmarks.parquet", parse_dates=["date"])
        if "benchmark_name" not in df.columns:
            # Backwards-compatibility: older files may lack this field
            if "ticker" in df.columns:
                df = df.rename(columns={"ticker": "benchmark_name"})
            else:
                df["benchmark_name"] = benchmark
        df = df[df["benchmark_name"] == benchmark]
        df = self._filter_dates(df, start_date, end_date)
        return df.sort_values("date").reset_index(drop=True)
