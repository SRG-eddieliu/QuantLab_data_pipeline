from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd

DateLike = str
AssetLike = Iterable[str]


class DataHandler(ABC):
    """
    Abstract interface for unified data access.

    All downstream modules must depend on this API rather than reading
    files directly so backends can be swapped without refactors.
    """

    def __init__(self, data_root: Path):
        self.data_root = Path(data_root).expanduser().resolve()

    @abstractmethod
    def get_prices(
        self,
        tickers: AssetLike | None,
        start_date: DateLike | None = None,
        end_date: DateLike | None = None,
        fields: Optional[list[str]] = None,
    ) -> pd.DataFrame:
        """Return daily prices dataframe filtered by tickers/date range."""

    @abstractmethod
    def get_returns(
        self,
        tickers: AssetLike | None,
        start_date: DateLike | None = None,
        end_date: DateLike | None = None,
    ) -> pd.DataFrame:
        """Return daily returns dataframe filtered by tickers/date range."""

    @abstractmethod
    def get_universe(
        self, date: DateLike | None = None
    ) -> pd.DataFrame:
        """Return universe membership; if date provided, filter to that date."""

    @abstractmethod
    def get_fundamentals(
        self,
        tickers: AssetLike | None,
        start_date: DateLike | None = None,
        end_date: DateLike | None = None,
    ) -> pd.DataFrame:
        """Return fundamentals panels."""

    @abstractmethod
    def get_macro(
        self,
        start_date: DateLike | None = None,
        end_date: DateLike | None = None,
    ) -> pd.DataFrame:
        """Return macro timeseries."""

    @abstractmethod
    def get_style_factor_returns(
        self,
        start_date: DateLike | None = None,
        end_date: DateLike | None = None,
    ) -> pd.DataFrame:
        """Return style factor returns."""

    @abstractmethod
    def get_benchmark_returns(
        self,
        benchmark: str,
        start_date: DateLike | None = None,
        end_date: DateLike | None = None,
    ) -> pd.DataFrame:
        """Return benchmark price/returns."""

