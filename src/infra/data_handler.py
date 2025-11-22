from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Iterable


class DataHandler(ABC):
    """
    Abstract interface for all data access.

    Concrete implementations should handle reading/writing from the chosen
    storage backend (local Parquet, database, object store, etc.).
    """

    @abstractmethod
    def get_prices(self, assets: Iterable[str], start_date: str, end_date: str) -> Any:  # pragma: no cover - interface only
        raise NotImplementedError

    @abstractmethod
    def get_returns(self, assets: Iterable[str], start_date: str, end_date: str) -> Any:  # pragma: no cover - interface only
        raise NotImplementedError

    @abstractmethod
    def get_fundamentals(self, assets: Iterable[str], start_date: str, end_date: str) -> Any:  # pragma: no cover - interface only
        raise NotImplementedError

    @abstractmethod
    def get_universe(self, date: str) -> Any:  # pragma: no cover - interface only
        raise NotImplementedError

    @abstractmethod
    def get_macro(self, start_date: str, end_date: str) -> Any:  # pragma: no cover - interface only
        raise NotImplementedError

    @abstractmethod
    def get_style_factor_returns(self, start_date: str, end_date: str) -> Any:  # pragma: no cover - interface only
        raise NotImplementedError

    @abstractmethod
    def get_benchmark_returns(self, benchmark: str, start_date: str, end_date: str) -> Any:  # pragma: no cover - interface only
        raise NotImplementedError

