from abc import ABC, abstractmethod
from datetime import date
import pandas as pd


class MarketDataProvider(ABC):
    """Abstract interface for market data.

    All providers must return a Series with DatetimeIndex and float values
    representing closing prices (or simulated NAVs for rate-based assets).
    """

    @abstractmethod
    def fetch_prices(
        self,
        ticker: str,
        start: date,
        end: date,
        data_source: str = "yahoo",
    ) -> pd.Series:
        """Return a Series[date → float] of closing prices between start and end."""
        ...
