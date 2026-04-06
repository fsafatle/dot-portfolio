import logging
from datetime import date, timedelta

import numpy as np
import pandas as pd
import yfinance as yf

from app.market_data.base import MarketDataProvider

logger = logging.getLogger(__name__)


class YahooProvider(MarketDataProvider):
    """Fetches total-return price data from Yahoo Finance via yfinance."""

    def fetch_prices(
        self,
        ticker: str,
        start: date,
        end: date,
        data_source: str = "yahoo",
    ) -> pd.Series:
        """
        Return a Series[date → float] of total-return prices.

        Uses yf.Ticker.history() with auto_adjust=True, which applies
        dividend and split adjustments to every historical close price —
        giving a true total-return series (price appreciation + distributions).

        For data_source == 'yahoo_rate' (e.g. ^IRX), the annualised rate is
        converted to a simulated NAV instead.
        """
        end_dt = end + timedelta(days=1)   # yfinance end is exclusive
        try:
            t = yf.Ticker(ticker)
            raw = t.history(
                start=start.isoformat(),
                end=end_dt.isoformat(),
                auto_adjust=True,   # Close = dividend & split adjusted (total return)
                actions=True,       # Include dividends so adjustment is applied immediately
            )
        except Exception as exc:
            logger.error("yfinance history failed for %s: %s", ticker, exc)
            return pd.Series(dtype=float)

        if raw is None or raw.empty:
            logger.warning("No data returned for %s (%s → %s)", ticker, start, end)
            return pd.Series(dtype=float)

        close = raw["Close"]
        close.index = pd.to_datetime(close.index).date  # type: ignore[assignment]
        close = close.dropna()

        if data_source == "yahoo_rate":
            close = self._rate_to_nav(close)

        return close

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _rate_to_nav(rate_series: pd.Series, start_nav: float = 100.0) -> pd.Series:
        """
        Convert an annualised rate series (% per year, e.g. 4.3 ≡ 4.3 % p.a.)
        to a simulated NAV.

        Compounding convention: 252 trading days per year.
        NAV[t] = NAV[t-1] × (1 + rate[t] / 100)^(1/252)
        """
        rates = rate_series.values
        navs = np.empty(len(rates))
        navs[0] = start_nav
        for i in range(1, len(rates)):
            daily_r = (1.0 + rates[i] / 100.0) ** (1.0 / 252.0) - 1.0
            navs[i] = navs[i - 1] * (1.0 + daily_r)
        return pd.Series(navs, index=rate_series.index)
