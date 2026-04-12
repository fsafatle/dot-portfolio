"""FX rate provider — USDBRL via Yahoo Finance."""

from datetime import date, timedelta
import logging

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)

_TICKER = "BRL=X"   # Yahoo Finance: USD/BRL (BRL per 1 USD)


def fetch_usdbrl(start: date, end: date) -> pd.Series:
    """
    Return daily USDBRL closing rates as Series[date → float].
    Rate = BRL per 1 USD (e.g. 5.85 means 1 USD = 5.85 BRL).
    When BRL depreciates the rate goes UP → USD value of BRL assets decreases.
    """
    end_dt = end + timedelta(days=1)  # yfinance end is exclusive
    try:
        t = yf.Ticker(_TICKER)
        raw = t.history(
            start=start.isoformat(),
            end=end_dt.isoformat(),
            auto_adjust=True,
        )
    except Exception as exc:
        logger.error("yfinance USDBRL failed: %s", exc)
        return pd.Series(dtype=float)

    if raw is None or raw.empty:
        logger.warning("No USDBRL data for %s → %s", start, end)
        return pd.Series(dtype=float)

    close = raw["Close"]
    close.index = pd.to_datetime(close.index).date
    return close.dropna()
