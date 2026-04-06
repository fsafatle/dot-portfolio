"""
Fetch US CPI-U (All Items) from the BLS public API.

No API key required for v1 (rate-limited to ~25 req/day per IP).
Series: CUUR0000SA0 = CPI-U, All items, US City Average, Not Seasonally Adjusted.
"""

import logging
from datetime import date

import pandas as pd
import requests

logger = logging.getLogger(__name__)

BLS_URL = "https://api.bls.gov/publicAPI/v1/timeseries/data/CUUR0000SA0"


def fetch_cpi_daily(start: date, end: date) -> pd.Series:
    """
    Return a Series[date → float] of daily CPI values, interpolated linearly
    from monthly BLS data.  Normalized so that start == 1000 (same base as
    the portfolio index).

    Returns an empty Series on failure — the chart simply omits the CPI line.
    """
    try:
        resp = requests.post(
            BLS_URL,
            json={
                "seriesid": ["CUUR0000SA0"],
                "startyear": str(start.year),
                "endyear": str(end.year),
            },
            timeout=15,
        )
        resp.raise_for_status()
        payload = resp.json()

        if payload.get("status") != "REQUEST_SUCCEEDED":
            logger.warning("BLS API returned status: %s", payload.get("status"))
            return pd.Series(dtype=float)

        raw = payload["Results"]["series"][0]["data"]
        records = []
        for item in raw:
            try:
                month = int(item["period"][1:])   # "M01" → 1
                records.append((date(int(item["year"]), month, 1), float(item["value"])))
            except (KeyError, ValueError):
                continue

        if not records:
            return pd.Series(dtype=float)

        records.sort()
        monthly = pd.Series({d: v for d, v in records})
        monthly.index = pd.to_datetime(monthly.index)

        # Reindex to daily and interpolate
        daily_idx = pd.date_range(
            start=monthly.index.min(),
            end=pd.Timestamp(end),
            freq="D",
        )
        daily = monthly.reindex(daily_idx).interpolate(method="linear").ffill()

        # Clip to requested range and convert index to date
        daily = daily[daily.index >= pd.Timestamp(start)]
        daily = daily[daily.index <= pd.Timestamp(end)]
        daily.index = daily.index.date  # type: ignore[assignment]

        # Normalize to 1000 at start
        if len(daily) == 0 or daily.iloc[0] == 0:
            return pd.Series(dtype=float)

        return daily / daily.iloc[0]

    except Exception as exc:
        logger.warning("CPI fetch failed: %s", exc)
        return pd.Series(dtype=float)
