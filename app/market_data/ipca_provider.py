"""
IPCA benchmark provider — fetches monthly IPCA rates from Banco Central do Brasil (BCB)
and returns a normalized accumulated index (base = 1.0 at the portfolio start date).

Source: BCB SGS série 433 — IPCA (variação mensal em %)
API: https://api.bcb.gov.br/dados/serie/bcdata.sgs.433/dados
"""

import calendar
from datetime import date

import pandas as pd
import requests

BCB_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.433/dados"
TIMEOUT = 15


def fetch_ipca_daily(start: date, end: date) -> pd.Series:
    """
    Return an accumulated IPCA index normalized to 1.0 on `start`.

    Monthly rates are converted to equivalent daily rates by compounding
    evenly across all calendar days of each month.
    Caching is handled at the Streamlit layer via @st.cache_data in dashboard.py.
    """
    params = {
        "formato":     "json",
        "dataInicial": start.strftime("%d/%m/%Y"),
        "dataFinal":   end.strftime("%d/%m/%Y"),
    }
    try:
        resp = requests.get(BCB_URL, params=params, timeout=TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        print(f"[ipca_provider] BCB API error: {exc}")
        return pd.Series(dtype=float)

    # BCB returns rows like {"data": "01/01/2026", "valor": "0.52"}
    monthly: dict[date, float] = {}
    for row in data:
        try:
            dt  = date(int(row["data"][6:]), int(row["data"][3:5]), int(row["data"][:2]))
            val = float(row["valor"]) / 100.0  # % → decimal
            monthly[dt] = val
        except (KeyError, ValueError):
            continue

    if not monthly:
        return pd.Series(dtype=float)

    # Spread each monthly rate as an equivalent daily rate across the month's days
    daily_rates: dict[date, float] = {}
    for month_ref, monthly_rate in sorted(monthly.items()):
        year, month = month_ref.year, month_ref.month
        n_days = calendar.monthrange(year, month)[1]
        daily_rate = (1.0 + monthly_rate) ** (1.0 / n_days) - 1.0
        for day in range(1, n_days + 1):
            d = date(year, month, day)
            if start <= d <= end:
                daily_rates[d] = daily_rate

    if not daily_rates:
        return pd.Series(dtype=float)

    # Fill any missing calendar days with the last known daily rate
    all_dates = pd.date_range(start, end, freq="D")
    raw = (
        pd.Series(daily_rates)
          .reindex([d.date() for d in all_dates])
          .ffill()
          .fillna(0.0)
    )

    # Accumulate — base 1.0 on start date
    index_val = 1.0
    accumulated: dict[date, float] = {}
    first = True
    for dt, rate in sorted(raw.items()):
        if first:
            accumulated[dt] = 1.0
            first = False
        else:
            index_val *= (1.0 + rate)
            accumulated[dt] = index_val

    series = pd.Series(accumulated)
    if not series.empty:
        series = series / series.iloc[0]  # re-anchor exactly at 1.0

    return series
