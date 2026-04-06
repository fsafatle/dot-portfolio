"""
CDI benchmark provider — fetches daily CDI rates from Banco Central do Brasil (BCB)
and returns a normalized accumulated index (base = 1.0 at the portfolio start date).

Source: BCB SGS series 11 — CDI Over (taxa diária em % a.d.)
API: https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados
"""

from datetime import date

import pandas as pd
import requests

BCB_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados"
TIMEOUT = 15


def fetch_cdi_daily(start: date, end: date) -> pd.Series:
    """
    Return an accumulated CDI index normalized to 1.0 on `start`.

    Fetches directly from BCB API each time — caching is handled at the
    Streamlit layer via @st.cache_data in dashboard.py.
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
        print(f"[cdi_provider] BCB API error: {exc}")
        return pd.Series(dtype=float)

    records: dict[date, float] = {}
    for row in data:
        try:
            dt  = date(int(row["data"][6:]), int(row["data"][3:5]), int(row["data"][:2]))
            val = float(row["valor"]) / 100.0   # % a.d. → decimal
            records[dt] = val
        except (KeyError, ValueError):
            continue

    if not records:
        return pd.Series(dtype=float)

    raw = pd.Series(records)

    # Fill calendar gaps using forward-fill of the last known rate
    all_dates = pd.date_range(start, end, freq="D")
    raw = raw.reindex([d.date() for d in all_dates]).ffill().dropna()

    # Build accumulated index — base 1.0 on start date
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
        series = series / series.iloc[0]   # re-normalize anchor to exactly 1.0

    return series
