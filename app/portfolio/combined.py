"""
DOT Combined Portfolio — blends Global (USD) and Brazil (BRL → USD).

Conversão BRL → USD:
  - Brazil BRL index normalizado a 1.0 na data inicial comum.
  - Brazil USD norm(t) = Brazil BRL norm(t) / FX norm(t)
    onde FX norm(t) = USDBRL(t) / USDBRL(t0).
  - Se o BRL deprecia (USDBRL sobe), FX norm > 1 → valor USD do Brazil cai.
  - Se o BRL aprecia (USDBRL cai), FX norm < 1 → valor USD do Brazil sobe.

Combinação:
  - Retorno diário DOT = w_global × r_global(t) + w_brazil × r_brazil_usd(t)
  - DOT index acumulado a partir de 1.0 na data inicial comum.
"""

from datetime import date
from typing import Optional

import pandas as pd

from app.database import get_db_for
from app.portfolio.performance import snapshot_series
from app.market_data.fx_provider import fetch_usdbrl
from app.market_data.cpi_provider import fetch_cpi_daily
from app.market_data.ipca_provider import fetch_ipca_daily


def _common_start(global_snap: pd.Series, brazil_snap: pd.Series) -> date:
    return max(global_snap.index[0], brazil_snap.index[0])


def _get_snaps(cutoff: Optional[date]) -> tuple[pd.Series, pd.Series]:
    db_g = get_db_for("global")
    try:
        g = snapshot_series(db_g, cutoff=cutoff)
    finally:
        db_g.close()

    db_b = get_db_for("brazil")
    try:
        b = snapshot_series(db_b, cutoff=cutoff)
    finally:
        db_b.close()

    return g, b


def _rebal_key(d: date, freq: str):
    """Retorna a chave do período de rebalanceamento para uma data."""
    if freq == "monthly":
        return (d.year, d.month)
    if freq == "annual":
        return d.year
    return d  # daily: cada dia é seu próprio período


def compute_dot_series(
    cutoff: Optional[date] = None,
    w_brazil: float = 0.5,
    w_global: float = 0.5,
    rebal_freq: str = "monthly",
) -> pd.Series:
    """
    DOT combined index (base = 1.0 at common start date), in USD.

    rebal_freq: 'daily' | 'monthly' | 'annual'
      - Rastreia o valor de cada componente separadamente.
      - No primeiro dia de cada período (mês/ano) rebalanceia aos pesos-alvo.
      - Entre rebalanceamentos os pesos flutuam livremente (buy-and-hold).
    """
    global_snap, brazil_snap = _get_snaps(cutoff)
    if global_snap.empty or brazil_snap.empty:
        return pd.Series(dtype=float)

    t0    = _common_start(global_snap, brazil_snap)
    t_end = cutoff or min(global_snap.index[-1], brazil_snap.index[-1])

    global_snap = global_snap[global_snap.index >= t0]
    brazil_snap = brazil_snap[brazil_snap.index >= t0]

    fx = fetch_usdbrl(t0, t_end)
    if fx.empty:
        return pd.Series(dtype=float)

    all_dates = sorted(
        set(global_snap.index) | set(brazil_snap.index) | set(fx.index)
    )
    all_dates = [d for d in all_dates if t0 <= d <= t_end]

    df = pd.DataFrame(index=all_dates)
    df["g"]  = global_snap.reindex(all_dates)
    df["b"]  = brazil_snap.reindex(all_dates)
    df["fx"] = fx.reindex(all_dates)
    df = df.ffill().bfill().dropna()

    if len(df) < 2:
        return pd.Series(dtype=float)

    # Brazil em USD normalizado
    df["g_norm"]     = df["g"]  / df["g"].iloc[0]
    df["b_norm"]     = df["b"]  / df["b"].iloc[0]
    df["fx_norm"]    = df["fx"] / df["fx"].iloc[0]
    df["b_usd_norm"] = df["b_norm"] / df["fx_norm"]

    dates     = list(df.index)
    g_vals    = df["g_norm"].values
    b_vals    = df["b_usd_norm"].values

    # Valor de cada componente dentro do DOT (começa nos pesos-alvo)
    val_g = w_global
    val_b = w_brazil
    dot_vals = [1.0]

    seen_periods: set = set()
    seen_periods.add(_rebal_key(dates[0], rebal_freq))  # primeiro dia já "rebalanceado"

    for i in range(1, len(dates)):
        d = dates[i]

        # Retorno diário de cada componente
        r_g = (g_vals[i] / g_vals[i - 1]) - 1.0 if g_vals[i - 1] != 0 else 0.0
        r_b = (b_vals[i] / b_vals[i - 1]) - 1.0 if b_vals[i - 1] != 0 else 0.0

        val_g *= (1.0 + r_g)
        val_b *= (1.0 + r_b)
        total  = val_g + val_b
        dot_vals.append(total)

        # Rebalanceia no primeiro dia de cada novo período
        period = _rebal_key(d, rebal_freq)
        if period not in seen_periods:
            seen_periods.add(period)
            val_g = total * w_global
            val_b = total * w_brazil

    return pd.Series(dot_vals, index=dates)


def compute_global_usd_norm(cutoff: Optional[date] = None) -> pd.Series:
    """Global portfolio normalizado a 1.0 na data inicial comum (USD)."""
    global_snap, brazil_snap = _get_snaps(cutoff)
    if global_snap.empty or brazil_snap.empty:
        return pd.Series(dtype=float)

    t0 = _common_start(global_snap, brazil_snap)
    global_snap = global_snap[global_snap.index >= t0]
    if global_snap.empty:
        return pd.Series(dtype=float)

    return global_snap / global_snap.iloc[0]


def compute_brazil_usd_norm(cutoff: Optional[date] = None) -> pd.Series:
    """Brazil portfolio convertido para USD, normalizado a 1.0 na data inicial comum."""
    global_snap, brazil_snap = _get_snaps(cutoff)
    if global_snap.empty or brazil_snap.empty:
        return pd.Series(dtype=float)

    t0 = _common_start(global_snap, brazil_snap)
    t_end = cutoff or min(global_snap.index[-1], brazil_snap.index[-1])

    brazil_snap = brazil_snap[brazil_snap.index >= t0]
    fx = fetch_usdbrl(t0, t_end)

    if brazil_snap.empty or fx.empty:
        return pd.Series(dtype=float)

    all_dates = sorted(set(brazil_snap.index) | set(fx.index))
    all_dates = [d for d in all_dates if t0 <= d <= t_end]

    df = pd.DataFrame(index=all_dates)
    df["b"]  = brazil_snap.reindex(all_dates)
    df["fx"] = fx.reindex(all_dates)
    df = df.ffill().bfill().dropna()

    if df.empty:
        return pd.Series(dtype=float)

    df["b_norm"]  = df["b"]  / df["b"].iloc[0]
    df["fx_norm"] = df["fx"] / df["fx"].iloc[0]

    return df["b_norm"] / df["fx_norm"]


def _returns_from_series(s: pd.Series) -> dict:
    """Compute daily / weekly / MTD / YTD / since-inception returns from an index series."""
    if s.empty or len(s) < 2:
        return dict(daily=None, weekly=None, mtd=None, ytd=None, total=None, last_date=None)

    last_date = s.index[-1]
    last_val  = s.iloc[-1]
    first_val = s.iloc[0]

    # Daily
    daily = (last_val / s.iloc[-2]) - 1.0 if len(s) >= 2 else None

    # Weekly (últimos 7 dias)
    from datetime import timedelta
    week_ago = last_date - timedelta(days=7)
    w_s = s[s.index >= week_ago]
    weekly = (w_s.iloc[-1] / w_s.iloc[0]) - 1.0 if len(w_s) >= 2 else None

    # MTD
    month_start = last_date.replace(day=1)
    mtd_s = s[s.index >= month_start]
    mtd = (mtd_s.iloc[-1] / mtd_s.iloc[0]) - 1.0 if len(mtd_s) >= 2 else None

    # YTD
    year_start = last_date.replace(month=1, day=1)
    inception  = s.index[0]
    ytd_ref    = inception if inception >= year_start else year_start
    ytd_s = s[s.index >= ytd_ref]
    ytd = (ytd_s.iloc[-1] / ytd_s.iloc[0]) - 1.0 if len(ytd_s) >= 2 else None

    # Since inception
    total = (last_val / first_val) - 1.0

    return dict(daily=daily, weekly=weekly, mtd=mtd, ytd=ytd, total=total, last_date=last_date)


def _apply_multiplier(series: pd.Series, multiplier: float) -> pd.Series:
    if multiplier == 1.0 or series.empty:
        return series
    daily  = series.pct_change().fillna(0.0)
    result = (1.0 + daily * multiplier).cumprod()
    return result / result.iloc[0]


def compute_blended_benchmark(
    cutoff: Optional[date] = None,
    w_brazil: float = 0.5,
    w_global: float = 0.5,
    multiplier: float = 1.5,
) -> pd.Series:
    """
    Benchmark combinado para o DOT Portfolio (base = 1.0 na data inicial comum).

    - Componente Global: multiplier × CPI (USD)
    - Componente Brazil: multiplier × IPCA (BRL → USD via FX)
    - Blendado pelos mesmos pesos do portfolio DOT.
    """
    global_snap, brazil_snap = _get_snaps(cutoff)
    if global_snap.empty or brazil_snap.empty:
        return pd.Series(dtype=float)

    t0    = _common_start(global_snap, brazil_snap)
    t_end = cutoff or date.today()

    # CPI (USD) com multiplicador
    cpi_raw = fetch_cpi_daily(t0, t_end)
    if cpi_raw.empty:
        return pd.Series(dtype=float)
    cpi = _apply_multiplier(cpi_raw, multiplier)

    # IPCA (BRL) com multiplicador
    ipca_raw = fetch_ipca_daily(t0, t_end)
    if ipca_raw.empty:
        return pd.Series(dtype=float)
    ipca = _apply_multiplier(ipca_raw, multiplier)

    # FX para converter IPCA BRL → USD
    fx = fetch_usdbrl(t0, t_end)
    if fx.empty:
        return pd.Series(dtype=float)

    all_dates = sorted(set(cpi.index) | set(ipca.index) | set(fx.index))
    all_dates  = [d for d in all_dates if t0 <= d <= t_end]

    df = pd.DataFrame(index=all_dates)
    df["cpi"]  = cpi.reindex(all_dates)
    df["ipca"] = ipca.reindex(all_dates)
    df["fx"]   = fx.reindex(all_dates)
    df = df.ffill().bfill().dropna()

    if len(df) < 2:
        return pd.Series(dtype=float)

    df["fx_norm"]   = df["fx"]   / df["fx"].iloc[0]
    df["ipca_usd"]  = df["ipca"] / df["fx_norm"]   # IPCA convertido para USD

    df["r_cpi"]  = df["cpi"].pct_change().fillna(0.0)
    df["r_ipca"] = df["ipca_usd"].pct_change().fillna(0.0)
    df["r_bench"] = w_global * df["r_cpi"] + w_brazil * df["r_ipca"]

    vals = [1.0]
    for r in df["r_bench"].iloc[1:]:
        vals.append(vals[-1] * (1.0 + r))

    return pd.Series(vals, index=df.index)
