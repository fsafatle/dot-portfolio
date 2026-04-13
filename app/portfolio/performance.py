"""Performance analytics: returns, attribution, cumulative series."""

from datetime import date, timedelta
from typing import Optional

import pandas as pd
from sqlalchemy.orm import Session

from app.models import Asset, Allocation, PortfolioSnapshot, Price
from app.portfolio.engine import get_active_allocations, get_prices_df, get_actual_weights


def snapshot_series(db: Session, cutoff: Optional[date] = None) -> pd.Series:
    """Return portfolio index as a Series[date → float]."""
    q = db.query(PortfolioSnapshot).order_by(PortfolioSnapshot.date)
    if cutoff:
        q = q.filter(PortfolioSnapshot.date <= cutoff)
    rows = q.all()
    if not rows:
        return pd.Series(dtype=float)
    return pd.Series({r.date: r.index_value for r in rows})


def daily_return_series(db: Session, cutoff: Optional[date] = None) -> pd.Series:
    """Return daily returns as a Series[date → float]."""
    q = (
        db.query(PortfolioSnapshot)
        .filter(PortfolioSnapshot.daily_return.isnot(None))
        .order_by(PortfolioSnapshot.date)
    )
    if cutoff:
        q = q.filter(PortfolioSnapshot.date <= cutoff)
    rows = q.all()
    return pd.Series({r.date: r.daily_return for r in rows})


def total_return(db: Session, cutoff: Optional[date] = None) -> Optional[float]:
    """Total return since inception (decimal)."""
    q = db.query(PortfolioSnapshot).order_by(PortfolioSnapshot.date)
    if cutoff:
        q = q.filter(PortfolioSnapshot.date <= cutoff)
    rows = q.all()
    if len(rows) < 2:
        return None
    return (rows[-1].index_value / rows[0].index_value) - 1.0


def latest_daily_return(db: Session, cutoff: Optional[date] = None) -> Optional[float]:
    q = (
        db.query(PortfolioSnapshot)
        .filter(PortfolioSnapshot.daily_return.isnot(None))
        .order_by(PortfolioSnapshot.date.desc())
    )
    if cutoff:
        q = q.filter(PortfolioSnapshot.date <= cutoff)
    row = q.first()
    return row.daily_return if row else None


def latest_snapshot_date(db: Session, cutoff: Optional[date] = None) -> Optional[date]:
    q = db.query(PortfolioSnapshot).order_by(PortfolioSnapshot.date.desc())
    if cutoff:
        q = q.filter(PortfolioSnapshot.date <= cutoff)
    row = q.first()
    return row.date if row else None


def latest_index_value(db: Session, cutoff: Optional[date] = None) -> Optional[float]:
    q = db.query(PortfolioSnapshot).order_by(PortfolioSnapshot.date.desc())
    if cutoff:
        q = q.filter(PortfolioSnapshot.date <= cutoff)
    row = q.first()
    return row.index_value if row else None


def _nearest_price(col: pd.Series, target: date) -> Optional[float]:
    """Return the price on or before target date."""
    candidates = col[col.index <= target]
    return float(candidates.iloc[-1]) if not candidates.empty else None


def _nearest_price_after(col: pd.Series, target: date) -> Optional[float]:
    """Return the price on or after target date (first available trading day)."""
    candidates = col[col.index >= target]
    return float(candidates.iloc[0]) if not candidates.empty else None


def asset_performance(db: Session, cutoff: Optional[date] = None) -> pd.DataFrame:
    """
    Return a DataFrame with per-asset performance stats.

    Columns: name, ticker, asset_class, target_weight, current_weight,
             start_date, daily_return, mtd_return, ytd_return, ret_12m, total_return

    Source of truth (priority):
      1. V2 engine — assets with position in AssetSnapshot (motor de transações)
      2. Legacy — assets with active Allocation entry
    Prices: legacy Price table first; AssetPrice (v2) as fallback for manual assets.
    """
    first_snap = db.query(PortfolioSnapshot).order_by(PortfolioSnapshot.date).first()
    if first_snap is None:
        return pd.DataFrame()

    start = first_snap.date
    end_q = db.query(PortfolioSnapshot).order_by(PortfolioSnapshot.date.desc())
    if cutoff:
        end_q = end_q.filter(PortfolioSnapshot.date <= cutoff)
    end_row = end_q.first()
    end = end_row.date if end_row else (cutoff or date.today())

    fetch_from = min(start, end - timedelta(days=366))

    # Legacy price table (Yahoo-sourced assets)
    prices_df = get_prices_df(db, fetch_from, end)
    if not prices_df.empty:
        prices_df = prices_df.ffill()

    actual_weights = get_actual_weights(db, end)
    portfolio_inception = start

    today = end
    month_start = today.replace(day=1)
    calendar_year_start = today.replace(month=1, day=1)
    ytd_ref = (
        portfolio_inception
        if portfolio_inception >= calendar_year_start - timedelta(days=93)
        else calendar_year_start
    )
    ago_12m = today - timedelta(days=365)

    def _ret(p_end, p_beg):
        if p_end and p_beg and p_beg != 0:
            return (p_end / p_beg) - 1.0
        return None

    # ── Build asset list: V2 AssetSnapshot → Allocation fallback ─────────────
    # Each entry: (Asset ORM object, asset_start_date)
    asset_list: list[tuple] = []

    try:
        from app.models import (
            Portfolio as _PortV2,
            AssetSnapshot as _AsSnap,
            DailySnapshot as _DaySnap,
            Transaction as _Tx,
        )
        port_v2 = db.query(_PortV2).first()
        if port_v2 is not None:
            day_q = db.query(_DaySnap).filter_by(portfolio_id=port_v2.id)
            if cutoff:
                day_q = day_q.filter(_DaySnap.date <= cutoff)
            day = day_q.order_by(_DaySnap.date.desc()).first()
            if day:
                snaps = (
                    db.query(_AsSnap)
                      .filter_by(portfolio_id=port_v2.id, date=day.date)
                      .all()
                )
                seen: set[int] = set()
                for s in snaps:
                    if (s.asset
                            and s.quantity > 1e-9
                            and s.asset.data_source != "cash_account"
                            and s.asset_id not in seen):
                        seen.add(s.asset_id)
                        # First buy date = when asset entered the portfolio
                        first_tx = (
                            db.query(_Tx)
                              .filter_by(portfolio_id=port_v2.id,
                                         asset_id=s.asset_id,
                                         type="compra",
                                         is_void=False)
                              .order_by(_Tx.date)
                              .first()
                        )
                        asset_list.append((
                            s.asset,
                            first_tx.date if first_tx else start,
                        ))
    except Exception:
        pass

    if not asset_list:
        # Legacy fallback: iterate active Allocations
        active = get_active_allocations(db, end)
        for alloc in active:
            first_alloc = (
                db.query(Allocation)
                .filter_by(asset_id=alloc.asset_id)
                .order_by(Allocation.start_date)
                .first()
            )
            asset_list.append((
                alloc.asset,
                first_alloc.start_date if first_alloc else start,
            ))

    # ── Pass 1: collect price series for all assets ───────────────────────────
    asset_data: list[tuple] = []  # (asset, asset_start, col_or_None)
    for asset, asset_start in asset_list:
        ticker = asset.ticker

        # Price series: legacy Price table first
        col: Optional[pd.Series] = None
        if not prices_df.empty and ticker in prices_df.columns:
            col = prices_df[ticker].dropna()
            if col.empty:
                col = None

        # Fallback: AssetPrice table (v2 — manual / fund assets)
        if col is None or col.empty:
            try:
                from app.models import AssetPrice as _AP
                ap_rows = (
                    db.query(_AP)
                      .filter(
                          _AP.asset_id == asset.id,
                          _AP.date >= fetch_from,
                          _AP.date <= end,
                      )
                      .order_by(_AP.date)
                      .all()
                )
                if ap_rows:
                    col = pd.Series(
                        {r.date: r.price for r in ap_rows}
                    ).ffill()
            except Exception:
                col = None

        asset_data.append((asset, asset_start, col))

    # ── Build beta series by bucket ───────────────────────────────────────────
    # For each bucket that has a beta asset with price data, keep its series
    # so alpha assets can compute their differential return.
    beta_by_bucket: dict[str, pd.Series] = {}
    for asset, _start, col in asset_data:
        role   = (asset.role or "beta").lower()
        bucket = asset.bucket or "—"
        if role == "beta" and col is not None and not col.empty:
            beta_by_bucket[bucket] = col

    # ── Pass 2: compute returns + Geração de Valor ────────────────────────────
    rows = []
    for asset, asset_start, col in asset_data:
        ticker = asset.ticker
        cw     = actual_weights.get(ticker, 0.0)
        role   = (asset.role or "beta").lower()
        bucket = asset.bucket or "—"

        if col is None or col.empty:
            rows.append({
                "name":           asset.name,
                "ticker":         ticker,
                "asset_class":    asset.asset_class,
                "role":           role,
                "bucket":         bucket,
                "target_weight":  cw,
                "current_weight": cw,
                "start_date":     asset_start,
                "daily_return":   None,
                "mtd_return":     None,
                "ytd_return":     None,
                "ret_12m":        None,
                "total_return":   None,
                "value_add":      None,
            })
            continue

        p_today     = _nearest_price(col, today)
        p_yesterday = _nearest_price(col, today - timedelta(days=1))
        p_month     = _nearest_price_after(col, month_start)
        p_year      = _nearest_price_after(col, ytd_ref)
        p_12m       = _nearest_price_after(col, ago_12m)
        # Inception return uses the asset's own entry date, not portfolio start
        p_inception = _nearest_price_after(col, asset_start)
        total_ret   = _ret(p_today, p_inception)

        # ── Geração de Valor: only for alpha assets ───────────────────────
        # = alpha_total_return − beta_return_since_alpha_start_date
        value_add: Optional[float] = None
        if role == "alpha":
            beta_col = beta_by_bucket.get(bucket)
            if beta_col is not None and total_ret is not None:
                p_beta_at_start = _nearest_price_after(beta_col, asset_start)
                p_beta_today    = _nearest_price(beta_col, today)
                beta_ret        = _ret(p_beta_today, p_beta_at_start)
                if beta_ret is not None:
                    value_add = (1 + total_ret) / (1 + beta_ret) - 1

        rows.append({
            "name":           asset.name,
            "ticker":         ticker,
            "asset_class":    asset.asset_class,
            "role":           role,
            "bucket":         bucket,
            "target_weight":  cw,
            "current_weight": cw,
            "start_date":     asset_start,
            "daily_return":   _ret(p_today, p_yesterday),
            "mtd_return":     _ret(p_today, p_month),
            "ytd_return":     _ret(p_today, p_year),
            "ret_12m":        _ret(p_today, p_12m),
            "total_return":   total_ret,
            "value_add":      value_add,
        })

    return pd.DataFrame(rows)


def weekly_return(db: Session, cutoff: Optional[date] = None) -> Optional[float]:
    """Retorno dos últimos 7 dias corridos (âncora = último snapshot disponível)."""
    # Use the latest available snapshot as anchor (not date.today()) so weekends/holidays work
    latest_q = db.query(PortfolioSnapshot).order_by(PortfolioSnapshot.date.desc())
    if cutoff:
        latest_q = latest_q.filter(PortfolioSnapshot.date <= cutoff)
    latest = latest_q.first()
    if latest is None:
        return None
    anchor = latest.date
    week_ago = anchor - timedelta(days=7)
    q = (
        db.query(PortfolioSnapshot)
        .filter(PortfolioSnapshot.date >= week_ago, PortfolioSnapshot.date <= anchor)
        .order_by(PortfolioSnapshot.date)
    )
    rows = q.all()
    if len(rows) < 2:
        return None
    return (rows[-1].index_value / rows[0].index_value) - 1.0


def mtd_return(db: Session, cutoff: Optional[date] = None) -> Optional[float]:
    today = cutoff or date.today()
    month_start = today.replace(day=1)
    q = (
        db.query(PortfolioSnapshot)
        .filter(PortfolioSnapshot.date >= month_start)
        .order_by(PortfolioSnapshot.date)
    )
    if cutoff:
        q = q.filter(PortfolioSnapshot.date <= cutoff)
    rows = q.all()
    if len(rows) < 2:
        return None
    return (rows[-1].index_value / rows[0].index_value) - 1.0


def ytd_return(db: Session, cutoff: Optional[date] = None) -> Optional[float]:
    today = cutoff or date.today()
    # Derive inception from the first snapshot in this portfolio's DB
    first_snap = db.query(PortfolioSnapshot).order_by(PortfolioSnapshot.date).first()
    if first_snap is None:
        return None
    portfolio_inception = first_snap.date
    calendar_year_start = today.replace(month=1, day=1)
    ytd_ref = (
        portfolio_inception
        if portfolio_inception >= calendar_year_start - timedelta(days=93)
        else calendar_year_start
    )
    # If YTD start == inception, reuse total_return to avoid floating point divergence
    if ytd_ref <= portfolio_inception:
        return total_return(db, cutoff)
    q = (
        db.query(PortfolioSnapshot)
        .filter(PortfolioSnapshot.date >= ytd_ref)
        .order_by(PortfolioSnapshot.date)
    )
    if cutoff:
        q = q.filter(PortfolioSnapshot.date <= cutoff)
    rows = q.all()
    if len(rows) < 2:
        return None
    return (rows[-1].index_value / rows[0].index_value) - 1.0


def allocation_history(db: Session) -> pd.DataFrame:
    """Return allocation event log as a DataFrame for the history view."""
    from app.models import AllocationLog
    rows = (
        db.query(AllocationLog)
        .join(Asset)
        .order_by(AllocationLog.effective_date.desc(), AllocationLog.id.desc())
        .all()
    )
    if not rows:
        return pd.DataFrame()
    records = []
    for r in rows:
        records.append({
            "Data":        str(r.effective_date),
            "Ativo":       r.asset.name,
            "Ticker":      r.asset.ticker,
            "Ação":        r.action,
            "Peso Antigo": f"{r.old_weight*100:.1f}%" if r.old_weight is not None else "—",
            "Peso Novo":   f"{r.new_weight*100:.1f}%" if r.new_weight is not None else "—",
            "Obs":         r.notes or "",
        })
    return pd.DataFrame(records)
