"""Core portfolio engine: price updates, allocation management, snapshots."""

import logging
from datetime import date, timedelta
from typing import Optional

import pandas as pd
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.config import PORTFOLIO_BASE_VALUE, PORTFOLIO_START_DATE
from app.models import (
    Allocation, AllocationLog, Asset, PortfolioSnapshot, Price,
)
from app.market_data.yahoo_provider import YahooProvider

logger = logging.getLogger(__name__)
_provider = YahooProvider()


# ---------------------------------------------------------------------------
# Allocation helpers
# ---------------------------------------------------------------------------

def get_active_allocations(db: Session, as_of: date) -> list[Allocation]:
    """Return allocations active on a given date (start_date ≤ as_of < end_date)."""
    return (
        db.query(Allocation)
        .join(Asset)
        .filter(
            Allocation.start_date <= as_of,
            (Allocation.end_date.is_(None)) | (Allocation.end_date > as_of),
            Asset.is_active.is_(True),
        )
        .all()
    )


def update_allocations(
    db: Session,
    new_weights: dict[str, float],   # {ticker: weight}
    effective_date: date,
    notes: str = "",
) -> None:
    """
    Register a new allocation version.

    - Closes (sets end_date on) every currently-active allocation.
    - Creates new Allocation rows starting from effective_date.
    - Writes an AllocationLog entry for every change.

    new_weights values should sum to 1.0 (validated here with a warning).
    """
    total = sum(new_weights.values())
    if abs(total - 1.0) > 0.001:
        logger.warning("Weights sum to %.4f, not 1.0", total)

    # Close ALL active allocations (including any duplicates from prior bugs)
    active = get_active_allocations(db, effective_date)

    # Build a per-ticker summary using the first (oldest) row as the weight reference
    active_by_ticker: dict[str, Allocation] = {}
    for a in active:
        if a.asset.ticker not in active_by_ticker:
            active_by_ticker[a.asset.ticker] = a
        # Close every duplicate row unconditionally
        a.end_date = effective_date

    for ticker, alloc in active_by_ticker.items():
        old_w = alloc.weight
        new_w = new_weights.get(ticker)
        if new_w is None:
            # Asset removed from portfolio
            db.add(AllocationLog(
                asset_id=alloc.asset_id,
                action="remove",
                old_weight=old_w,
                new_weight=None,
                effective_date=effective_date,
                notes=notes,
            ))
        elif abs(new_w - old_w) > 1e-6:
            db.add(AllocationLog(
                asset_id=alloc.asset_id,
                action="update",
                old_weight=old_w,
                new_weight=new_w,
                effective_date=effective_date,
                notes=notes,
            ))
        # If weight unchanged: closed silently, will be reopened below

    # Create new allocation rows
    for ticker, weight in new_weights.items():
        asset = db.query(Asset).filter_by(ticker=ticker).first()
        if asset is None:
            logger.error("Ticker %s not found in assets table", ticker)
            continue
        db.add(Allocation(
            asset_id=asset.id,
            weight=weight,
            start_date=effective_date,
            end_date=None,
            notes=notes,
        ))
        if ticker not in active_by_ticker:
            db.add(AllocationLog(
                asset_id=asset.id,
                action="add",
                old_weight=None,
                new_weight=weight,
                effective_date=effective_date,
                notes=notes,
            ))

    db.commit()
    logger.info("Allocations updated effective %s", effective_date)


# ---------------------------------------------------------------------------
# Price management
# ---------------------------------------------------------------------------

def _reset_prices_sequence(db: Session) -> None:
    """
    Resync the auto-increment sequence for prices.id with the actual MAX(id).

    Necessary when the table was populated via bulk INSERT with explicit IDs
    (e.g. migrations), which leaves the sequence below the real max ID.
    Without this, subsequent INSERTs generate IDs that already exist → PK error.
    """
    try:
        from sqlalchemy import text
        db.execute(text(
            "SELECT setval("
            "  pg_get_serial_sequence('prices', 'id'),"
            "  COALESCE((SELECT MAX(id) FROM prices), 0) + 1,"
            "  false"
            ")"
        ))
        db.flush()
    except Exception as exc:
        logger.warning("Could not reset prices sequence (non-PostgreSQL?): %s", exc)


def refresh_prices(
    db: Session,
    start: Optional[date] = None,
    end: Optional[date] = None,
) -> None:
    """
    Fetch and cache prices for all active assets.

    start defaults to the portfolio start date.
    end   defaults to today.
    """
    if start is None:
        start = date.fromisoformat(PORTFOLIO_START_DATE)
    if end is None:
        end = date.today()

    # Ensure the sequence is in sync before any INSERT (fixes desync caused by
    # bulk migrations that inserted rows with explicit IDs).
    _reset_prices_sequence(db)

    assets = db.query(Asset).filter_by(is_active=True).all()
    for asset in assets:
        logger.info("Fetching prices for %s (%s)", asset.ticker, asset.name)
        series = _provider.fetch_prices(
            asset.ticker, start, end, data_source=asset.data_source
        )
        _upsert_prices(db, asset.id, series, asset.data_source)

    db.commit()
    logger.info("Price refresh complete")


def _upsert_prices(
    db: Session, asset_id: int, series: pd.Series, source: str
) -> None:
    """
    Upsert price rows via DELETE-range + INSERT.

    The sequence is reset by the caller (refresh_prices) before the first
    call to this function, so INSERT never conflicts with existing rows.
    """
    # Deduplicate & normalize to datetime.date (Yahoo returns pd.Timestamp)
    series = series[~series.index.duplicated(keep="last")]
    clean = {
        (dt.date() if hasattr(dt, "date") and callable(dt.date) else dt): float(p)
        for dt, p in series.items() if not pd.isna(p)
    }
    if not clean:
        return

    min_date = min(clean)
    max_date = max(clean)

    db.query(Price).filter(
        Price.asset_id == asset_id,
        Price.date >= min_date,
        Price.date <= max_date,
    ).delete(synchronize_session="fetch")
    db.flush()

    for dt, price in clean.items():
        db.add(Price(asset_id=asset_id, date=dt, close_price=float(price), source=source))
    db.flush()


def upsert_manual_price(db: Session, asset_id: int, price_date: date, price: float) -> None:
    """Insert or overwrite a manually-entered price. Always overwrites (no cutoff)."""
    existing = db.query(Price).filter_by(asset_id=asset_id, date=price_date).first()
    if existing:
        existing.close_price = float(price)
        existing.source = "manual"
    else:
        db.add(Price(asset_id=asset_id, date=price_date, close_price=float(price), source="manual"))
    db.commit()


def get_price(db: Session, asset_id: int, on_date: date) -> Optional[float]:
    row = db.query(Price).filter_by(asset_id=asset_id, date=on_date).first()
    return row.close_price if row else None


def get_prices_df(db: Session, start: date, end: date) -> pd.DataFrame:
    """Return a wide DataFrame[date × ticker] of cached close prices."""
    assets = db.query(Asset).filter_by(is_active=True).all()
    frames = {}
    for asset in assets:
        rows = (
            db.query(Price)
            .filter(
                Price.asset_id == asset.id,
                Price.date >= start,
                Price.date <= end,
            )
            .order_by(Price.date)
            .all()
        )
        if rows:
            frames[asset.ticker] = pd.Series(
                {r.date: r.close_price for r in rows}
            )
    if not frames:
        return pd.DataFrame()
    return pd.DataFrame(frames)


# ---------------------------------------------------------------------------
# Snapshot / portfolio index  (buy-and-hold with drift)
# ---------------------------------------------------------------------------

def _has_rebalance(db: Session, prev_date: date, curr_date: date) -> bool:
    """Return True if any allocation starts strictly between prev_date and curr_date."""
    return (
        db.query(Allocation)
        .filter(Allocation.start_date > prev_date, Allocation.start_date <= curr_date)
        .count()
        > 0
    )


def _compute_units(
    db: Session, as_of: date, portfolio_value: float, prices_df: pd.DataFrame
) -> dict[int, float]:
    """
    Derive the units held as of `as_of` by replaying from the most recent
    rebalance date.  Called when resuming an incremental snapshot build.
    """
    last_rebalance = (
        db.query(func.max(Allocation.start_date))
        .filter(Allocation.start_date <= as_of)
        .scalar()
    )
    if last_rebalance is None:
        return {}

    # Use the first snapshot on or after the rebalance date as the anchor
    snap = (
        db.query(PortfolioSnapshot)
        .filter(PortfolioSnapshot.date >= last_rebalance)
        .order_by(PortfolioSnapshot.date)
        .first()
    )
    if snap is None:
        return {}

    anchor_date = snap.date
    anchor_value = snap.index_value
    allocations = get_active_allocations(db, anchor_date)

    units: dict[int, float] = {}
    for alloc in allocations:
        ticker = alloc.asset.ticker
        if ticker not in prices_df.columns or anchor_date not in prices_df.index:
            continue
        price = prices_df.loc[anchor_date, ticker]
        if pd.isna(price) or price == 0:
            continue
        units[alloc.asset_id] = (alloc.weight * anchor_value) / price
    return units


def build_snapshots(
    db: Session,
    force_rebuild: bool = False,
    start_date_str: Optional[str] = None,
    base_value: Optional[float] = None,
) -> None:
    """
    Build PortfolioSnapshot rows using a buy-and-hold (drifting weights) model.

    Incremental by default: only adds snapshots for dates not yet stored.
    Use force_rebuild=True to wipe and recalculate everything from scratch
    (e.g. after a price correction or a past rebalance was edited).

    Logic:
      - On the first trading day, buy each asset proportionally to its target weight.
      - Each subsequent day: portfolio_value = Σ units[asset] × price(asset, t)
      - When a rebalance is detected, recalculate units after that day's return.
    """
    start = date.fromisoformat(start_date_str or PORTFOLIO_START_DATE)
    pv_base = base_value if base_value is not None else PORTFOLIO_BASE_VALUE
    today = date.today()

    prices_df = get_prices_df(db, start, today)
    if prices_df.empty:
        logger.warning("No prices cached — run refresh_prices first")
        return

    prices_df = prices_df.ffill()

    # Normalize index to datetime.date — pandas converts Date values to pd.Timestamp
    # which psycopg2 cannot write to a PostgreSQL DATE column (causes ProgrammingError)
    def _to_date(d):
        return d.date() if hasattr(d, "date") and callable(d.date) else d

    prices_df.index = pd.Index([_to_date(d) for d in prices_df.index])
    trading_dates = sorted(prices_df.index)

    if force_rebuild:
        db.query(PortfolioSnapshot).delete(synchronize_session="fetch")
        db.flush()

    # ── Determine starting point ───────────────────────────────────────────
    last_snap = (
        db.query(PortfolioSnapshot)
        .order_by(PortfolioSnapshot.date.desc())
        .first()
    )

    if last_snap is None:
        # No history yet — start fresh
        portfolio_value = pv_base
        units: dict[int, float] = {}
        initialized = False
        dates_to_process = trading_dates
    else:
        # Resume from last snapshot
        portfolio_value = last_snap.index_value
        units = _compute_units(db, last_snap.date, portfolio_value, prices_df)
        initialized = True
        cutoff = last_snap.date
        dates_to_process = [d for d in trading_dates if d > cutoff]

    if not dates_to_process:
        logger.info("Snapshots already up to date")
        return

    # Defensively remove any snapshots that might exist for dates we're about to write
    db.query(PortfolioSnapshot).filter(
        PortfolioSnapshot.date >= min(dates_to_process)
    ).delete(synchronize_session="fetch")
    db.flush()

    # ── Main loop ─────────────────────────────────────────────────────────
    all_dates = trading_dates  # needed to find prev_date
    for i_global, current_date in enumerate(trading_dates):
        if current_date not in dates_to_process:
            continue

        allocations = get_active_allocations(db, current_date)
        if not allocations:
            continue

        # First-ever snapshot
        if not initialized:
            for alloc in allocations:
                ticker = alloc.asset.ticker
                if ticker not in prices_df.columns:
                    continue
                price = prices_df.loc[current_date, ticker]
                if pd.isna(price) or price == 0:
                    continue
                units[alloc.asset_id] = (alloc.weight * portfolio_value) / float(price)

            db.add(PortfolioSnapshot(
                date=current_date,
                index_value=float(portfolio_value),
                daily_return=None,
            ))
            initialized = True
            continue

        # Mark-to-market
        new_value = 0.0
        missing = False
        for asset_id, qty in units.items():
            asset = db.query(Asset).filter_by(id=asset_id).first()
            if asset is None or asset.ticker not in prices_df.columns:
                missing = True
                break
            price = prices_df.loc[current_date, asset.ticker] if current_date in prices_df.index else None
            if price is None or pd.isna(price):
                missing = True
                break
            new_value += qty * float(price)

        if missing or new_value == 0:
            continue

        daily_ret = (new_value / portfolio_value) - 1.0
        portfolio_value = new_value

        db.add(PortfolioSnapshot(
            date=current_date,
            index_value=float(portfolio_value),
            daily_return=float(daily_ret),
        ))

        # Rebalance (after calculating return — value is unchanged by rebalance)
        prev_date = all_dates[i_global - 1] if i_global > 0 else current_date
        if _has_rebalance(db, prev_date, current_date):
            logger.info("Rebalancing on %s", current_date)
            units = {}
            for alloc in allocations:
                ticker = alloc.asset.ticker
                if ticker not in prices_df.columns:
                    continue
                price = prices_df.loc[current_date, ticker]
                if pd.isna(price) or price == 0:
                    continue
                units[alloc.asset_id] = (alloc.weight * portfolio_value) / float(price)

    db.commit()
    logger.info("Snapshots built up to %s", today)


def get_actual_weights(db: Session, as_of: date) -> dict[str, float]:
    """
    Retorna pesos reais de cada ativo na data `as_of`.

    Prioridade:
      1. Motor v2 (AssetSnapshot + DailySnapshot) — usa market_value / nav se disponível.
      2. Motor legado — recalcula via unidades × preço × alocação.
    """
    # ── Motor v2: lê diretamente do AssetSnapshot ─────────────────────────
    try:
        from app.models import (
            Portfolio as PortfolioV2,
            AssetSnapshot as AsSnap,
            DailySnapshot as DaySnap,
        )
        port_v2 = db.query(PortfolioV2).first()
        if port_v2 is not None:
            # NAV mais recente até as_of
            day = (
                db.query(DaySnap)
                  .filter(DaySnap.portfolio_id == port_v2.id,
                          DaySnap.date <= as_of)
                  .order_by(DaySnap.date.desc())
                  .first()
            )
            if day is not None and day.nav and day.nav > 0:
                snaps = (
                    db.query(AsSnap)
                      .filter_by(portfolio_id=port_v2.id, date=day.date)
                      .all()
                )
                result: dict[str, float] = {}
                for s in snaps:
                    if s.asset and s.market_value and s.market_value > 0:
                        result[s.asset.ticker] = s.market_value / day.nav
                if result:
                    return result
    except Exception:
        pass  # fallback para motor legado

    # ── Motor legado ──────────────────────────────────────────────────────
    last_rebalance = (
        db.query(func.max(Allocation.start_date))
        .filter(Allocation.start_date <= as_of)
        .scalar()
    )
    if last_rebalance is None:
        return {}

    snap = (
        db.query(PortfolioSnapshot)
        .filter(PortfolioSnapshot.date >= last_rebalance)
        .order_by(PortfolioSnapshot.date)
        .first()
    )
    if snap is None:
        return {}

    rebalance_snap_date = snap.date
    value_at_rebalance = snap.index_value

    allocations = get_active_allocations(db, rebalance_snap_date)

    units: dict[int, float] = {}
    for alloc in allocations:
        price_row = (
            db.query(Price)
            .filter(Price.asset_id == alloc.asset_id, Price.date <= rebalance_snap_date)
            .order_by(Price.date.desc())
            .first()
        )
        if price_row and price_row.close_price:
            units[alloc.asset_id] = (alloc.weight * value_at_rebalance) / price_row.close_price

    asset_values: dict[int, float] = {}
    for asset_id, qty in units.items():
        price_row = (
            db.query(Price)
            .filter(Price.asset_id == asset_id, Price.date <= as_of)
            .order_by(Price.date.desc())
            .first()
        )
        if price_row and price_row.close_price:
            asset_values[asset_id] = qty * price_row.close_price

    total = sum(asset_values.values())
    if total == 0:
        return {}

    result_leg: dict[str, float] = {}
    for asset_id, value in asset_values.items():
        asset = db.query(Asset).filter_by(id=asset_id).first()
        if asset:
            result_leg[asset.ticker] = value / total
    return result_leg
