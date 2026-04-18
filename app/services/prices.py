"""
Serviço de preços para o motor v2.

Usa a tabela asset_prices (não a tabela prices legada).
Suporta:
  - Busca de preço de mercado via Yahoo Finance
  - Inserção/atualização manual
  - Fallback para último preço disponível
  - Sincronização da tabela legada (prices → asset_prices) para migração
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Optional

import pandas as pd
from sqlalchemy.orm import Session

from app.models import Asset, AssetPrice, Price
from app.market_data.yahoo_provider import YahooProvider

logger = logging.getLogger(__name__)
_provider = YahooProvider()


# ── Upsert de preço ──────────────────────────────────────────────────────────

def upsert_price(
    db: Session,
    asset_id: int,
    price_date: date,
    price: float,
    source: str = "market",
) -> AssetPrice:
    """Insere ou atualiza preço em asset_prices."""
    existing = db.query(AssetPrice).filter_by(
        asset_id=asset_id, date=price_date
    ).first()

    if existing:
        existing.price  = price
        existing.source = source
        return existing

    ap = AssetPrice(asset_id=asset_id, date=price_date,
                    price=price, source=source)
    db.add(ap)
    db.flush()
    return ap


def upsert_manual_price(
    db: Session,
    asset_id: int,
    price_date: date,
    price: float,
) -> AssetPrice:
    """Preço manual — sempre sobrescreve."""
    return upsert_price(db, asset_id, price_date, price, source="manual")


# ── Busca ────────────────────────────────────────────────────────────────────

def get_price(
    db: Session,
    asset_id: int,
    on_date: date,
    fallback_last: bool = True,
) -> Optional[float]:
    """
    Retorna preço de asset_id na data. Se não existir:
      - fallback_last=True: retorna último preço disponível antes de on_date
      - fallback_last=False: retorna None
    """
    row = db.query(AssetPrice).filter_by(
        asset_id=asset_id, date=on_date
    ).first()
    if row:
        return row.price

    if fallback_last:
        last = (db.query(AssetPrice)
                  .filter(AssetPrice.asset_id == asset_id,
                          AssetPrice.date < on_date)
                  .order_by(AssetPrice.date.desc())
                  .first())
        return last.price if last else None

    return None


def get_latest_price(db: Session, asset_id: int) -> Optional[float]:
    """Último preço disponível."""
    row = (db.query(AssetPrice)
             .filter_by(asset_id=asset_id)
             .order_by(AssetPrice.date.desc())
             .first())
    return row.price if row else None


def get_latest_price_date(db: Session, asset_id: int) -> Optional[date]:
    row = (db.query(AssetPrice)
             .filter_by(asset_id=asset_id)
             .order_by(AssetPrice.date.desc())
             .first())
    return row.date if row else None


# ── Refresh de mercado ───────────────────────────────────────────────────────

def refresh_prices_for_portfolio(
    db: Session,
    portfolio_id: int,
    start: Optional[date] = None,
    end: Optional[date] = None,
) -> None:
    """Busca e armazena preços de mercado para todos os ativos do portfólio."""
    from app.models import Portfolio
    port = db.query(Portfolio).filter_by(id=portfolio_id).first()
    if port is None:
        return

    end   = end   or date.today()
    start = start or port.base_date

    assets = db.query(Asset).filter_by(
        portfolio_id=portfolio_id, is_active=True
    ).all()

    for asset in assets:
        if asset.data_source == "manual":
            logger.debug("Ativo %s é manual — pulando", asset.ticker)
            continue
        _fetch_and_store(db, asset, start, end)

    db.flush()
    logger.info("Refresh de preços concluído para portfólio=%d", portfolio_id)


def _fetch_and_store(
    db: Session, asset: Asset, start: date, end: date
) -> None:
    """Busca preços do Yahoo e armazena em asset_prices."""
    try:
        series = _provider.fetch_prices(
            asset.ticker, start, end, data_source=asset.data_source
        )
    except Exception as exc:
        logger.warning("Falha ao buscar %s: %s", asset.ticker, exc)
        return

    today  = date.today()
    cutoff = today - timedelta(days=2)

    # Deduplicate & normalize to datetime.date (Yahoo returns pd.Timestamp)
    series = series[~series.index.duplicated(keep="last")]
    clean = {
        (dt.date() if hasattr(dt, "date") and callable(dt.date) else dt): float(p)
        for dt, p in series.items() if not pd.isna(p)
    }
    if not clean:
        return

    dialect = db.bind.dialect.name

    if dialect == "postgresql":
        # Native PostgreSQL upsert — never raises IntegrityError
        from sqlalchemy.dialects.postgresql import insert as pg_insert
        stmt = pg_insert(AssetPrice.__table__).values([
            {"asset_id": asset.id, "date": dt, "price": price_val, "source": "market"}
            for dt, price_val in clean.items()
        ]).on_conflict_do_nothing(index_elements=["asset_id", "date"])
        db.execute(stmt)
        # Update recent prices with fresh values
        for dt, price_val in clean.items():
            if dt >= cutoff:
                db.query(AssetPrice).filter_by(asset_id=asset.id, date=dt).update(
                    {"price": price_val}, synchronize_session=False
                )
    else:
        # SQLite fallback (local dev)
        existing_dates = set(
            row.date for row in db.query(AssetPrice.date).filter_by(asset_id=asset.id).all()
        )
        for dt, price_val in clean.items():
            if dt in existing_dates:
                if dt >= cutoff:
                    db.query(AssetPrice).filter_by(asset_id=asset.id, date=dt).update(
                        {"price": price_val}, synchronize_session=False
                    )
            else:
                db.add(AssetPrice(asset_id=asset.id, date=dt, price=price_val, source="market"))


# ── Migração: tabela legada (prices) → asset_prices ─────────────────────────

def migrate_legacy_prices(db: Session) -> int:
    """
    Copia registros da tabela legada `prices` para `asset_prices`.
    Idempotente: usa INSERT OR IGNORE via upsert_price com check.
    Retorna número de registros copiados.
    """
    legacy = db.query(Price).order_by(Price.asset_id, Price.date).all()
    copied = 0
    for row in legacy:
        existing = db.query(AssetPrice).filter_by(
            asset_id=row.asset_id, date=row.date
        ).first()
        if existing is None:
            db.add(AssetPrice(
                asset_id=row.asset_id,
                date=row.date,
                price=row.close_price,
                source=row.source or "market",
            ))
            copied += 1
    db.flush()
    logger.info("Migração de preços: %d registros copiados", copied)
    return copied
