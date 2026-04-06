"""
Create all database tables and seed the initial portfolio.

Run once:
    python -m scripts.init_db
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import logging
from datetime import date

from app.database import Base, engine, get_db
from app.models import Asset, Allocation, AllocationLog
from app.config import INITIAL_PORTFOLIO, PORTFOLIO_START_DATE

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def create_tables() -> None:
    logger.info("Creating database tables...")
    Base.metadata.create_all(bind=engine)
    logger.info("Tables created.")


def seed_portfolio(db) -> None:
    start = date.fromisoformat(PORTFOLIO_START_DATE)

    for entry in INITIAL_PORTFOLIO:
        existing = db.query(Asset).filter_by(ticker=entry["ticker"]).first()
        if existing:
            logger.info("Asset %s already exists, skipping.", entry["ticker"])
            continue

        asset = Asset(
            name=entry["name"],
            ticker=entry["ticker"],
            asset_class=entry["asset_class"],
            data_source=entry["data_source"],
            bucket=entry.get("bucket"),
            role=entry.get("role", "beta"),
        )
        db.add(asset)
        db.flush()  # get asset.id

        alloc = Allocation(
            asset_id=asset.id,
            weight=entry["weight"],
            start_date=start,
            end_date=None,
            notes="Initial portfolio — 2026-01-02",
        )
        db.add(alloc)

        log = AllocationLog(
            asset_id=asset.id,
            action="add",
            old_weight=None,
            new_weight=entry["weight"],
            effective_date=start,
            notes="Initial portfolio setup",
        )
        db.add(log)
        logger.info("Seeded %s @ %.0f%%", entry["ticker"], entry["weight"] * 100)

    db.commit()
    logger.info("Portfolio seed complete.")


if __name__ == "__main__":
    create_tables()
    db = get_db()
    try:
        seed_portfolio(db)
    finally:
        db.close()
    logger.info("Done. Run 'python -m scripts.fetch_prices' to load market data.")
