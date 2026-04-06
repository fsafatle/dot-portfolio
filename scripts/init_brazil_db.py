"""
Initialize the Brazil portfolio database.

Run once:
    python3 -m scripts.init_brazil_db
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import logging
from datetime import date

from app.config import PORTFOLIOS
from app.database import Base, get_db_for
from app.models import Asset, Allocation, AllocationLog
from app.portfolio.engine import refresh_prices, build_snapshots

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def init_brazil() -> None:
    cfg = PORTFOLIOS["brazil"]
    start = date.fromisoformat(cfg["start_date"])

    logger.info("Initializing Brazil portfolio DB: %s", cfg["db_path"])

    db = get_db_for("brazil")
    try:
        for entry in cfg["initial_portfolio"]:
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
            db.flush()

            db.add(Allocation(
                asset_id=asset.id,
                weight=entry["weight"],
                start_date=start,
                end_date=None,
                notes="Initial Brazil portfolio — 2025-12-31",
            ))
            db.add(AllocationLog(
                asset_id=asset.id,
                action="add",
                old_weight=None,
                new_weight=entry["weight"],
                effective_date=start,
                notes="Initial Brazil portfolio setup",
            ))
            logger.info("Seeded %s @ %.0f%%", entry["ticker"], entry["weight"] * 100)

        db.commit()
        logger.info("Seed complete.")

        logger.info("Fetching prices...")
        refresh_prices(db, start=start)

        logger.info("Building snapshots...")
        build_snapshots(
            db,
            start_date_str=cfg["start_date"],
            base_value=cfg["base_value"],
        )

        logger.info("Brazil portfolio ready.")
    finally:
        db.close()


if __name__ == "__main__":
    init_brazil()
