"""
Startup script executed by Railway before the Streamlit server starts.

1. Creates tables (idempotent).
2. Seeds initial portfolio if empty.
3. Fetches prices + builds snapshots if no snapshots exist yet.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import logging
from app.database import Base, engine, get_db
from app.models import PortfolioSnapshot

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main():
    # 1. Create tables
    logger.info("Creating tables (idempotent)...")
    Base.metadata.create_all(bind=engine)

    # 2. Seed initial portfolio
    from scripts.init_db import seed_portfolio
    db = get_db()
    try:
        seed_portfolio(db)
    finally:
        db.close()

    # 3. Fetch prices + snapshots only on first deploy (no snapshots yet)
    db = get_db()
    try:
        has_snapshots = db.query(PortfolioSnapshot).first() is not None
    finally:
        db.close()

    if not has_snapshots:
        logger.info("No snapshots found — fetching prices and building snapshots...")
        from app.portfolio.engine import refresh_prices, build_snapshots
        db = get_db()
        try:
            refresh_prices(db)
            build_snapshots(db)
        finally:
            db.close()
        logger.info("Initial data load complete.")
    else:
        logger.info("Snapshots already exist — skipping initial fetch.")


if __name__ == "__main__":
    main()
