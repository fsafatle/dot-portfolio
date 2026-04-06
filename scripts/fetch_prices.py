"""
Fetch / refresh market prices and rebuild portfolio snapshots.

Run daily (or on demand):
    python -m scripts.fetch_prices
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import logging
from app.database import get_db
from app.portfolio.engine import refresh_prices, build_snapshots

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    db = get_db()
    try:
        logger.info("Refreshing prices...")
        refresh_prices(db)
        logger.info("Building portfolio snapshots...")
        build_snapshots(db)
        logger.info("Done.")
    finally:
        db.close()
