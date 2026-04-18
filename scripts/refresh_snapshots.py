"""
Atualiza preços e reconstrói snapshots para Global e Brazil.

Executado automaticamente pelo GitHub Actions todo dia útil antes do envio do Slack.
Pode também ser rodado manualmente:
    python scripts/refresh_snapshots.py
"""

import sys
import logging
from datetime import date
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

from app.database import get_db_for
from app.config import PORTFOLIOS
from app.portfolio import engine
from app.models import Portfolio as PortfolioModel, Transaction


def refresh_portfolio(key: str):
    cfg = PORTFOLIOS[key]
    start_date = cfg["start_date"]
    base_value = cfg.get("base_value", 1.0)

    logger.info(f"=== Atualizando {cfg['name']} ===")
    db = get_db_for(key)
    try:
        # Verifica se usa motor v2 (tem transações)
        port_v2 = db.query(PortfolioModel).filter_by(key=key).first()
        has_txs = (
            port_v2 is not None
            and db.query(Transaction)
                 .filter_by(portfolio_id=port_v2.id, is_void=False)
                 .count() > 0
        )

        if has_txs:
            logger.info("Motor v2 detectado — usando reprocessor")
            from app.services.prices import refresh_prices_for_portfolio as _refresh_v2
            from app.engine.reprocessor import recompute_from as _recompute_v2

            engine.refresh_prices(db, start=date.fromisoformat(start_date))
            db.commit()
            logger.info("  Preços v1 atualizados")

            _refresh_v2(db, port_v2.id, start=date.fromisoformat(start_date))
            db.commit()
            logger.info("  Preços v2 atualizados")

            _recompute_v2(db, port_v2.id, port_v2.base_date)
            db.commit()
            logger.info("  Snapshots reconstruídos (v2)")

        else:
            logger.info("Motor legado — usando build_snapshots")
            engine.refresh_prices(db, start=date.fromisoformat(start_date))
            engine.build_snapshots(db, start_date_str=start_date, base_value=base_value)
            logger.info("  Snapshots reconstruídos (legado)")

    except Exception as e:
        import traceback
        logger.error(f"  ERRO ao atualizar {key}: {e}")
        logger.error(traceback.format_exc())
        raise
    finally:
        db.close()

    logger.info(f"  {cfg['name']} atualizado com sucesso")


if __name__ == "__main__":
    errors = []
    for key in ("global", "brazil"):
        try:
            refresh_portfolio(key)
        except Exception as e:
            errors.append(f"{key}: {e}")

    if errors:
        logger.error("Falhas durante o refresh:")
        for err in errors:
            logger.error(f"  {err}")
        sys.exit(1)

    logger.info("=== Refresh concluído com sucesso ===")
