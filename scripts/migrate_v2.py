"""
Migração v2: adiciona tabelas e dados do novo motor ao banco existente.

O que faz (idempotente — pode rodar múltiplas vezes):
  1. CREATE TABLE IF NOT EXISTS para todas as novas tabelas
  2. ALTER TABLE assets ADD COLUMN portfolio_id (SQLite)
  3. Cria registro em `portfolios` para cada portfólio do config
  4. Vincula assets ao portfolio (UPDATE assets SET portfolio_id = ?)
  5. Copia prices → asset_prices (migração de preços legados)
  6. NÃO toca em transactions / positions / snapshots (tabelas vazias no início)

Executar:
    python scripts/migrate_v2.py
"""

import sys
from pathlib import Path

# Garante que o projeto está no path
sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import date

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker

from app.config import PORTFOLIOS, DATA_DIR
from app.database import Base
from app.models import Portfolio, Asset   # força registro no Base.metadata
# Importar todos os outros modelos novos para o Base.metadata os reconhecer
from app.models import (  # noqa: F401
    Transaction, Position, DailySnapshot, AssetSnapshot,
    AssetPrice, EngineAuditLog,
)
from app.services.prices import migrate_legacy_prices


def migrate_portfolio(key: str, cfg: dict) -> None:
    db_path = str(DATA_DIR / cfg["db_path"])
    engine  = create_engine(
        f"sqlite:///{db_path}",
        connect_args={"check_same_thread": False},
    )

    print(f"\n{'='*60}")
    print(f"  Migrando: {cfg['name']} ({db_path})")
    print(f"{'='*60}")

    # ── 1. Criar novas tabelas ────────────────────────────────────────────
    Base.metadata.create_all(bind=engine)
    print("  ✓ Tabelas criadas (CREATE TABLE IF NOT EXISTS)")

    Session = sessionmaker(bind=engine)
    db      = Session()

    try:
        # ── 2. Adicionar portfolio_id em assets (SQLite não tem IF NOT EXISTS) ──
        insp = inspect(engine)
        col_names = [c["name"] for c in insp.get_columns("assets")]
        if "portfolio_id" not in col_names:
            with engine.connect() as conn:
                conn.execute(text(
                    "ALTER TABLE assets ADD COLUMN portfolio_id INTEGER"
                ))
                conn.commit()
            print("  ✓ Coluna portfolio_id adicionada em assets")
        else:
            print("  · portfolio_id já existe em assets")

        # ── 3. Criar registro em portfolios ──────────────────────────────
        existing = db.query(Portfolio).filter_by(key=key).first()
        if existing is None:
            base_date    = date.fromisoformat(cfg["start_date"])
            initial_cash = float(cfg.get("initial_cash", 0.0))
            cota_base    = float(cfg.get("cota_base", 1000.0))

            portfolio = Portfolio(
                key          = key,
                name         = cfg["name"],
                currency     = cfg.get("currency", "USD"),
                base_date    = base_date,
                initial_cash = initial_cash,
                cota_base    = cota_base,
            )
            db.add(portfolio)
            db.flush()
            print(f"  ✓ Portfolio criado: id={portfolio.id}, "
                  f"base_date={base_date}, initial_cash={initial_cash}")
        else:
            portfolio = existing
            print(f"  · Portfolio já existe: id={portfolio.id}")

        # ── 4. Vincular assets ao portfolio ──────────────────────────────
        unlinked = db.query(Asset).filter(Asset.portfolio_id == None).all()
        if unlinked:
            for a in unlinked:
                a.portfolio_id = portfolio.id
            db.flush()
            print(f"  ✓ {len(unlinked)} ativos vinculados ao portfólio")
        else:
            print("  · Todos os ativos já estão vinculados")

        # ── 5. Migrar preços legados → asset_prices ───────────────────────
        copied = migrate_legacy_prices(db)
        print(f"  ✓ {copied} preços migrados de prices → asset_prices")

        db.commit()
        print(f"  ✓ Migração concluída com sucesso")

    except Exception as exc:
        db.rollback()
        print(f"  ✗ ERRO: {exc}")
        raise
    finally:
        db.close()


def main():
    print("\nMIGRAÇÃO V2 — Motor de portfólio financeiramente correto")
    print("=" * 60)

    for key, cfg in PORTFOLIOS.items():
        migrate_portfolio(key, cfg)

    print("\n" + "=" * 60)
    print("  MIGRAÇÃO COMPLETA")
    print("  Próximo passo: registre os movimentos via Cash Flow")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
