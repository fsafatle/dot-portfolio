"""
Migração SQLite → PostgreSQL (Supabase)

Copia todos os dados dos bancos SQLite locais para os projetos Supabase.

Uso:
    export GLOBAL_DATABASE_URL="postgresql://postgres.[ref]:[password]@aws-1-us-east-1.pooler.supabase.com:6543/postgres"
    export BRAZIL_DATABASE_URL="postgresql://postgres.[ref]:[password]@aws-1-us-east-1.pooler.supabase.com:6543/postgres"
    python3 scripts/migrate_to_postgres.py
"""

import sys
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import os
from sqlalchemy import create_engine, MetaData, text
from sqlalchemy.orm import sessionmaker

# Ordem correta respeitando foreign keys
TABLE_ORDER = [
    "portfolios",
    "cash_accounts",
    "assets",
    "allocations",
    "allocation_log",
    "prices",
    "asset_prices",
    "positions",
    "transactions",
    "asset_snapshots",
    "daily_snapshots",
    "portfolio_snapshots",
    "portfolio_movements",
    "cash_transactions",
    "engine_audit_log",
]


def _engine(url: str):
    url = url.replace("postgres://", "postgresql://", 1)
    kwargs = {}
    if url.startswith("sqlite"):
        kwargs["connect_args"] = {"check_same_thread": False}
    return create_engine(url, **kwargs)


def migrate_portfolio(portfolio_key: str, sqlite_path: str, pg_url: str) -> None:
    print(f"\n{'='*60}")
    print(f"  Migrando portfolio: {portfolio_key}")
    print(f"  Origem : sqlite:///{sqlite_path}")
    print(f"  Destino: {pg_url[:50]}...")
    print(f"{'='*60}")

    src_engine = _engine(f"sqlite:///{sqlite_path}")
    dst_engine = _engine(pg_url)

    # Cria todas as tabelas no destino
    from app.models import Base
    print("\n→ Criando tabelas no PostgreSQL...")
    Base.metadata.create_all(bind=dst_engine)
    print("  ✓ Tabelas criadas")

    # Reflete schemas
    src_meta = MetaData()
    src_meta.reflect(bind=src_engine)

    dst_meta = MetaData()
    dst_meta.reflect(bind=dst_engine)

    # Monta a lista de tabelas na ordem correta, depois adiciona as que sobraram
    available = set(src_meta.tables.keys())
    ordered = [t for t in TABLE_ORDER if t in available]
    remaining = [t for t in available if t not in ordered]
    table_names = ordered + remaining

    print(f"\n→ Tabelas a migrar: {', '.join(table_names)}")

    with src_engine.connect() as src_conn:
        with dst_engine.connect() as dst_conn:
            # Desabilita verificação de FK temporariamente
            dst_conn.execute(text("SET session_replication_role = 'replica'"))

            for table_name in table_names:
                src_table = src_meta.tables[table_name]

                rows = src_conn.execute(src_table.select()).fetchall()
                if not rows:
                    print(f"  • {table_name}: vazia, pulando")
                    continue

                if table_name not in dst_meta.tables:
                    print(f"  ⚠ {table_name}: não encontrada no destino, pulando")
                    continue

                dst_table = dst_meta.tables[table_name]

                # Limpa e reinsere
                dst_conn.execute(dst_table.delete())

                records = [dict(r._mapping) for r in rows]
                batch_size = 500
                for i in range(0, len(records), batch_size):
                    dst_conn.execute(dst_table.insert(), records[i:i + batch_size])

                print(f"  ✓ {table_name}: {len(records)} registros")

            # Reabilita verificação de FK
            dst_conn.execute(text("SET session_replication_role = 'origin'"))
            dst_conn.commit()

    print(f"\n✅ Portfolio '{portfolio_key}' migrado com sucesso!")


def main():
    parser = argparse.ArgumentParser(description="Migra dados SQLite → PostgreSQL")
    parser.add_argument(
        "--portfolio",
        choices=["global", "brazil", "all"],
        default="all",
        help="Portfolio a migrar (padrão: all)",
    )
    args = parser.parse_args()

    from app.config import PORTFOLIOS, DATA_DIR

    portfolios_to_migrate = (
        list(PORTFOLIOS.keys()) if args.portfolio == "all" else [args.portfolio]
    )

    env_keys = {
        "global": "GLOBAL_DATABASE_URL",
        "brazil": "BRAZIL_DATABASE_URL",
    }

    errors = []
    for key in portfolios_to_migrate:
        env_var  = env_keys.get(key, f"{key.upper()}_DATABASE_URL")
        pg_url   = os.getenv(env_var, "")
        cfg      = PORTFOLIOS[key]
        sqlite_p = str(DATA_DIR / cfg["db_path"])

        if not pg_url:
            print(f"\n⚠ {key}: variável {env_var} não definida — pulando")
            errors.append(key)
            continue

        if not Path(sqlite_p).exists():
            print(f"\n⚠ {key}: arquivo SQLite '{sqlite_p}' não encontrado — pulando")
            errors.append(key)
            continue

        try:
            migrate_portfolio(key, sqlite_p, pg_url)
        except Exception as exc:
            print(f"\n❌ Erro ao migrar '{key}': {exc}")
            errors.append(key)

    print("\n" + "="*60)
    if errors:
        print(f"Concluído com erros: {', '.join(errors)}")
        sys.exit(1)
    else:
        print("✅ Migração concluída com sucesso!")


if __name__ == "__main__":
    main()
