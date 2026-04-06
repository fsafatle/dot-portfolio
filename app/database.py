"""
Database engine factory.

Local dev  → SQLite, um arquivo por portfolio (data/portfolio.db, data/brazil.db)
Produção   → PostgreSQL (Supabase), um projeto por portfolio

Variáveis de ambiente esperadas em produção (Streamlit Cloud secrets):
    GLOBAL_DATABASE_URL = "postgresql://user:pass@host:5432/global_db"
    BRAZIL_DATABASE_URL = "postgresql://user:pass@host:5432/brazil_db"
"""

import os

from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker, Session

Base = declarative_base()

# ── Chave de env por portfolio ────────────────────────────────────────────────
_ENV_KEY = {
    "global": "GLOBAL_DATABASE_URL",
    "brazil": "BRAZIL_DATABASE_URL",
}

# Cache de engines (SQLite por path ou PostgreSQL por URL)
_engines: dict[str, object] = {}


def _build_engine(url: str):
    """Cria engine adequado para SQLite ou PostgreSQL."""
    # Heroku/Render/Railway exportam "postgres://" — SQLAlchemy exige "postgresql://"
    url = url.replace("postgres://", "postgresql://", 1)
    kwargs = {}
    if url.startswith("sqlite"):
        kwargs["connect_args"] = {"check_same_thread": False}
    return create_engine(url, **kwargs)


def get_db_for(portfolio_key: str) -> Session:
    """
    Retorna uma sessão SQLAlchemy para o portfolio indicado.

    Em produção lê a URL PostgreSQL da variável de ambiente correspondente.
    Em dev usa SQLite local.
    """
    from app.config import PORTFOLIOS, DATA_DIR

    # ── Produção: PostgreSQL ──────────────────────────────────────────────────
    env_key = _ENV_KEY.get(portfolio_key, f"{portfolio_key.upper()}_DATABASE_URL")
    pg_url  = os.getenv(env_key, "")
    if pg_url:
        cache_key = pg_url
        if cache_key not in _engines:
            eng = _build_engine(pg_url)
            Base.metadata.create_all(bind=eng)
            _engines[cache_key] = eng
        eng = _engines[cache_key]
        Sess = sessionmaker(autocommit=False, autoflush=False, bind=eng)
        return Sess()

    # ── Dev: SQLite local ─────────────────────────────────────────────────────
    cfg     = PORTFOLIOS[portfolio_key]
    db_path = str(DATA_DIR / cfg["db_path"])
    if db_path not in _engines:
        eng = _build_engine(f"sqlite:///{db_path}")
        Base.metadata.create_all(bind=eng)
        _engines[db_path] = eng
    eng = _engines[db_path]
    Sess = sessionmaker(autocommit=False, autoflush=False, bind=eng)
    return Sess()


# ── Compat legado ─────────────────────────────────────────────────────────────
# Alguns scripts antigos importam 'engine' e 'SessionLocal' diretamente.
# Mantemos apontando para o portfolio global por compatibilidade.

from app.config import DATABASE_URL as _DEFAULT_URL  # noqa: E402

engine       = _build_engine(_DEFAULT_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_session():
    """Context-managed session (legado)."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_db() -> Session:
    """Sessão direta para o portfolio global (legado)."""
    return SessionLocal()
