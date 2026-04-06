from sqlalchemy import (
    Column, Integer, String, Float, Date, DateTime,
    Boolean, Text, ForeignKey, UniqueConstraint, CheckConstraint,
)
from sqlalchemy.orm import relationship
from datetime import datetime
from app.database import Base


class Asset(Base):
    __tablename__ = "assets"

    id = Column(Integer, primary_key=True)
    # ── v2: FK para o portfólio deste DB (nullable para compat com dados antigos) ──
    portfolio_id = Column(Integer, ForeignKey("portfolios.id"), nullable=True)
    name = Column(String(200), nullable=False)
    ticker = Column(String(50), nullable=False, unique=True)
    asset_class = Column(String(50), nullable=False)
    # equity | fixed_income | commodity | crypto | cash_equivalent
    currency = Column(String(10), default="USD")
    data_source = Column(String(50), default="yahoo")
    # yahoo | yahoo_rate | manual
    bucket = Column(String(50), nullable=True)
    # Yield | Growth | Reserva de Valor | Liquidez
    role = Column(String(20), default="beta")
    # beta | alpha
    notes = Column(Text)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    allocations = relationship("Allocation", back_populates="asset")
    prices = relationship("Price", back_populates="asset", cascade="all, delete-orphan")
    log_entries = relationship("AllocationLog", back_populates="asset")
    # v2
    asset_prices = relationship("AssetPrice", back_populates="asset",
                                cascade="all, delete-orphan")
    transactions = relationship("Transaction", back_populates="asset")


class Allocation(Base):
    """Versioned allocation record. A new row is inserted on every change."""
    __tablename__ = "allocations"

    id = Column(Integer, primary_key=True)
    asset_id = Column(Integer, ForeignKey("assets.id"), nullable=False)
    weight = Column(Float, nullable=False)   # 0.0 – 1.0
    start_date = Column(Date, nullable=False)
    end_date = Column(Date, nullable=True)   # NULL ⟹ currently active
    notes = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)

    asset = relationship("Asset", back_populates="allocations")


class Price(Base):
    """Daily price cache. For ^IRX this stores the simulated NAV."""
    __tablename__ = "prices"

    id = Column(Integer, primary_key=True)
    asset_id = Column(Integer, ForeignKey("assets.id"), nullable=False)
    date = Column(Date, nullable=False)
    close_price = Column(Float, nullable=False)
    source = Column(String(50), default="yahoo")
    fetched_at = Column(DateTime, default=datetime.utcnow)

    asset = relationship("Asset", back_populates="prices")

    __table_args__ = (
        UniqueConstraint("asset_id", "date", name="uq_asset_date"),
    )


class PortfolioSnapshot(Base):
    """Daily portfolio index value (starts at PORTFOLIO_BASE_VALUE)."""
    __tablename__ = "portfolio_snapshots"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False, unique=True)
    index_value = Column(Float, nullable=False)
    daily_return = Column(Float, nullable=True)  # decimal, e.g. 0.01 = 1 %
    created_at = Column(DateTime, default=datetime.utcnow)


class AllocationLog(Base):
    """Immutable audit trail for every allocation change."""
    __tablename__ = "allocation_log"

    id = Column(Integer, primary_key=True)
    asset_id = Column(Integer, ForeignKey("assets.id"), nullable=False)
    action = Column(String(20), nullable=False)  # add | update | remove
    old_weight = Column(Float, nullable=True)
    new_weight = Column(Float, nullable=True)
    effective_date = Column(Date, nullable=False)
    notes = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)

    asset = relationship("Asset", back_populates="log_entries")


# ── Cash flow ─────────────────────────────────────────────────────────────────

class CashAccount(Base):
    """One cash account per portfolio (keyed by portfolio_key string)."""
    __tablename__ = "cash_accounts"

    id            = Column(Integer, primary_key=True)
    portfolio_key = Column(String(50), nullable=False, unique=True)
    balance       = Column(Float, nullable=False, default=0.0)
    # cached sum — always updated in the same transaction as a new CashTransaction
    currency      = Column(String(10), default="USD")
    updated_at    = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    transactions  = relationship(
        "CashTransaction", back_populates="account",
        cascade="all, delete-orphan", order_by="CashTransaction.date",
    )


class CashTransaction(Base):
    """
    Immutable ledger entry for every cash movement.

    type values:
      deposit    — external money coming in (aporte do investidor)
      withdrawal — money leaving the portfolio (resgate)
      dividend   — equity dividend credited to cash
      coupon     — fixed-income coupon/interest credited to cash
      buy        — cash deployed into an asset position
      fee        — cost / tax (optional, negative)

    Sign convention: positive = cash in, negative = cash out.
    movement_id — optional link to a PortfolioMovement that originated this entry.
    """
    __tablename__ = "cash_transactions"

    id          = Column(Integer, primary_key=True)
    account_id  = Column(Integer, ForeignKey("cash_accounts.id"), nullable=False)
    date        = Column(Date, nullable=False)
    type        = Column(String(30), nullable=False)
    amount      = Column(Float, nullable=False)
    # positive = inflow, negative = outflow
    asset_id    = Column(Integer, ForeignKey("assets.id"), nullable=True)
    # NULL for deposit/withdrawal/fee; set for dividend/coupon/buy
    movement_id = Column(Integer, nullable=True)
    # optional FK to portfolio_movements.id (stored as plain int, no FK constraint)
    description = Column(Text, nullable=True)
    created_at  = Column(DateTime, default=datetime.utcnow)

    account     = relationship("CashAccount", back_populates="transactions")
    asset       = relationship("Asset")

    __table_args__ = (
        CheckConstraint("amount != 0", name="chk_cash_nonzero"),
    )


# ── Portfolio Movements (linked cash + allocation events) ─────────────────────

class PortfolioMovement(Base):
    """
    A single logical event that may change both cash balance and portfolio weights.

    type values:
      cash_in       — external deposit then immediately invested in assets
      reallocation  — swap between assets with no cash change (e.g. IMA-B → Infleet)
      cash_out      — redeem assets back to cash
      income        — dividend/coupon received into cash (no allocation change)

    portfolio_value_ref — the user-provided market value of the portfolio (in currency)
                          at the moment of the movement. Used to compute new weights
                          from R$ amounts. NULL for reallocation (uses % directly).
    """
    __tablename__ = "portfolio_movements"

    id                   = Column(Integer, primary_key=True)
    portfolio_key        = Column(String(50), nullable=False)
    date                 = Column(Date, nullable=False)
    type                 = Column(String(30), nullable=False)
    total_amount         = Column(Float, nullable=True)
    # net cash flow: + = money in, - = money out; NULL for reallocation
    portfolio_value_ref  = Column(Float, nullable=True)
    # portfolio market value in R$/USD at moment of movement
    description          = Column(Text, nullable=True)
    created_at           = Column(DateTime, default=datetime.utcnow)

    lines = relationship(
        "PortfolioMovementLine", back_populates="movement",
        cascade="all, delete-orphan",
    )


class PortfolioMovementLine(Base):
    """
    Per-asset detail line of a PortfolioMovement.

    direction: buy | sell
    amount_brl — monetary value in portfolio currency (always positive)
    weight_before / weight_after — allocation weight before and after the movement
    """
    __tablename__ = "portfolio_movement_lines"

    id             = Column(Integer, primary_key=True)
    movement_id    = Column(Integer, ForeignKey("portfolio_movements.id"), nullable=False)
    asset_id       = Column(Integer, ForeignKey("assets.id"), nullable=False)
    direction      = Column(String(10), nullable=False)  # buy | sell
    amount         = Column(Float, nullable=True)        # R$/USD value, always positive
    weight_before  = Column(Float, nullable=True)        # 0–1
    weight_after   = Column(Float, nullable=True)        # 0–1
    created_at     = Column(DateTime, default=datetime.utcnow)

    movement = relationship("PortfolioMovement", back_populates="lines")
    asset    = relationship("Asset")


# ═══════════════════════════════════════════════════════════════════════════════
# V2 — Motor de portfólio financeiramente correto
#
# Princípio: TRANSACTIONS é o ledger imutável.
#            Tudo mais (positions, daily_snapshots, asset_snapshots) é cache
#            derivado pelo reprocessor. Pode ser descartado e recalculado.
# ═══════════════════════════════════════════════════════════════════════════════


class Portfolio(Base):
    """
    Registro de configuração do portfólio dentro do próprio banco.
    Cada arquivo .db contém exatamente um Portfolio (id=1).
    """
    __tablename__ = "portfolios"

    id           = Column(Integer, primary_key=True)
    key          = Column(String(50), nullable=False, unique=True)
    name         = Column(String(200), nullable=False)
    currency     = Column(String(10), default="BRL")
    base_date    = Column(Date, nullable=False)    # D0: cota=cota_base, cash=initial_cash
    initial_cash = Column(Float, nullable=False, default=0.0)
    cota_base    = Column(Float, default=1000.0)   # índice começa em 1000
    created_at   = Column(DateTime, default=datetime.utcnow)

    transactions     = relationship("Transaction",  back_populates="portfolio")
    daily_snapshots  = relationship("DailySnapshot", back_populates="portfolio",
                                    cascade="all, delete-orphan")
    asset_snapshots  = relationship("AssetSnapshot", back_populates="portfolio",
                                    cascade="all, delete-orphan")
    positions        = relationship("Position", back_populates="portfolio",
                                    cascade="all, delete-orphan")


class Transaction(Base):
    """
    LEDGER — fonte de verdade imutável.
    Nunca editar registros existentes; para corrigir: is_void=True + novo registro.

    Tipos:
        aporte      — dinheiro externo entra no portfólio         cash_impact = +value
        retirada    — dinheiro sai do portfólio                   cash_impact = -value
        despesa     — custo/imposto                               cash_impact = -value
        compra      — caixa paga para comprar ativo               cash_impact = -(qty×price)
        venda       — caixa recebe ao vender ativo                cash_impact = +(qty×price)
        dividendo   — dividendo de ativo equity                   cash_impact = +value
        cupom       — cupom/juros renda fixa                      cash_impact = +value
        amortizacao — amortização de principal (reduz qty)        cash_impact = +value
        ajuste_preco — corrige preço (sem impacto em caixa)       cash_impact = 0
        ajuste_qty  — corrige quantidade (sem impacto em caixa)   cash_impact = 0

    Sinal cash_impact: + = caixa recebe, − = caixa paga.
    value: sempre positivo (a direção está no tipo).
    """
    __tablename__ = "transactions"

    id           = Column(Integer, primary_key=True)
    portfolio_id = Column(Integer, ForeignKey("portfolios.id"), nullable=False)
    date         = Column(Date, nullable=False)
    type         = Column(String(30), nullable=False)
    asset_id     = Column(Integer, ForeignKey("assets.id"), nullable=True)
    quantity     = Column(Float, nullable=True)    # cotas/ações negociadas
    price        = Column(Float, nullable=True)    # preço unitário
    value        = Column(Float, nullable=False)   # valor total (sempre ≥ 0)
    cash_impact  = Column(Float, nullable=False)   # impacto no caixa (+/-)
    description  = Column(Text, nullable=True)
    is_void      = Column(Boolean, default=False)
    voided_at    = Column(DateTime, nullable=True)
    original_id  = Column(Integer, ForeignKey("transactions.id"), nullable=True)
    created_at   = Column(DateTime, default=datetime.utcnow)

    portfolio = relationship("Portfolio", back_populates="transactions")
    asset     = relationship("Asset", back_populates="transactions")


class Position(Base):
    """
    Cache de posição por ativo por dia.
    Reconstruído pelo reprocessor — NÃO editar diretamente.
    """
    __tablename__ = "positions"

    id            = Column(Integer, primary_key=True)
    portfolio_id  = Column(Integer, ForeignKey("portfolios.id"), nullable=False)
    asset_id      = Column(Integer, ForeignKey("assets.id"), nullable=False)
    date          = Column(Date, nullable=False)
    quantity      = Column(Float, default=0.0)
    avg_cost      = Column(Float, default=0.0)
    realized_pnl  = Column(Float, default=0.0)
    dividends     = Column(Float, default=0.0)

    portfolio = relationship("Portfolio", back_populates="positions")

    __table_args__ = (UniqueConstraint("portfolio_id", "asset_id", "date"),)


class DailySnapshot(Base):
    """
    Snapshot diário do portfólio consolidado (cache).
    Reconstruído pelo reprocessor.
    NAV = cash + invested_value (marcação a mercado real).
    Cota = índice base 1000, isolado de aportes via retorno ex-fluxo.
    """
    __tablename__ = "daily_snapshots"

    id             = Column(Integer, primary_key=True)
    portfolio_id   = Column(Integer, ForeignKey("portfolios.id"), nullable=False)
    date           = Column(Date, nullable=False)
    cash           = Column(Float, nullable=False)
    invested_value = Column(Float, nullable=False, default=0.0)  # Σ(qty × price)
    nav            = Column(Float, nullable=False)               # cash + invested_value
    cota           = Column(Float, nullable=False)               # índice base 1000
    net_flow       = Column(Float, default=0.0)                  # aportes - retiradas
    daily_return   = Column(Float, nullable=True)                # retorno ex-fluxo
    cum_return     = Column(Float, nullable=True)                # desde D0

    portfolio = relationship("Portfolio", back_populates="daily_snapshots")

    __table_args__ = (UniqueConstraint("portfolio_id", "date"),)


class AssetSnapshot(Base):
    """
    Snapshot diário por ativo (cache).
    Inclui PnL realizado, não realizado, dividendos e retorno total.
    """
    __tablename__ = "asset_snapshots"

    id             = Column(Integer, primary_key=True)
    portfolio_id   = Column(Integer, ForeignKey("portfolios.id"), nullable=False)
    asset_id       = Column(Integer, ForeignKey("assets.id"), nullable=False)
    date           = Column(Date, nullable=False)
    quantity       = Column(Float, default=0.0)
    avg_cost       = Column(Float, default=0.0)
    price          = Column(Float, nullable=True)
    market_value   = Column(Float, default=0.0)    # qty × price
    unrealized_pnl = Column(Float, default=0.0)    # qty × (price − avg_cost)
    realized_pnl   = Column(Float, default=0.0)
    dividends      = Column(Float, default=0.0)
    total_pnl      = Column(Float, default=0.0)    # unrealized + realized + dividends

    portfolio = relationship("Portfolio", back_populates="asset_snapshots")
    asset     = relationship("Asset")

    __table_args__ = (UniqueConstraint("portfolio_id", "asset_id", "date"),)


class AssetPrice(Base):
    """
    Tabela de preços do motor v2 (separada da Price legada).
    source = 'market' | 'manual'
    """
    __tablename__ = "asset_prices"

    id         = Column(Integer, primary_key=True)
    asset_id   = Column(Integer, ForeignKey("assets.id"), nullable=False)
    date       = Column(Date, nullable=False)
    price      = Column(Float, nullable=False)
    source     = Column(String(10), default="market")
    created_at = Column(DateTime, default=datetime.utcnow)

    asset = relationship("Asset", back_populates="asset_prices")

    __table_args__ = (UniqueConstraint("asset_id", "date"),)


class EngineAuditLog(Base):
    """Audit log imutável para todas as operações do motor v2."""
    __tablename__ = "engine_audit_log"

    id             = Column(Integer, primary_key=True)
    portfolio_id   = Column(Integer, ForeignKey("portfolios.id"), nullable=False)
    transaction_id = Column(Integer, ForeignKey("transactions.id"), nullable=True)
    action         = Column(String(20), nullable=False)  # insert|void|edit|reprocess
    note           = Column(Text, nullable=True)
    created_at     = Column(DateTime, default=datetime.utcnow)
