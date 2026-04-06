"""
Serviço de ledger — único ponto de escrita de transações.

Princípio: o ledger é imutável.
  - Inserção: cria nova Transaction, chama recompute_from(date).
  - Anulação: is_void=True + recompute_from(date).
  - Edição:   void_transaction + record_transaction (void+repost).

Todo movimento de portfólio — aporte, realocação, venda — é composto
de uma ou mais Transactions atômicas neste ledger.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Optional

from sqlalchemy.orm import Session

from app.engine.reprocessor import recompute_from
from app.models import Transaction, EngineAuditLog, Portfolio, Asset

logger = logging.getLogger(__name__)

# ── Definição de tipos válidos e cálculo de cash_impact ─────────────────────

VALID_TYPES = {
    "aporte", "retirada", "despesa",
    "compra", "venda",
    "dividendo", "cupom", "amortizacao",
    "ajuste_preco", "ajuste_qty",
}

MOVEMENT_TYPE_LABELS = {
    "aporte":     "💰 Aporte",
    "retirada":   "📤 Retirada",
    "despesa":    "💸 Despesa / Taxa",
    "compra":     "📈 Compra de Ativo",
    "venda":      "📉 Venda de Ativo",
    "dividendo":  "🎁 Dividendo",
    "cupom":      "🎁 Cupom / Juros",
    "amortizacao":"🔄 Amortização",
    "ajuste_preco": "🔧 Ajuste de Preço",
    "ajuste_qty":   "🔧 Ajuste de Quantidade",
}


def _calc_cash_impact(tx_type: str, value: float,
                       qty: float, price: float) -> float:
    """Calcula o impacto no caixa de uma transação (positivo = entrada)."""
    if tx_type == "aporte":
        return +value
    elif tx_type in ("retirada", "despesa"):
        return -value
    elif tx_type == "compra":
        return -(qty * price)
    elif tx_type == "venda":
        return +(qty * price)
    elif tx_type in ("dividendo", "cupom", "amortizacao"):
        return +value
    else:
        return 0.0   # ajuste_preco, ajuste_qty


def get_portfolio(db: Session, portfolio_id: int) -> Portfolio:
    p = db.query(Portfolio).filter_by(id=portfolio_id).first()
    if p is None:
        raise ValueError(f"Portfólio id={portfolio_id} não encontrado.")
    return p


# ── Operações fundamentais ───────────────────────────────────────────────────

def record_transaction(
    db: Session,
    portfolio_id: int,
    tx_date: date,
    tx_type: str,
    value: float,
    asset_id: Optional[int] = None,
    quantity: Optional[float] = None,
    price: Optional[float] = None,
    description: str = "",
    recompute: bool = True,
) -> Transaction:
    """
    Registra uma transação atômica no ledger e (opcionalmente) reprocessa.

    Para movimentos compostos (ex: aporte + compras), chame com recompute=False
    em cada transação individual e chame recompute_from() uma vez no final.
    """
    if tx_type not in VALID_TYPES:
        raise ValueError(f"Tipo de transação inválido: {tx_type!r}. "
                         f"Válidos: {sorted(VALID_TYPES)}")
    if value < 0:
        raise ValueError("value deve ser ≥ 0. A direção é determinada pelo tipo.")

    portfolio = get_portfolio(db, portfolio_id)
    if tx_date < portfolio.base_date:
        raise ValueError(
            f"Data {tx_date} anterior ao início do portfólio ({portfolio.base_date})."
        )

    # Para compra/venda, qty e price são obrigatórios
    if tx_type in ("compra", "venda"):
        if not quantity or quantity <= 0:
            raise ValueError(f"Quantidade obrigatória para {tx_type}.")
        if price is None or price < 0:
            raise ValueError(f"Preço obrigatório para {tx_type}.")

    qty_safe   = quantity or 0.0
    price_safe = price    or 0.0
    impact     = _calc_cash_impact(tx_type, value, qty_safe, price_safe)

    tx = Transaction(
        portfolio_id = portfolio_id,
        date         = tx_date,
        type         = tx_type,
        asset_id     = asset_id,
        quantity     = quantity,
        price        = price,
        value        = value,
        cash_impact  = impact,
        description  = description or "",
    )
    db.add(tx)
    db.flush()

    db.add(EngineAuditLog(
        portfolio_id=portfolio_id,
        transaction_id=tx.id,
        action="insert",
        note=description or tx_type,
    ))
    db.flush()

    if recompute:
        recompute_from(db, portfolio_id, tx_date)

    logger.info("TX registrada: #%d %s %s %s",
                tx.id, tx_date, tx_type, value)
    return tx


def void_transaction(
    db: Session,
    transaction_id: int,
    note: str = "",
    recompute: bool = True,
) -> date:
    """
    Anula uma transação (soft delete).
    Retorna a data da transação para o caller usar no reprocessamento.
    """
    tx = db.query(Transaction).filter_by(id=transaction_id).first()
    if tx is None:
        raise ValueError(f"Transação #{transaction_id} não encontrada.")
    if tx.is_void:
        raise ValueError(f"Transação #{transaction_id} já está anulada.")

    tx.is_void   = True
    tx.voided_at = datetime.utcnow()
    db.flush()

    db.add(EngineAuditLog(
        portfolio_id=tx.portfolio_id,
        transaction_id=transaction_id,
        action="void",
        note=note or "anulação manual",
    ))
    db.flush()

    if recompute:
        recompute_from(db, tx.portfolio_id, tx.date)

    logger.info("TX anulada: #%d", transaction_id)
    return tx.date


def edit_transaction(
    db: Session,
    transaction_id: int,
    **new_values,
) -> Transaction:
    """
    Edita via void + repost.
    new_values: campos a alterar (tx_date, tx_type, value, asset_id,
                                  quantity, price, description)
    """
    original = db.query(Transaction).filter_by(id=transaction_id).first()
    if original is None:
        raise ValueError(f"Transação #{transaction_id} não encontrada.")

    # Anula sem reprocessar (fazemos no final)
    void_transaction(db, transaction_id, note="void para edição",
                     recompute=False)

    # Herda campos não alterados
    kwargs = {
        "portfolio_id": original.portfolio_id,
        "tx_date":      new_values.get("tx_date",      original.date),
        "tx_type":      new_values.get("tx_type",      original.type),
        "value":        new_values.get("value",        original.value),
        "asset_id":     new_values.get("asset_id",     original.asset_id),
        "quantity":     new_values.get("quantity",     original.quantity),
        "price":        new_values.get("price",        original.price),
        "description":  new_values.get("description",  original.description or ""),
        "recompute":    False,
    }

    new_tx = record_transaction(db, **kwargs)
    new_tx.original_id = transaction_id
    db.flush()

    # Reprocessa a partir da data mais antiga
    earliest = min(original.date, kwargs["tx_date"])
    recompute_from(db, original.portfolio_id, earliest)

    db.add(EngineAuditLog(
        portfolio_id=original.portfolio_id,
        transaction_id=new_tx.id,
        action="edit",
        note=f"substitui #{transaction_id}",
    ))

    logger.info("TX editada: #%d → nova #%d", transaction_id, new_tx.id)
    return new_tx


# ── Helpers para movimentos compostos (aporte/realocação/resgate) ────────────

def record_aporte(
    db: Session,
    portfolio_id: int,
    mv_date: date,
    buy_lines: list[dict],
    description: str = "",
) -> list[Transaction]:
    """
    Registra aporte: 1 tx de aporte (cash in) + 1 tx de compra por ativo.

    buy_lines: [{asset_id, ticker, value, quantity, price}]
    quantity e price obrigatórios. Se não disponíveis, derive antes de chamar.
    """
    txs = []
    total = sum(ln["value"] for ln in buy_lines)

    # 1. Aporte (dinheiro entra)
    txs.append(record_transaction(
        db, portfolio_id, mv_date, "aporte", total,
        description=description or "Aporte",
        recompute=False,
    ))

    # 2. Compras
    for ln in buy_lines:
        txs.append(record_transaction(
            db, portfolio_id, mv_date, "compra",
            value=ln["value"],
            asset_id=ln["asset_id"],
            quantity=ln["quantity"],
            price=ln["price"],
            description=f"Compra {ln.get('ticker','')}",
            recompute=False,
        ))

    recompute_from(db, portfolio_id, mv_date)
    return txs


def record_realocacao(
    db: Session,
    portfolio_id: int,
    mv_date: date,
    sell_lines: list[dict],
    buy_lines: list[dict],
    description: str = "",
) -> list[Transaction]:
    """
    Registra realocação: vendas + compras (caixa líquido = 0).
    Valida equilíbrio antes de gravar.
    """
    total_sell = sum(ln["value"] for ln in sell_lines)
    total_buy  = sum(ln["value"] for ln in buy_lines)
    if abs(total_sell - total_buy) > 0.01:
        raise ValueError(
            f"Realocação desequilibrada: vendas={total_sell:.2f}, "
            f"compras={total_buy:.2f}."
        )

    txs = []
    for ln in sell_lines:
        txs.append(record_transaction(
            db, portfolio_id, mv_date, "venda",
            value=ln["value"],
            asset_id=ln["asset_id"],
            quantity=ln["quantity"],
            price=ln["price"],
            description=f"Venda {ln.get('ticker','')}",
            recompute=False,
        ))

    for ln in buy_lines:
        txs.append(record_transaction(
            db, portfolio_id, mv_date, "compra",
            value=ln["value"],
            asset_id=ln["asset_id"],
            quantity=ln["quantity"],
            price=ln["price"],
            description=f"Compra {ln.get('ticker','')}",
            recompute=False,
        ))

    recompute_from(db, portfolio_id, mv_date)
    return txs


def record_resgate(
    db: Session,
    portfolio_id: int,
    mv_date: date,
    sell_lines: list[dict],
    description: str = "",
) -> list[Transaction]:
    """Registra resgate: vendas + retirada do total."""
    txs = []
    total = sum(ln["value"] for ln in sell_lines)

    for ln in sell_lines:
        txs.append(record_transaction(
            db, portfolio_id, mv_date, "venda",
            value=ln["value"],
            asset_id=ln["asset_id"],
            quantity=ln["quantity"],
            price=ln["price"],
            description=f"Venda {ln.get('ticker','')}",
            recompute=False,
        ))

    txs.append(record_transaction(
        db, portfolio_id, mv_date, "retirada", total,
        description=description or "Resgate",
        recompute=False,
    ))

    recompute_from(db, portfolio_id, mv_date)
    return txs


def record_rendimento(
    db: Session,
    portfolio_id: int,
    mv_date: date,
    asset_id: int,
    income_type: str,   # dividendo | cupom
    value: float,
    description: str = "",
) -> Transaction:
    """Registra dividendo ou cupom."""
    return record_transaction(
        db, portfolio_id, mv_date, income_type,
        value=value,
        asset_id=asset_id,
        description=description,
        recompute=True,
    )


# ── Consultas ────────────────────────────────────────────────────────────────

def get_transactions(
    db: Session,
    portfolio_id: int,
    include_void: bool = False,
    limit: Optional[int] = None,
) -> list[Transaction]:
    q = (db.query(Transaction)
           .filter(Transaction.portfolio_id == portfolio_id))
    if not include_void:
        q = q.filter(Transaction.is_void == False)
    q = q.order_by(Transaction.date.desc(), Transaction.id.desc())
    return q.limit(limit).all() if limit else q.all()


def get_transactions_for_day(
    db: Session,
    portfolio_id: int,
    tx_date: date,
) -> list[Transaction]:
    return (db.query(Transaction)
              .filter_by(portfolio_id=portfolio_id, date=tx_date, is_void=False)
              .order_by(Transaction.id)
              .all())
