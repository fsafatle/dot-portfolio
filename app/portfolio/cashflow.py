"""
Cash-flow engine — caixa é a fonte de verdade de todos os movimentos.

PRINCÍPIO:
    Todo movimento de portfólio nasce de uma movimentação de caixa.
    Não existe alteração de peso sem lançamento correspondente no extrato.

TIPOS DE MOVIMENTO (PortfolioMovement.type):
    aporte      — dinheiro novo do investidor → compra ativos
                  Cash: + depósito, − compra por ativo
    realocacao  — vende um ativo, compra outro; saldo caixa = 0
                  Cash: + venda, − compra  (duas pernas, líquido zero)
    resgate     — vende ativos e retira dinheiro
                  Cash: + venda por ativo, − saque total
    rendimento  — dividendo ou cupom creditado; pesos inalterados
                  Cash: + dividendo/cupom

TIPOS DE CashTransaction.type:
    deposito    — entrada de dinheiro do investidor
    saque       — saída de dinheiro para o investidor
    compra      — caixa paga para comprar ativo
    venda       — caixa recebe ao vender ativo
    dividendo   — dividendo de ativo equity
    cupom       — cupom / juros renda fixa
    taxa        — custo / IOF / IR

Sinal: + = entrada de caixa, − = saída de caixa.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Optional

import pandas as pd
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models import (
    Asset, CashAccount, CashTransaction,
    PortfolioMovement, PortfolioMovementLine,
)

logger = logging.getLogger(__name__)

MOVEMENT_TYPE_LABELS = {
    "aporte":     "💰 Aporte + Investimento",
    "realocacao": "🔄 Realocação entre Ativos",
    "resgate":    "📤 Resgate",
    "rendimento": "🎁 Rendimento (dividendo / cupom)",
}

TX_TYPE_LABELS = {
    "deposito":  "Depósito",
    "saque":     "Saque",
    "compra":    "Compra de ativo",
    "venda":     "Venda de ativo",
    "dividendo": "Dividendo",
    "cupom":     "Cupom / Juros",
    "taxa":      "Taxa / Imposto",
}


# ---------------------------------------------------------------------------
# Conta corrente
# ---------------------------------------------------------------------------

def get_or_create_account(db: Session, portfolio_key: str,
                           currency: str = "USD") -> CashAccount:
    acct = db.query(CashAccount).filter_by(portfolio_key=portfolio_key).first()
    if acct is None:
        acct = CashAccount(portfolio_key=portfolio_key, balance=0.0, currency=currency)
        db.add(acct)
        db.flush()
    return acct


def get_balance(db: Session, portfolio_key: str) -> float:
    acct = db.query(CashAccount).filter_by(portfolio_key=portfolio_key).first()
    if acct is None:
        return 0.0
    r = (db.query(func.sum(CashTransaction.amount))
         .filter(CashTransaction.account_id == acct.id).scalar())
    return float(r or 0.0)


def _sync_balance(db: Session, acct: CashAccount) -> None:
    r = (db.query(func.sum(CashTransaction.amount))
         .filter(CashTransaction.account_id == acct.id).scalar())
    acct.balance = float(r or 0.0)


def _add_cash_tx(db, acct, tx_type, signed_amount, tx_date,
                 asset_id=None, movement_id=None, description=""):
    tx = CashTransaction(
        account_id=acct.id, date=tx_date, type=tx_type,
        amount=signed_amount, asset_id=asset_id,
        movement_id=movement_id, description=description,
    )
    db.add(tx)
    db.flush()
    return tx


# ---------------------------------------------------------------------------
# Cálculo de novos pesos a partir de fluxos em R$
# ---------------------------------------------------------------------------

def compute_new_weights(
    current_weights: dict[str, float],
    portfolio_value: float,
    flows: dict[str, float],   # {ticker: R$}  + = compra, − = venda
) -> dict[str, float]:
    """
    Fórmula:
        new_value_i  = peso_i × V + fluxo_i
        new_total    = V + Σ fluxos
        new_weight_i = new_value_i / new_total

    Suporta portfólio vazio (V = 0): pesos são proporcionais aos fluxos.
    """
    if portfolio_value < 0:
        raise ValueError("Valor do portfólio não pode ser negativo.")

    all_tickers = set(current_weights) | set(flows)
    new_values: dict[str, float] = {}
    for t in all_tickers:
        v = current_weights.get(t, 0.0) * portfolio_value + flows.get(t, 0.0)
        if v < -0.01:
            raise ValueError(
                f"Ativo {t}: valor ficaria negativo "
                f"(atual R${current_weights.get(t,0)*portfolio_value:.2f}, "
                f"fluxo R${flows.get(t,0):.2f})."
            )
        if v > 0.001:
            new_values[t] = v

    total = sum(new_values.values())
    if total <= 0:
        raise ValueError("Portfólio ficaria com valor zero após o movimento.")

    return {t: v / total for t, v in new_values.items()}


# ---------------------------------------------------------------------------
# Resolução / criação de ativos
# ---------------------------------------------------------------------------

def _resolve_asset(db: Session, line: dict) -> int:
    """
    Retorna asset_id para uma linha de movimento.
    Se o ativo não existir e a linha tiver metadados, cria o ativo.
    """
    if line.get("asset_id"):
        return line["asset_id"]

    ticker = line["ticker"]
    asset  = db.query(Asset).filter_by(ticker=ticker).first()
    if asset is None:
        asset = Asset(
            name        = line.get("name", ticker),
            ticker      = ticker,
            asset_class = line.get("asset_class", "other"),
            data_source = line.get("data_source", "manual"),
            bucket      = line.get("bucket"),
            role        = line.get("role", "beta"),
            is_active   = True,
        )
        db.add(asset)
        db.flush()
        logger.info("Novo ativo criado: %s", ticker)
    return asset.id


# ---------------------------------------------------------------------------
# MOVIMENTO PRINCIPAL — atômico
# ---------------------------------------------------------------------------

def execute_movement(
    db: Session,
    portfolio_key: str,
    mv_type: str,
    mv_date: date,
    portfolio_value_ref: float,   # valor atual do portfólio ANTES do movimento
    description: str,
    currency: str,
    buy_lines:    list[dict],   # [{ticker, amount, ?asset_id, ?name, ...}]
    sell_lines:   list[dict],   # idem
    income_lines: list[dict],   # [{ticker, amount, income_type, ?asset_id}]
    fn_get_active,
    fn_update_allocs,
    fn_build_snaps,
    start_date_str: str,
    base_value: float,
) -> PortfolioMovement:
    """
    Executa um movimento de portfólio de forma atômica:
      1. Resolve/cria ativos
      2. Calcula novos pesos
      3. Registra PortfolioMovement + PortfolioMovementLines
      4. Registra CashTransactions (vinculadas ao movimento)
      5. update_allocations + build_snapshots
    """
    if mv_type not in MOVEMENT_TYPE_LABELS:
        raise ValueError(f"Tipo de movimento desconhecido: {mv_type!r}")

    # ── 1. Resolver asset_ids (cria ativos novos se necessário) ─────────────
    for ln in buy_lines + sell_lines + income_lines:
        ln["asset_id"] = _resolve_asset(db, ln)

    # ── 2. Pesos atuais ─────────────────────────────────────────────────────
    active_allocs = fn_get_active(db, mv_date)
    current_w: dict[str, float] = {a.asset.ticker: a.weight for a in active_allocs}

    # ── 3. Calcular novos pesos ──────────────────────────────────────────────
    if mv_type == "rendimento":
        new_weights = current_w.copy()
    else:
        flows: dict[str, float] = {}
        for ln in sell_lines:
            flows[ln["ticker"]] = flows.get(ln["ticker"], 0.0) - ln["amount"]
        for ln in buy_lines:
            flows[ln["ticker"]] = flows.get(ln["ticker"], 0.0) + ln["amount"]

        if not flows:
            raise ValueError("Nenhum valor de compra ou venda informado.")

        if portfolio_value_ref < 0:
            raise ValueError("Valor do portfólio não pode ser negativo.")

        new_weights = compute_new_weights(current_w, portfolio_value_ref, flows)

        # Validar realocação: saldo líquido deve ser zero
        if mv_type == "realocacao":
            net = sum(flows.values())
            if abs(net) > 0.01:
                total_sell = sum(ln["amount"] for ln in sell_lines)
                total_buy  = sum(ln["amount"] for ln in buy_lines)
                raise ValueError(
                    f"Realocação desequilibrada: "
                    f"vendas {total_sell:.2f} ≠ compras {total_buy:.2f}. "
                    "O valor vendido deve ser igual ao valor comprado."
                )

    # ── 4. Registrar movimento ───────────────────────────────────────────────
    net_cash = sum(ln["amount"] for ln in sell_lines) - sum(ln["amount"] for ln in buy_lines)
    mv = PortfolioMovement(
        portfolio_key      = portfolio_key,
        date               = mv_date,
        type               = mv_type,
        total_amount       = net_cash if mv_type != "rendimento"
                             else sum(ln["amount"] for ln in income_lines),
        portfolio_value_ref = portfolio_value_ref if portfolio_value_ref > 0 else None,
        description        = description or "",
    )
    db.add(mv)
    db.flush()

    # ── 5. Linhas de movimento ───────────────────────────────────────────────
    for ln in sell_lines:
        db.add(PortfolioMovementLine(
            movement_id=mv.id, asset_id=ln["asset_id"],
            direction="venda", amount=ln["amount"],
            weight_before=current_w.get(ln["ticker"]),
            weight_after=new_weights.get(ln["ticker"]),
        ))
    for ln in buy_lines:
        db.add(PortfolioMovementLine(
            movement_id=mv.id, asset_id=ln["asset_id"],
            direction="compra", amount=ln["amount"],
            weight_before=current_w.get(ln["ticker"]),
            weight_after=new_weights.get(ln["ticker"]),
        ))
    for ln in income_lines:
        db.add(PortfolioMovementLine(
            movement_id=mv.id, asset_id=ln["asset_id"],
            direction="rendimento", amount=ln["amount"],
            weight_before=current_w.get(ln["ticker"]),
            weight_after=current_w.get(ln["ticker"]),
        ))
    db.flush()

    # ── 6. Cash transactions (TODAS vinculadas ao movimento) ────────────────
    acct = get_or_create_account(db, portfolio_key, currency)

    if mv_type == "aporte":
        total_investido = sum(ln["amount"] for ln in buy_lines)
        _add_cash_tx(db, acct, "deposito", +total_investido, mv_date,
                     movement_id=mv.id, description=description or "Aporte")
        for ln in buy_lines:
            _add_cash_tx(db, acct, "compra", -ln["amount"], mv_date,
                         asset_id=ln["asset_id"], movement_id=mv.id,
                         description=f"Compra {ln['ticker']}")

    elif mv_type == "realocacao":
        for ln in sell_lines:
            _add_cash_tx(db, acct, "venda", +ln["amount"], mv_date,
                         asset_id=ln["asset_id"], movement_id=mv.id,
                         description=f"Venda {ln['ticker']}")
        for ln in buy_lines:
            _add_cash_tx(db, acct, "compra", -ln["amount"], mv_date,
                         asset_id=ln["asset_id"], movement_id=mv.id,
                         description=f"Compra {ln['ticker']}")

    elif mv_type == "resgate":
        total_resgatado = sum(ln["amount"] for ln in sell_lines)
        for ln in sell_lines:
            _add_cash_tx(db, acct, "venda", +ln["amount"], mv_date,
                         asset_id=ln["asset_id"], movement_id=mv.id,
                         description=f"Venda {ln['ticker']}")
        _add_cash_tx(db, acct, "saque", -total_resgatado, mv_date,
                     movement_id=mv.id, description=description or "Resgate")

    elif mv_type == "rendimento":
        for ln in income_lines:
            inc_type = ln.get("income_type", "dividendo")
            _add_cash_tx(db, acct, inc_type, +ln["amount"], mv_date,
                         asset_id=ln["asset_id"], movement_id=mv.id,
                         description=f"{TX_TYPE_LABELS.get(inc_type,'Rend.')} {ln['ticker']}")

    _sync_balance(db, acct)

    # ── 7. Atualizar alocações + snapshots ───────────────────────────────────
    if mv_type != "rendimento":
        fn_update_allocs(db, new_weights, mv_date, notes=description or mv_type)
        fn_build_snaps(db, force_rebuild=True,
                       start_date_str=start_date_str, base_value=base_value)

    db.flush()
    return mv


# ---------------------------------------------------------------------------
# Edição de movimento existente (void + repost)
# ---------------------------------------------------------------------------

def load_movement_for_edit(db: Session, movement_id: int) -> dict:
    """
    Carrega um movimento e suas linhas em um dict pronto para pré-popular
    o formulário de edição.

    Retorna:
        {
          "id":        int,
          "mv_type":   str,
          "mv_date":   date,
          "pv_ref":    float | None,
          "description": str,
          "buy_lines":  [{asset_id, ticker, name, amount}, ...],
          "sell_lines": [{asset_id, ticker, name, amount}, ...],
          "income_lines": [{asset_id, ticker, name, amount, income_type}, ...],
        }
    """
    mv = db.query(PortfolioMovement).filter_by(id=movement_id).first()
    if mv is None:
        raise ValueError(f"Movimento #{movement_id} não encontrado.")

    buy_lines, sell_lines, income_lines = [], [], []
    for ln in mv.lines:
        ticker = ln.asset.ticker if ln.asset else "?"
        name   = ln.asset.name   if ln.asset else "?"
        entry  = {"asset_id": ln.asset_id, "ticker": ticker,
                  "name": name, "amount": ln.amount or 0.0}
        if ln.direction == "compra":
            buy_lines.append(entry)
        elif ln.direction == "venda":
            sell_lines.append(entry)
        elif ln.direction == "rendimento":
            entry["income_type"] = "dividendo"  # default; extrato mostra o tipo real
            income_lines.append(entry)

    return {
        "id":          mv.id,
        "mv_type":     mv.type,
        "mv_date":     mv.date,
        "pv_ref":      mv.portfolio_value_ref,
        "description": mv.description or "",
        "buy_lines":   buy_lines,
        "sell_lines":  sell_lines,
        "income_lines": income_lines,
    }


def void_movement(db: Session, movement_id: int) -> str:
    """
    Anula um movimento existente:
      - Remove as CashTransactions vinculadas (ressincroniza saldo)
      - Remove as PortfolioMovementLines
      - Remove o PortfolioMovement

    NÃO desfaz as alocações — o caller deve re-executar execute_movement
    com os valores corrigidos, o que sobrescreverá as alocações via
    update_allocations + build_snapshots.

    Retorna portfolio_key para o caller usar no build_snapshots.
    """
    mv = db.query(PortfolioMovement).filter_by(id=movement_id).first()
    if mv is None:
        raise ValueError(f"Movimento #{movement_id} não encontrado.")

    portfolio_key = mv.portfolio_key

    # Remover cash transactions vinculadas
    db.query(CashTransaction).filter(
        CashTransaction.movement_id == movement_id
    ).delete(synchronize_session=False)

    # Remover linhas de movimento
    db.query(PortfolioMovementLine).filter(
        PortfolioMovementLine.movement_id == movement_id
    ).delete(synchronize_session=False)

    # Remover o movimento
    db.delete(mv)
    db.flush()

    # Ressincronizar saldo de caixa
    acct = db.query(CashAccount).filter_by(portfolio_key=portfolio_key).first()
    if acct:
        _sync_balance(db, acct)

    return portfolio_key


# ---------------------------------------------------------------------------
# Taxa isolada (sem movimento de portfólio)
# ---------------------------------------------------------------------------

def record_taxa(db, portfolio_key, amount, tx_date,
                description="", asset_id=None, currency="USD"):
    if amount <= 0:
        raise ValueError("Valor deve ser > 0.")
    acct = get_or_create_account(db, portfolio_key, currency)
    if acct.balance - amount < -0.005:
        raise ValueError(
            f"Saldo insuficiente: disponível {acct.balance:.2f}, taxa {amount:.2f}."
        )
    tx = _add_cash_tx(db, acct, "taxa", -amount, tx_date,
                      asset_id=asset_id, description=description)
    _sync_balance(db, acct)
    return tx


# ---------------------------------------------------------------------------
# Consultas
# ---------------------------------------------------------------------------

def get_cash_flow_summary(db, portfolio_key):
    acct = db.query(CashAccount).filter_by(portfolio_key=portfolio_key).first()
    zero = dict(balance=0.0, total_depositos=0.0, total_saques=0.0,
                total_compras=0.0, total_vendas=0.0,
                total_dividendos=0.0, total_cupons=0.0, total_taxas=0.0)
    if acct is None:
        return zero

    def _s(t):
        r = (db.query(func.sum(CashTransaction.amount))
             .filter(CashTransaction.account_id == acct.id,
                     CashTransaction.type == t).scalar())
        return abs(float(r or 0.0))

    return dict(
        balance=acct.balance,
        total_depositos=_s("deposito"), total_saques=_s("saque"),
        total_compras=_s("compra"),     total_vendas=_s("venda"),
        total_dividendos=_s("dividendo"), total_cupons=_s("cupom"),
        total_taxas=_s("taxa"),
    )


def get_transaction_history(db, portfolio_key, limit=None):
    acct = db.query(CashAccount).filter_by(portfolio_key=portfolio_key).first()
    if acct is None:
        return []
    q = (db.query(CashTransaction)
         .filter(CashTransaction.account_id == acct.id)
         .order_by(CashTransaction.date.desc(), CashTransaction.id.desc()))
    return q.limit(limit).all() if limit else q.all()


def get_movements(db, portfolio_key, limit=None):
    q = (db.query(PortfolioMovement)
         .filter(PortfolioMovement.portfolio_key == portfolio_key)
         .order_by(PortfolioMovement.date.desc(), PortfolioMovement.id.desc()))
    return q.limit(limit).all() if limit else q.all()


def get_running_balance_series(db, portfolio_key) -> pd.Series:
    acct = db.query(CashAccount).filter_by(portfolio_key=portfolio_key).first()
    if acct is None:
        return pd.Series(dtype=float)
    rows = (db.query(CashTransaction.date,
                     func.sum(CashTransaction.amount).label("s"))
            .filter(CashTransaction.account_id == acct.id)
            .group_by(CashTransaction.date)
            .order_by(CashTransaction.date).all())
    if not rows:
        return pd.Series(dtype=float)
    return pd.Series([r.s for r in rows],
                     index=[r.date for r in rows], dtype=float).cumsum()
