"""
Funções puras de cálculo financeiro — sem IO, sem banco, 100% testáveis.

Convenções:
  - value/amount: sempre positivo; a direção está no tipo da transação.
  - cash_impact:  + = caixa recebe, − = caixa paga.
  - avg_cost:     custo médio por unidade (FIFO não implementado, apenas CMM).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


# ─────────────────────────────────────────────────────────────────────────────
# Estado de posição por ativo
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class PositionState:
    asset_id:     int
    quantity:     float = 0.0
    avg_cost:     float = 0.0
    realized_pnl: float = 0.0
    dividends:    float = 0.0


# ─────────────────────────────────────────────────────────────────────────────
# Mutações de posição
# ─────────────────────────────────────────────────────────────────────────────

def apply_buy(pos: PositionState, qty: float, price: float) -> PositionState:
    """
    Compra: aumenta quantidade, recalcula custo médio ponderado.
        new_avg = (old_qty × old_avg + qty × price) / new_qty
    """
    if qty <= 0 or price < 0:
        raise ValueError(f"Compra inválida: qty={qty}, price={price}")
    new_qty = pos.quantity + qty
    new_avg = (pos.quantity * pos.avg_cost + qty * price) / new_qty
    return PositionState(
        asset_id=pos.asset_id,
        quantity=new_qty,
        avg_cost=new_avg,
        realized_pnl=pos.realized_pnl,
        dividends=pos.dividends,
    )


def apply_sell(pos: PositionState, qty: float, price: float) -> PositionState:
    """
    Venda: reduz quantidade, realiza PnL pelo custo médio.
    Custo médio NÃO muda na venda.
        realized_pnl += qty × (price − avg_cost)
    """
    if qty <= 0:
        raise ValueError(f"Venda inválida: qty={qty}")
    if qty > pos.quantity + 1e-9:
        raise ValueError(
            f"Posição insuficiente para vender {qty:.4f} "
            f"(posição atual: {pos.quantity:.4f})"
        )
    realized = qty * (price - pos.avg_cost)
    new_qty   = max(pos.quantity - qty, 0.0)
    new_avg   = pos.avg_cost if new_qty > 1e-9 else 0.0
    return PositionState(
        asset_id=pos.asset_id,
        quantity=new_qty,
        avg_cost=new_avg,
        realized_pnl=pos.realized_pnl + realized,
        dividends=pos.dividends,
    )


def apply_amortization(pos: PositionState,
                        qty: float, price: float) -> PositionState:
    """
    Amortização: reduz quantidade e retorna capital ao custo médio.
    Não gera lucro — é retorno de principal, não de retorno.
    """
    if qty > pos.quantity + 1e-9:
        raise ValueError(f"Amortização ({qty}) maior que posição ({pos.quantity}).")
    # Retorno de capital ao preço original (avg_cost), não ao preço de mercado
    realized = qty * pos.avg_cost
    new_qty  = max(pos.quantity - qty, 0.0)
    new_avg  = pos.avg_cost if new_qty > 1e-9 else 0.0
    return PositionState(
        asset_id=pos.asset_id,
        quantity=new_qty,
        avg_cost=new_avg,
        realized_pnl=pos.realized_pnl + realized,
        dividends=pos.dividends,
    )


def apply_dividend(pos: PositionState, value: float) -> PositionState:
    """Dividendo/cupom: acumula em dividends. Posição e custo inalterados."""
    return PositionState(
        asset_id=pos.asset_id,
        quantity=pos.quantity,
        avg_cost=pos.avg_cost,
        realized_pnl=pos.realized_pnl,
        dividends=pos.dividends + value,
    )


def apply_qty_adjustment(pos: PositionState,
                          new_qty: float, new_avg: Optional[float] = None) -> PositionState:
    """Ajuste manual de quantidade (correção de lançamento)."""
    return PositionState(
        asset_id=pos.asset_id,
        quantity=new_qty,
        avg_cost=new_avg if new_avg is not None else pos.avg_cost,
        realized_pnl=pos.realized_pnl,
        dividends=pos.dividends,
    )


# ─────────────────────────────────────────────────────────────────────────────
# NAV e Cota
# ─────────────────────────────────────────────────────────────────────────────

def compute_nav(cash: float,
                positions: dict[int, PositionState],
                prices: dict[int, float]) -> float:
    """
    NAV = caixa + Σ(qty_i × price_i)

    Ativos sem preço disponível são ignorados no cálculo
    (situação que deve ser evitada via inserção manual de preço).
    """
    invested = sum(
        pos.quantity * prices[aid]
        for aid, pos in positions.items()
        if pos.quantity > 1e-9 and aid in prices
    )
    return cash + invested


def compute_cota(
    cota_prev: float,
    nav_open: float,
    nav_close: float,
    net_flow: float,
) -> tuple[float, float]:
    """
    Cota ex-fluxo — isola performance de aportes/retiradas.

    Fórmula:
        retorno = (nav_close − net_flow) / nav_open − 1
        cota    = cota_prev × (1 + retorno)

    Onde:
        net_flow  = aportes − retiradas do dia (positivo = entrada líquida)
        nav_open  = NAV de fechamento do dia anterior (= abertura do dia atual)
        nav_close = NAV de fechamento do dia atual (já com preços marcados)

    Retorna (nova_cota, retorno_do_dia).
    """
    if nav_open <= 1e-9:
        # D0 ou portfólio zerado — sem histórico anterior
        return cota_prev, 0.0

    retorno = (nav_close - net_flow) / nav_open - 1.0
    return cota_prev * (1.0 + retorno), retorno


# ─────────────────────────────────────────────────────────────────────────────
# Snapshots por ativo
# ─────────────────────────────────────────────────────────────────────────────

def compute_unrealized_pnl(pos: PositionState,
                            price: Optional[float]) -> float:
    if price is None or pos.quantity <= 1e-9:
        return 0.0
    return pos.quantity * (price - pos.avg_cost)


def compute_asset_snapshot(pos: PositionState,
                            price: Optional[float]) -> dict:
    """
    Retorna dict pronto para salvar em AssetSnapshot.
    """
    unrealized = compute_unrealized_pnl(pos, price)
    market_val = pos.quantity * price if (price is not None and pos.quantity > 1e-9) else 0.0
    return {
        "quantity":       pos.quantity,
        "avg_cost":       pos.avg_cost,
        "price":          price,
        "market_value":   market_val,
        "unrealized_pnl": unrealized,
        "realized_pnl":   pos.realized_pnl,
        "dividends":      pos.dividends,
        "total_pnl":      unrealized + pos.realized_pnl + pos.dividends,
    }
