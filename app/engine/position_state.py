"""
Máquina de estado de posições.

Recebe uma lista ORDENADA de transações (por date, id) e um estado inicial,
retorna o estado final (posições, caixa, fluxo líquido do dia alvo).

Completamente sem IO — recebe dicts, retorna dicts.
"""

from __future__ import annotations

from datetime import date as Date
from typing import Optional

from app.engine.calculator import (
    PositionState,
    apply_buy, apply_sell, apply_amortization,
    apply_dividend, apply_qty_adjustment,
)


# Tipos que movem o caixa mas não criam posição
_CASH_ONLY_TYPES = {"aporte", "retirada", "despesa"}

# Tipos de renda (acumulam em dividends da posição)
_INCOME_TYPES = {"dividendo", "cupom"}

# Tipos que contribuem para fluxo líquido (para cálculo de cota ex-fluxo)
_INFLOW_TYPES  = {"aporte"}
_OUTFLOW_TYPES = {"retirada"}


def process_transactions(
    transactions: list[dict],
    initial_cash: float,
    initial_positions: dict[int, PositionState],
    target_date: Optional[Date] = None,
) -> tuple[dict[int, PositionState], float, float]:
    """
    Processa transações em ordem e retorna:
        (posições_finais, caixa_final, fluxo_líquido)

    fluxo_líquido = Σ(aportes) − Σ(retiradas) — somente no target_date se informado.
    Usado pelo reprocessor para calcular a cota ex-fluxo de cada dia.

    Transações com is_void=True são ignoradas.
    """
    # Cópia profunda das posições iniciais
    positions: dict[int, PositionState] = {
        aid: PositionState(
            asset_id=aid,
            quantity=p.quantity,
            avg_cost=p.avg_cost,
            realized_pnl=p.realized_pnl,
            dividends=p.dividends,
        )
        for aid, p in initial_positions.items()
    }

    cash     = initial_cash
    net_flow = 0.0

    for tx in transactions:
        if tx.get("is_void"):
            continue

        tx_type  = tx["type"]
        tx_date  = tx["date"]
        asset_id = tx.get("asset_id")
        qty      = tx.get("quantity") or 0.0
        price    = tx.get("price")    or 0.0
        value    = tx["value"]

        # ── 1. Impacto no caixa ──────────────────────────────────────────
        cash += tx["cash_impact"]

        # ── 2. Fluxo líquido (para cota ex-fluxo) ────────────────────────
        if target_date is None or tx_date == target_date:
            if tx_type in _INFLOW_TYPES:
                net_flow += value
            elif tx_type in _OUTFLOW_TYPES:
                net_flow -= value

        # ── 3. Posições ───────────────────────────────────────────────────
        if tx_type in _CASH_ONLY_TYPES:
            continue   # sem posição envolvida

        if tx_type in {"ajuste_preco"}:
            continue   # apenas atualiza preço, não posição

        if asset_id is None:
            continue   # segurança: tipos com ativo devem ter asset_id

        pos = positions.get(asset_id, PositionState(asset_id=asset_id))

        if tx_type == "compra":
            positions[asset_id] = apply_buy(pos, qty, price)

        elif tx_type == "venda":
            positions[asset_id] = apply_sell(pos, qty, price)

        elif tx_type in _INCOME_TYPES:
            positions[asset_id] = apply_dividend(pos, value)

        elif tx_type == "amortizacao":
            positions[asset_id] = apply_amortization(pos, qty, price)

        elif tx_type == "ajuste_qty":
            # tx.quantity = nova quantidade absoluta
            new_avg = tx.get("price")   # price usado como novo avg_cost se informado
            positions[asset_id] = apply_qty_adjustment(pos, qty, new_avg)

    return positions, cash, net_flow
