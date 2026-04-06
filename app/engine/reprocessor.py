"""
Motor de reprocessamento — reconstrói todo o estado do portfólio
a partir de um ponto no tempo, reproduzindo o ledger de transações.

Estratégia:
  1. Carrega estado imediatamente ANTES de from_date (posições + caixa + cota)
  2. Itera dia a dia até to_date
  3. Para cada dia:
       a. Processa transações do dia (position_state)
       b. Marca preços (asset_prices)
       c. Calcula NAV = cash + Σ(qty × price)
       d. Calcula cota ex-fluxo
       e. Grava daily_snapshot, positions, asset_snapshots
  4. Os snapshots antigos do período são deletados antes de regravar (idempotente)

Complexidade: O(N_dias × N_ativos) — aceitável para portfólios com poucos anos.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Optional

from sqlalchemy.orm import Session

from app.engine.calculator import (
    PositionState,
    compute_nav, compute_cota, compute_asset_snapshot,
)
from app.engine.position_state import process_transactions
from app.models import (
    Portfolio, Transaction, AssetPrice,
    Position, DailySnapshot, AssetSnapshot, Asset,
    PortfolioSnapshot as LegacySnapshot,
)

# Convenção: ativo com data_source="cash_account" é a Conta Corrente.
# O reprocessador espelha o saldo de caixa como posição deste ativo (qty = saldo, price = 1.00).
_CASH_ACCOUNT_SOURCE = "cash_account"

logger = logging.getLogger(__name__)


def get_portfolio(db: Session, portfolio_id: int) -> Portfolio:
    p = db.query(Portfolio).filter_by(id=portfolio_id).first()
    if p is None:
        raise ValueError(f"Portfólio id={portfolio_id} não encontrado.")
    return p


def _load_state_before(
    db: Session,
    portfolio_id: int,
    before_date: date,
    base_date: date,
    initial_cash: float,
    cota_base: float,
) -> tuple[dict[int, PositionState], float, float, float]:
    """
    Retorna (posições, caixa, cota, nav) do dia imediatamente anterior a before_date.
    Se before_date == base_date, retorna estado inicial zerado.
    """
    prev_date = before_date - timedelta(days=1)

    if prev_date < base_date:
        return {}, initial_cash, cota_base, initial_cash

    snap = (db.query(DailySnapshot)
              .filter_by(portfolio_id=portfolio_id, date=prev_date)
              .first())

    if snap is None:
        # Snapshot não existe — vai ao início total
        return {}, initial_cash, cota_base, initial_cash

    prev_positions_db = (db.query(Position)
                           .filter_by(portfolio_id=portfolio_id, date=prev_date)
                           .all())
    pos_state = {
        p.asset_id: PositionState(
            asset_id=p.asset_id,
            quantity=p.quantity,
            avg_cost=p.avg_cost,
            realized_pnl=p.realized_pnl,
            dividends=p.dividends,
        )
        for p in prev_positions_db
    }

    return pos_state, snap.cash, snap.cota, snap.nav


def _clear_range(db: Session, portfolio_id: int,
                 from_date: date, to_date: date) -> None:
    """Deleta snapshots e posições do intervalo para regravar de forma idempotente."""
    for model in (DailySnapshot, AssetSnapshot, Position):
        (db.query(model)
           .filter(
               model.portfolio_id == portfolio_id,
               model.date >= from_date,
               model.date <= to_date,
           )
           .delete(synchronize_session=False))
    db.flush()


def recompute_from(
    db: Session,
    portfolio_id: int,
    from_date: date,
    to_date: Optional[date] = None,
) -> None:
    """
    Reprocessa o portfólio a partir de from_date.

    Pode ser chamado:
      - Após inserir/anular transação em from_date
      - Após alterar preço em from_date
      - Força rebuild total chamando com from_date=portfolio.base_date
    """
    portfolio = get_portfolio(db, portfolio_id)
    to_date   = to_date   or date.today()
    from_date = max(from_date, portfolio.base_date)

    logger.info("Reprocessando portfólio=%d de %s até %s",
                portfolio_id, from_date, to_date)

    # Detecta ativo de Conta Corrente (se existir)
    cash_account_asset = (
        db.query(Asset)
          .filter_by(portfolio_id=portfolio_id,
                     data_source=_CASH_ACCOUNT_SOURCE,
                     is_active=True)
          .first()
    )

    # ── 1. Estado inicial (antes de from_date) ────────────────────────────
    pos_state, cash, cota, nav_prev = _load_state_before(
        db, portfolio_id, from_date,
        portfolio.base_date, portfolio.initial_cash, portfolio.cota_base,
    )

    # Se há snapshot faltando antes do from_date, recomeça do início
    if from_date > portfolio.base_date and not pos_state and cash == portfolio.initial_cash:
        prev = (db.query(DailySnapshot)
                  .filter_by(portfolio_id=portfolio_id,
                              date=from_date - timedelta(days=1))
                  .first())
        if prev is None:
            from_date = portfolio.base_date
            pos_state, cash, cota, nav_prev = {}, portfolio.initial_cash, portfolio.cota_base, portfolio.initial_cash

    # ── 2. Limpar período a recalcular ────────────────────────────────────
    _clear_range(db, portfolio_id, from_date, to_date)

    # ── 3. Carregar transações do período ─────────────────────────────────
    all_txs = (
        db.query(Transaction)
          .filter(
              Transaction.portfolio_id == portfolio_id,
              Transaction.date >= from_date,
              Transaction.date <= to_date,
              Transaction.is_void == False,
          )
          .order_by(Transaction.date, Transaction.id)
          .all()
    )

    # Agrupar por data
    txs_by_date: dict[date, list] = {}
    for tx in all_txs:
        txs_by_date.setdefault(tx.date, []).append({
            "type":        tx.type,
            "date":        tx.date,
            "asset_id":    tx.asset_id,
            "quantity":    tx.quantity,
            "price":       tx.price,
            "value":       tx.value,
            "cash_impact": tx.cash_impact,
            "is_void":     tx.is_void,
        })

    # ── 4. Replay dia a dia ───────────────────────────────────────────────
    current_cash  = cash
    current_cota  = cota
    current_nav   = nav_prev
    current_pos   = pos_state

    d = from_date
    while d <= to_date:
        day_txs = txs_by_date.get(d, [])

        # Processa transações do dia
        new_positions, new_cash, net_flow = process_transactions(
            transactions=day_txs,
            initial_cash=current_cash,
            initial_positions=current_pos,
            target_date=d,
        )

        # Buscar preços do dia para ativos com posição > 0
        active_ids = {aid for aid, pos in new_positions.items()
                      if pos.quantity > 1e-9}

        prices: dict[int, float] = {}
        if active_ids:
            price_rows = (
                db.query(AssetPrice)
                  .filter(
                      AssetPrice.asset_id.in_(active_ids),
                      AssetPrice.date == d,
                  )
                  .all()
            )
            prices = {p.asset_id: p.price for p in price_rows}

            # Fallback 1: último preço disponível antes do dia atual
            missing = active_ids - set(prices.keys())
            for aid in missing:
                last = (db.query(AssetPrice)
                          .filter(AssetPrice.asset_id == aid,
                                  AssetPrice.date < d)
                          .order_by(AssetPrice.date.desc())
                          .first())
                if last:
                    prices[aid] = last.price

            # Fallback 2: preço da transação do dia (compra/venda)
            # Evita PnL artificial em ativos sem cotação externa no dia da compra.
            # Garante: NAV após compra = caixa gasto (sem perda/ganho no D0 do ativo).
            missing2 = active_ids - set(prices.keys())
            if missing2:
                tx_prices: dict[int, float] = {}
                for tx in day_txs:
                    aid = tx.get("asset_id")
                    if aid in missing2 and tx.get("price") and tx["price"] > 0:
                        tx_prices[aid] = tx["price"]
                prices.update(tx_prices)

        # NAV de fechamento
        nav_close = compute_nav(new_cash, new_positions, prices)

        # Cota ex-fluxo
        new_cota, daily_ret = compute_cota(
            cota_prev=current_cota,
            nav_open=current_nav,
            nav_close=nav_close,
            net_flow=net_flow,
        )

        cum_ret = new_cota / portfolio.cota_base - 1.0
        invested = nav_close - new_cash

        # ── Gravar DailySnapshot ─────────────────────────────────────────
        db.add(DailySnapshot(
            portfolio_id   = portfolio_id,
            date           = d,
            cash           = new_cash,
            invested_value = max(invested, 0.0),
            nav            = nav_close,
            cota           = new_cota,
            net_flow       = net_flow,
            daily_return   = daily_ret,
            cum_return     = cum_ret,
        ))

        # ── Gravar Positions ─────────────────────────────────────────────
        # Exclui o cash_account do loop normal — é gravado separadamente abaixo
        ca_id_excl = cash_account_asset.id if cash_account_asset is not None else None
        for aid, pos in new_positions.items():
            if aid == ca_id_excl:
                continue
            if pos.quantity > 1e-9 or pos.realized_pnl != 0 or pos.dividends != 0:
                db.add(Position(
                    portfolio_id = portfolio_id,
                    asset_id     = aid,
                    date         = d,
                    quantity     = pos.quantity,
                    avg_cost     = pos.avg_cost,
                    realized_pnl = pos.realized_pnl,
                    dividends    = pos.dividends,
                ))

        # ── Gravar AssetSnapshots (ativos normais) ───────────────────────
        for aid, pos in new_positions.items():
            if aid == ca_id_excl:
                continue
            snap_data = compute_asset_snapshot(pos, prices.get(aid))
            if snap_data["quantity"] > 1e-9 or snap_data["total_pnl"] != 0:
                db.add(AssetSnapshot(
                    portfolio_id = portfolio_id,
                    asset_id     = aid,
                    date         = d,
                    **snap_data,
                ))

        # ── Conta Corrente: espelha saldo de caixa como posição ──────────
        # O saldo de caixa (new_cash) é registrado como posição do ativo
        # data_source="cash_account" (preço = R$1,00 fixo, qty = saldo).
        # Isto aparece no bucket Liquidez sem alterar o NAV (que já inclui cash).
        if cash_account_asset is not None:
            ca_id = cash_account_asset.id
            db.add(Position(
                portfolio_id = portfolio_id,
                asset_id     = ca_id,
                date         = d,
                quantity     = new_cash,
                avg_cost     = 1.0,
                realized_pnl = 0.0,
                dividends    = 0.0,
            ))
            db.add(AssetSnapshot(
                portfolio_id   = portfolio_id,
                asset_id       = ca_id,
                date           = d,
                quantity       = new_cash,
                avg_cost       = 1.0,
                price          = 1.0,
                market_value   = new_cash,
                unrealized_pnl = 0.0,
                realized_pnl   = 0.0,
                dividends      = 0.0,
                total_pnl      = 0.0,
            ))

        # Avançar estado
        current_cash = new_cash
        current_cota = new_cota
        current_nav  = nav_close
        current_pos  = new_positions
        d += timedelta(days=1)

    db.flush()
    logger.info("Reprocessamento concluído: portfólio=%d, %s → %s",
                portfolio_id, from_date, to_date)

    # ── Sync bridge: espelha DailySnapshot → portfolio_snapshots (legado) ──
    # O Dashboard ainda lê PortfolioSnapshot. Este bloco mantém as duas
    # tabelas sincronizadas sem exigir refatoração do dashboard.
    _sync_to_legacy_snapshots(db, portfolio_id, from_date, to_date)


def _sync_to_legacy_snapshots(
    db: Session,
    portfolio_id: int,
    from_date: date,
    to_date: Optional[date] = None,
) -> None:
    """
    Copia DailySnapshot → PortfolioSnapshot (tabela legada do dashboard).

    Mapeamento:
      DailySnapshot.cota          → PortfolioSnapshot.index_value
      DailySnapshot.daily_return  → PortfolioSnapshot.daily_return

    Idempotente: faz upsert (atualiza se existe, insere se não existe).
    """
    end = to_date or date.today()

    new_snaps = (
        db.query(DailySnapshot)
          .filter(
              DailySnapshot.portfolio_id == portfolio_id,
              DailySnapshot.date >= from_date,
              DailySnapshot.date <= end,
          )
          .order_by(DailySnapshot.date)
          .all()
    )

    for s in new_snaps:
        existing = db.query(LegacySnapshot).filter_by(date=s.date).first()
        if existing:
            existing.index_value  = s.cota
            existing.daily_return = s.daily_return
        else:
            db.add(LegacySnapshot(
                date         = s.date,
                index_value  = s.cota,
                daily_return = s.daily_return,
            ))

    db.flush()
    logger.info("Sync legacy snapshots: %d registros, %s → %s",
                len(new_snaps), from_date, end)
