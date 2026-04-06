"""
Seed do portfólio Brazil a partir do estado histórico.

Reconstrói o ledger v2 a partir das alocações e snapshots existentes:
  2025-12-31: aporte R$1.000.000
  2026-01-02: compra IMAB11 80% + BOVA11 20%
  2026-01-05: realocação — vende 1% IMAB11, compra 1% INFLEET

Idempotente: limpa transactions/snapshots v2 antes de recriar.
NÃO toca nas tabelas legadas (allocations, prices, portfolio_snapshots).
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import date
from typing import Optional

from app.database import get_db_for
from app.models import (
    Portfolio, Allocation, Asset, AssetPrice,
    PortfolioSnapshot, Transaction,
    DailySnapshot, AssetSnapshot, Position,
)
from app.services.ledger import record_transaction
from app.engine.reprocessor import recompute_from

PORTFOLIO_KEY  = "brazil"
INITIAL_VALUE  = 1_000_000.0   # R$
APORTE_DATE    = date(2025, 12, 31)
FIRST_BUY_DATE = date(2026, 1, 2)
REALLOC_DATE   = date(2026, 1, 5)


def get_price(db, asset_id: int, on_date: date) -> Optional[float]:
    """Último preço disponível na ou antes da data."""
    row = (db.query(AssetPrice)
             .filter(AssetPrice.asset_id == asset_id,
                     AssetPrice.date <= on_date)
             .order_by(AssetPrice.date.desc())
             .first())
    return row.price if row else None


def nav_on(db, on_date: date, initial_value: float) -> float:
    """
    Converte o index_value do snapshot antigo em R$.
    nav_R$ = initial_value × (index_na_data / index_inicial)
    """
    first = (db.query(PortfolioSnapshot)
               .order_by(PortfolioSnapshot.date)
               .first())
    snap  = (db.query(PortfolioSnapshot)
               .filter(PortfolioSnapshot.date <= on_date)
               .order_by(PortfolioSnapshot.date.desc())
               .first())
    if first is None or snap is None or first.index_value == 0:
        return initial_value
    return initial_value * (snap.index_value / first.index_value)


def seed():
    db = get_db_for(PORTFOLIO_KEY)
    port = db.query(Portfolio).filter_by(key=PORTFOLIO_KEY).first()

    if port is None:
        print("ERRO: Portfolio 'brazil' não encontrado. Rode migrate_v2.py primeiro.")
        return

    pid = port.id
    print(f"\nSEED — {port.name}  (id={pid})")
    print("=" * 55)

    # ── 1. Atualiza base_date para 31/12/2025 ────────────────────────
    port.base_date    = APORTE_DATE
    port.initial_cash = 0.0
    db.flush()
    print(f"  ✓ base_date atualizado para {APORTE_DATE}")

    # ── 2. Limpa ledger v2 (idempotente) ─────────────────────────────
    deleted_tx  = db.query(Transaction).filter_by(portfolio_id=pid).delete(synchronize_session=False)
    deleted_ds  = db.query(DailySnapshot).filter_by(portfolio_id=pid).delete(synchronize_session=False)
    deleted_as_ = db.query(AssetSnapshot).filter_by(portfolio_id=pid).delete(synchronize_session=False)
    deleted_pos = db.query(Position).filter_by(portfolio_id=pid).delete(synchronize_session=False)
    db.flush()
    print(f"  ✓ Ledger v2 limpo ({deleted_tx} txs, "
          f"{deleted_ds} daily_snaps, {deleted_pos} positions)")

    # ── 3. APORTE em 31/12/2025 ──────────────────────────────────────
    record_transaction(
        db, pid, APORTE_DATE, "aporte", INITIAL_VALUE,
        description="Aporte inicial",
        recompute=False,
    )
    print(f"\n  📥 Aporte R${INITIAL_VALUE:,.0f} em {APORTE_DATE}")

    # ── 4. COMPRAS INICIAIS em 02/01/2026 ────────────────────────────
    print(f"\n  📈 Compras iniciais em {FIRST_BUY_DATE}:")

    first_allocs = (db.query(Allocation)
                      .filter_by(start_date=FIRST_BUY_DATE)
                      .all())

    for alloc in first_allocs:
        asset = alloc.asset
        price = get_price(db, asset.id, FIRST_BUY_DATE)
        if price is None:
            print(f"     ⚠ Sem preço para {asset.ticker} em {FIRST_BUY_DATE} — pulando")
            continue

        value = alloc.weight * INITIAL_VALUE
        qty   = value / price

        record_transaction(
            db, pid, FIRST_BUY_DATE, "compra",
            value=value,
            asset_id=asset.id,
            quantity=qty,
            price=price,
            description=f"Compra inicial {asset.ticker}",
            recompute=False,
        )
        print(f"     {asset.ticker:15s}  peso={alloc.weight:.0%}  "
              f"R${value:>12,.2f}  @{price:.4f}  qty={qty:.4f}")

    # ── 5. REALOCAÇÃO em 05/01/2026 ──────────────────────────────────
    nav_realloc = nav_on(db, REALLOC_DATE, INITIAL_VALUE)
    print(f"\n  🔄 Realocação em {REALLOC_DATE}  (NAV ref = R${nav_realloc:,.2f}):")

    prev_allocs = {
        a.asset_id: a.weight
        for a in db.query(Allocation).filter_by(start_date=FIRST_BUY_DATE).all()
    }
    curr_allocs = {
        a.asset_id: a.weight
        for a in db.query(Allocation).filter_by(start_date=REALLOC_DATE).all()
    }

    all_ids   = set(prev_allocs) | set(curr_allocs)
    sell_txs  = []
    buy_txs   = []

    for aid in all_ids:
        prev_w = prev_allocs.get(aid, 0.0)
        curr_w = curr_allocs.get(aid, 0.0)
        diff   = curr_w - prev_w

        if abs(diff) < 1e-6:
            continue

        asset = db.query(Asset).filter_by(id=aid).first()
        price = get_price(db, aid, REALLOC_DATE)
        if price is None:
            print(f"     ⚠ Sem preço para {asset.ticker} — pulando")
            continue

        amount = abs(diff) * nav_realloc
        qty    = amount / price

        if diff < 0:
            sell_txs.append((asset, amount, qty, price))
        else:
            buy_txs.append((asset, amount, qty, price))

    for asset, amount, qty, price in sell_txs:
        record_transaction(
            db, pid, REALLOC_DATE, "venda",
            value=amount, asset_id=asset.id,
            quantity=qty, price=price,
            description=f"Realocação venda {asset.ticker}",
            recompute=False,
        )
        print(f"     📉 VENDA  {asset.ticker:15s}  R${amount:>10,.2f}  "
              f"@{price:.4f}  qty={qty:.4f}")

    for asset, amount, qty, price in buy_txs:
        record_transaction(
            db, pid, REALLOC_DATE, "compra",
            value=amount, asset_id=asset.id,
            quantity=qty, price=price,
            description=f"Realocação compra {asset.ticker}",
            recompute=False,
        )
        print(f"     📈 COMPRA {asset.ticker:15s}  R${amount:>10,.2f}  "
              f"@{price:.4f}  qty={qty:.4f}")

    # ── 6. Reprocessa tudo ───────────────────────────────────────────
    print(f"\n  ⚙  Reprocessando desde {APORTE_DATE}…")
    recompute_from(db, pid, APORTE_DATE)

    db.commit()
    print("  ✓ Commit concluído")

    # ── 7. Resumo ────────────────────────────────────────────────────
    print("\n" + "=" * 55)
    print("  RESULTADO:")

    latest = (db.query(DailySnapshot)
                .filter_by(portfolio_id=pid)
                .order_by(DailySnapshot.date.desc())
                .first())
    if latest:
        print(f"  Data:             {latest.date}")
        print(f"  Caixa:            R${latest.cash:>14,.2f}")
        print(f"  Valor investido:  R${latest.invested_value:>14,.2f}")
        print(f"  NAV:              R${latest.nav:>14,.2f}")
        print(f"  Cota:             {latest.cota:>14.4f}")
        print(f"  Retorno acum.:    {(latest.cum_return or 0)*100:>13.2f}%")

    print()
    snaps_total = db.query(DailySnapshot).filter_by(portfolio_id=pid).count()
    pos_total   = db.query(Position).filter_by(portfolio_id=pid).count()
    print(f"  Snapshots diários gerados: {snaps_total}")
    print(f"  Registros de posição:      {pos_total}")
    print("=" * 55)

    db.close()


if __name__ == "__main__":
    seed()
