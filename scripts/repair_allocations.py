"""
Repair script: enforce exactly one active allocation row per active asset.

Usage:
    python3 scripts/repair_allocations.py              # repara Global
    python3 scripts/repair_allocations.py brazil       # repara Brazil
    python3 scripts/repair_allocations.py --all        # repara todos
    python3 scripts/repair_allocations.py --dry-run    # só mostra, não altera

What it does:
  1. For every asset with more than one open allocation, closes all but the
     most recent one (highest start_date, then highest id).
  2. Prints a summary of current active allocations after repair.
  3. Exits with code 1 if weights don't sum to ~1.0.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.config import PORTFOLIOS
from app.database import get_db_for
from app.models import Allocation, Asset


def repair(portfolio_key: str, dry_run: bool = False) -> bool:
    print(f"\n{'─'*50}")
    print(f"Portfolio: {PORTFOLIOS[portfolio_key]['name']}")
    print(f"{'─'*50}")

    db = get_db_for(portfolio_key)
    try:
        open_allocs = (
            db.query(Allocation)
            .filter(Allocation.end_date.is_(None))
            .order_by(Allocation.asset_id, Allocation.start_date.desc(), Allocation.id.desc())
            .all()
        )

        by_asset: dict[int, list[Allocation]] = {}
        for a in open_allocs:
            by_asset.setdefault(a.asset_id, []).append(a)

        fixed = 0
        for asset_id, rows in by_asset.items():
            asset = db.query(Asset).filter_by(id=asset_id).first()
            ticker = asset.ticker if asset else str(asset_id)
            if len(rows) <= 1:
                continue
            keeper = rows[0]
            for dup in rows[1:]:
                print(f"  CLOSE duplicate: {ticker} id={dup.id} weight={dup.weight:.4f} start={dup.start_date}")
                if not dry_run:
                    dup.end_date = keeper.start_date
                fixed += 1

        if fixed == 0:
            print("  No duplicates found — allocations are clean.")
        else:
            if not dry_run:
                db.commit()
            print(f"\n  {'Would close' if dry_run else 'Closed'} {fixed} duplicate row(s).")

        # Re-query after fix
        open_allocs = (
            db.query(Allocation)
            .filter(Allocation.end_date.is_(None))
            .order_by(Allocation.asset_id, Allocation.start_date.desc())
            .all()
        )

        print("\n  Active allocations after repair:")
        print(f"  {'Ticker':<14} {'Weight':>8}  Start")
        print(f"  {'-'*14} {'-'*8}  {'-'*12}")
        total = 0.0
        seen = set()
        for row in sorted(open_allocs, key=lambda x: -x.weight):
            if row.asset_id in seen:
                continue
            seen.add(row.asset_id)
            asset = db.query(Asset).filter_by(id=row.asset_id).first()
            ticker = asset.ticker if asset else str(row.asset_id)
            print(f"  {ticker:<14} {row.weight*100:>7.2f}%  {row.start_date}")
            total += row.weight

        print(f"\n  {'TOTAL':<14} {total*100:>7.2f}%")

        if abs(total - 1.0) > 0.005:
            print(f"\n  ⚠ WARNING: weights sum to {total*100:.2f}%, not 100%.")
            return False
        return True

    finally:
        db.close()


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    dry_run = "--dry-run" in sys.argv
    run_all = "--all" in sys.argv

    if dry_run:
        print("DRY RUN — no changes will be written.")

    if run_all:
        keys = list(PORTFOLIOS.keys())
    elif args:
        key = args[0]
        if key not in PORTFOLIOS:
            print(f"Unknown portfolio '{key}'. Available: {list(PORTFOLIOS.keys())}")
            sys.exit(1)
        keys = [key]
    else:
        keys = ["global"]

    ok = all(repair(k, dry_run=dry_run) for k in keys)
    sys.exit(0 if ok else 1)
