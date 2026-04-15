"""
Resumo semanal dos portfolios DOT via Slack.

Executado pelo GitHub Actions toda segunda-feira às 8h BRT.
Pode também ser rodado manualmente:
    python scripts/send_weekly_email.py

Variáveis de ambiente necessárias:
    GLOBAL_DATABASE_URL  — connection string Supabase Global
    BRAZIL_DATABASE_URL  — connection string Supabase Brazil
    SLACK_WEBHOOK_URL    — URL do Incoming Webhook do Slack
"""

import os
import sys
import json
import requests
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from app.database import get_db_for
from app.portfolio import performance
from app.portfolio.combined import (
    compute_dot_series,
    compute_blended_benchmark,
    _returns_from_series,
)
from app.market_data.cpi_provider  import fetch_cpi_daily
from app.market_data.cdi_provider  import fetch_cdi_daily
from app.market_data.ipca_provider import fetch_ipca_daily
from app.config import PORTFOLIOS


# ── Helpers ───────────────────────────────────────────────────────────────────

def _pct(v, decimals=2):
    if v is None:
        return "—"
    sign = "+" if v >= 0 else ""
    return f"{sign}{v * 100:.{decimals}f}%"


def _pp(portfolio_ret, bench_ret):
    if portfolio_ret is None or bench_ret is None:
        return None
    return portfolio_ret - bench_ret


def _pp_str(pp_val, label):
    if pp_val is None:
        return ""
    sign  = "+" if pp_val >= 0 else ""
    arrow = "▲" if pp_val >= 0 else "▼"
    return f"   {arrow} {sign}{pp_val*100:.2f}pp vs {label}\n"


def _apply_multiplier(series, multiplier):
    if multiplier == 1.0 or series.empty:
        return series
    daily  = series.pct_change().fillna(0.0)
    result = (1.0 + daily * multiplier).cumprod()
    return result / result.iloc[0]


def _bench_total(series):
    if series.empty:
        return None
    return series.iloc[-1] - 1.0


# ── Coleta de dados ───────────────────────────────────────────────────────────

def collect_global():
    cfg = PORTFOLIOS["global"]
    db  = get_db_for("global")
    try:
        tot      = performance.total_return(db)
        ytd      = performance.ytd_return(db)
        mtd      = performance.mtd_return(db)
        weekly   = performance.weekly_return(db)
        daily    = performance.latest_daily_return(db)
        last_date = performance.latest_snapshot_date(db)
    finally:
        db.close()

    start = cfg["start_date"]
    ref   = last_date or date.today()
    cpi   = fetch_cpi_daily(date.fromisoformat(start), ref)
    cpi15 = _apply_multiplier(cpi, 1.5)

    return dict(
        tot=tot, ytd=ytd, mtd=mtd, weekly=weekly, daily=daily,
        last_date=last_date,
        cpi_tot=_bench_total(cpi),
        cpi15_tot=_bench_total(cpi15),
    )


def collect_brazil():
    cfg = PORTFOLIOS["brazil"]
    db  = get_db_for("brazil")
    try:
        tot       = performance.total_return(db)
        ytd       = performance.ytd_return(db)
        mtd       = performance.mtd_return(db)
        weekly    = performance.weekly_return(db)
        daily     = performance.latest_daily_return(db)
        last_date = performance.latest_snapshot_date(db)
    finally:
        db.close()

    start  = cfg["start_date"]
    ref    = last_date or date.today()
    cdi    = fetch_cdi_daily(date.fromisoformat(start), ref)
    ipca   = fetch_ipca_daily(date.fromisoformat(start), ref)
    ipca15 = _apply_multiplier(ipca, 1.5)

    return dict(
        tot=tot, ytd=ytd, mtd=mtd, weekly=weekly, daily=daily,
        last_date=last_date,
        cdi_tot=_bench_total(cdi),
        ipca_tot=_bench_total(ipca),
        ipca15_tot=_bench_total(ipca15),
    )


def collect_dot():
    dot_cfg    = PORTFOLIOS["dot"]
    w_g        = dot_cfg.get("w_global", 0.5)
    w_b        = dot_cfg.get("w_brazil", 0.5)
    mult       = dot_cfg.get("bench_multiplier", 1.5)
    rebal_freq = dot_cfg.get("rebal_freq", "annual")

    dot   = compute_dot_series(w_global=w_g, w_brazil=w_b, rebal_freq=rebal_freq)
    bench = compute_blended_benchmark(w_global=w_g, w_brazil=w_b, multiplier=mult)
    stats = _returns_from_series(dot)

    return dict(
        tot=stats["total"], mtd=stats["mtd"],
        ytd=stats["ytd"],   weekly=stats["weekly"], daily=stats["daily"],
        bench_tot=_bench_total(bench),
        w_global=w_g, w_brazil=w_b,
        rebal_freq=rebal_freq,
        bench_label=dot_cfg.get("bench_label", "1.5× CPI+IPCA"),
    )


# ── Monta mensagem Slack (Block Kit) ──────────────────────────────────────────

def _emoji_ret(v):
    if v is None: return ""
    return " 🟢" if v >= 0 else " 🔴"


def _portfolio_block(flag, name, currency, data, bench_lines):
    tot_str   = _pct(data["tot"])
    mtd_str   = _pct(data["mtd"])
    ytd_str   = _pct(data["ytd"])
    daily_str = _pct(data["daily"])

    weekly_str = _pct(data.get("weekly"))
    text = (
        f"*{flag} {name}* _{currency}_\n"
        f"```"
        f"Daily            {daily_str}{_emoji_ret(data['daily'])}\n"
        f"Weekly           {weekly_str}{_emoji_ret(data.get('weekly'))}\n"
        f"MTD              {mtd_str}{_emoji_ret(data['mtd'])}\n"
        f"YTD              {ytd_str}{_emoji_ret(data['ytd'])}\n"
        f"Since Inception  {tot_str}{_emoji_ret(data['tot'])}\n"
        f"```"
        f"{bench_lines}"
    )
    return {"type": "section", "text": {"type": "mrkdwn", "text": text}}


def build_slack_payload(g, b, d, report_date):
    week_str = report_date.strftime("%d/%m/%Y")
    prev_str = (report_date - timedelta(days=7)).strftime("%d/%m/%Y")

    # Benchmarks Global
    g_bench = (
        _pp_str(_pp(g["tot"], g["cpi_tot"]),   "CPI EUA acumulado")
        + _pp_str(_pp(g["tot"], g["cpi15_tot"]), "1.5× CPI EUA")
    )
    # Benchmarks Brazil
    b_bench = (
        _pp_str(_pp(b["tot"], b["cdi_tot"]),    "CDI acumulado")
        + _pp_str(_pp(b["tot"], b["ipca_tot"]),   "IPCA acumulado")
        + _pp_str(_pp(b["tot"], b["ipca15_tot"]), "1.5× IPCA")
    )
    # Benchmark DOT
    dot_bench = _pp_str(_pp(d["tot"], d["bench_tot"]), d["bench_label"])
    _rebal_label = {"daily": "Diário", "monthly": "Mensal", "annual": "Anual"}
    dot_comp  = (
        f"_Composição: {int(d['w_global']*100)}% Global / "
        f"{int(d['w_brazil']*100)}% Brazil · "
        f"Rebal. {_rebal_label.get(d['rebal_freq'], d['rebal_freq'])}_"
    )

    blocks = [
        # Data de referência
        {
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"📅 Dados de referência: *{week_str}*"}]
        },
        {"type": "divider"},

        # DOT
        _portfolio_block("⬤", "DOT Portfolio", "USD", d,
                         dot_bench + dot_comp),
        {"type": "divider"},

        # Global
        _portfolio_block("🌍", "Global Portfolio", "USD", g, g_bench),
        {"type": "divider"},

        # Brazil
        _portfolio_block("🇧🇷", "Brazil Portfolio", "BRL", b, b_bench),
    ]

    return {
        "username":   "DOT Portfolio",
        "icon_emoji": ":bar_chart:",
        "blocks": blocks,
    }


# ── Envio ─────────────────────────────────────────────────────────────────────

def send_slack(payload: dict):
    webhook_url = os.environ["SLACK_WEBHOOK_URL"]
    resp = requests.post(
        webhook_url,
        data=json.dumps(payload),
        headers={"Content-Type": "application/json"},
        timeout=15,
    )
    resp.raise_for_status()
    print("✅ Mensagem enviada ao Slack!")


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"📊 Coletando dados...")

    g = collect_global()
    b = collect_brazil()
    d = collect_dot()

    # Use the most recent snapshot date across all portfolios
    candidates = [g.get("last_date"), b.get("last_date")]
    report_date = max((dt for dt in candidates if dt is not None), default=date.today())
    print(f"📅 Data de referência: {report_date}")

    # Abort if data is stale (more than 3 calendar days old)
    staleness = (date.today() - report_date).days
    if staleness > 3:
        print(
            f"⚠️  Dados desatualizados ({report_date}, {staleness} dias atrás). "
            f"Verifique o processo de atualização de snapshots. Envio cancelado."
        )
        sys.exit(0)

    print("💬 Enviando para Slack...")
    payload = build_slack_payload(g, b, d, report_date)
    send_slack(payload)
