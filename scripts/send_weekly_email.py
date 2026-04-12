"""
Resumo semanal dos portfolios DOT via email.

Executado pelo GitHub Actions toda segunda-feira às 8h BRT.
Pode também ser rodado manualmente:
    python scripts/send_weekly_email.py

Variáveis de ambiente necessárias:
    GLOBAL_DATABASE_URL  — connection string Supabase Global
    BRAZIL_DATABASE_URL  — connection string Supabase Brazil
    EMAIL_SENDER         — endereço Gmail que envia
    EMAIL_PASSWORD       — App Password do Gmail (não a senha normal)
    EMAIL_RECIPIENTS     — emails separados por vírgula
"""

import os
import sys
import smtplib
import ssl
from datetime import date, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

# Garante que o root do projeto está no path
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


def _pp_html(pp_val, label):
    if pp_val is None:
        return ""
    sign  = "+" if pp_val >= 0 else ""
    color = "#16a34a" if pp_val >= 0 else "#dc2626"
    arrow = "▲" if pp_val >= 0 else "▼"
    return (
        f"<tr>"
        f"<td style='padding:2px 0;color:#6b7280;font-size:13px'>vs {label}</td>"
        f"<td style='padding:2px 0;text-align:right;font-weight:600;"
        f"color:{color};font-size:13px'>{arrow} {sign}{pp_val*100:.2f}pp</td>"
        f"</tr>"
    )


def _apply_multiplier(series, multiplier):
    if multiplier == 1.0 or series.empty:
        return series
    import pandas as pd
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
        tot   = performance.total_return(db)
        mtd   = performance.mtd_return(db)
        ytd   = performance.ytd_return(db)
        daily = performance.latest_daily_return(db)
    finally:
        db.close()

    start = cfg["start_date"]
    cpi   = fetch_cpi_daily(date.fromisoformat(start), date.today())
    cpi15 = _apply_multiplier(cpi, 1.5)

    return dict(
        tot=tot, mtd=mtd, ytd=ytd, daily=daily,
        cpi_tot=_bench_total(cpi),
        cpi15_tot=_bench_total(cpi15),
    )


def collect_brazil():
    cfg = PORTFOLIOS["brazil"]
    db  = get_db_for("brazil")
    try:
        tot   = performance.total_return(db)
        mtd   = performance.mtd_return(db)
        ytd   = performance.ytd_return(db)
        daily = performance.latest_daily_return(db)
    finally:
        db.close()

    start  = cfg["start_date"]
    cdi    = fetch_cdi_daily(date.fromisoformat(start), date.today())
    ipca   = fetch_ipca_daily(date.fromisoformat(start), date.today())
    ipca15 = _apply_multiplier(ipca, 1.5)

    return dict(
        tot=tot, mtd=mtd, ytd=ytd, daily=daily,
        cdi_tot=_bench_total(cdi),
        ipca_tot=_bench_total(ipca),
        ipca15_tot=_bench_total(ipca15),
    )


def collect_dot():
    dot_cfg = PORTFOLIOS["dot"]
    w_g = dot_cfg.get("w_global", 0.5)
    w_b = dot_cfg.get("w_brazil", 0.5)
    mult = dot_cfg.get("bench_multiplier", 1.5)

    dot   = compute_dot_series(w_global=w_g, w_brazil=w_b, rebal_freq="monthly")
    bench = compute_blended_benchmark(w_global=w_g, w_brazil=w_b, multiplier=mult)
    stats = _returns_from_series(dot)

    return dict(
        tot=stats["total"], mtd=stats["mtd"],
        ytd=stats["ytd"],   daily=stats["daily"],
        bench_tot=_bench_total(bench),
        w_global=w_g, w_brazil=w_b,
        bench_label=dot_cfg.get("bench_label", "1.5× CPI+IPCA"),
    )


# ── Geração do HTML ───────────────────────────────────────────────────────────

def _kpi_row(label, value):
    color = "#16a34a" if value and value >= 0 else "#dc2626" if value and value < 0 else "#374151"
    return (
        f"<tr>"
        f"<td style='padding:4px 0;color:#6b7280;font-size:14px'>{label}</td>"
        f"<td style='padding:4px 0;text-align:right;font-weight:700;"
        f"color:{color};font-size:14px'>{_pct(value)}</td>"
        f"</tr>"
    )


def _section(flag, name, currency, kpis_html, bench_html):
    return f"""
    <div style="background:#ffffff;border-radius:10px;padding:20px 24px;
                margin-bottom:16px;border:1px solid #e5e7eb;">
      <div style="font-size:18px;font-weight:700;color:#111827;margin-bottom:12px">
        {flag} {name}
        <span style="font-size:12px;font-weight:400;color:#9ca3af;margin-left:6px">{currency}</span>
      </div>
      <table style="width:100%;border-collapse:collapse">
        {kpis_html}
        {bench_html}
      </table>
    </div>
    """


def build_html(g, b, d, report_date):
    week_str = report_date.strftime("%d/%m/%Y")
    prev_week = (report_date - timedelta(days=7)).strftime("%d/%m/%Y")

    # Global
    g_kpis = (
        _kpi_row("Daily Return", g["daily"])
        + _kpi_row("MTD", g["mtd"])
        + _kpi_row("YTD", g["ytd"])
        + _kpi_row("Since Inception", g["tot"])
    )
    g_bench = (
        _pp_html(_pp(g["tot"], g["cpi_tot"]),   "CPI EUA acumulado")
        + _pp_html(_pp(g["tot"], g["cpi15_tot"]), "1.5× CPI EUA")
    )

    # Brazil
    b_kpis = (
        _kpi_row("Daily Return", b["daily"])
        + _kpi_row("MTD", b["mtd"])
        + _kpi_row("YTD", b["ytd"])
        + _kpi_row("Since Inception", b["tot"])
    )
    b_bench = (
        _pp_html(_pp(b["tot"], b["cdi_tot"]),    "CDI acumulado")
        + _pp_html(_pp(b["tot"], b["ipca_tot"]),   "IPCA acumulado")
        + _pp_html(_pp(b["tot"], b["ipca15_tot"]), "1.5× IPCA")
    )

    # DOT
    dot_kpis = (
        _kpi_row("Daily Return", d["daily"])
        + _kpi_row("MTD", d["mtd"])
        + _kpi_row("YTD", d["ytd"])
        + _kpi_row("Since Inception", d["tot"])
    )
    dot_bench = _pp_html(_pp(d["tot"], d["bench_tot"]), d["bench_label"])

    global_sec = _section("🌍", "Global Portfolio", "USD",  g_kpis, g_bench)
    brazil_sec = _section("🇧🇷", "Brazil Portfolio", "BRL",  b_kpis, b_bench)
    dot_sec    = _section("⬤",  "DOT Portfolio",     "USD",
                          dot_kpis + f"<tr><td colspan=2 style='padding:4px 0;"
                          f"font-size:12px;color:#9ca3af'>Composição: "
                          f"{int(d['w_global']*100)}% Global / "
                          f"{int(d['w_brazil']*100)}% Brazil · Rebal. mensal</td></tr>",
                          dot_bench)

    return f"""
<!DOCTYPE html>
<html lang="pt-BR">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f3f4f6;font-family:'Segoe UI',system-ui,sans-serif">
  <div style="max-width:560px;margin:32px auto;padding:0 16px">

    <!-- Header -->
    <div style="text-align:center;margin-bottom:24px">
      <div style="font-size:28px;font-weight:800;color:#111827;letter-spacing:-1px">DOT</div>
      <div style="font-size:13px;color:#9ca3af;margin-top:2px">Portfolio Management</div>
      <div style="font-size:13px;color:#6b7280;margin-top:6px">
        Resumo semanal · {prev_week} → {week_str}
      </div>
    </div>

    <!-- Portfolios -->
    {dot_sec}
    {global_sec}
    {brazil_sec}

    <!-- Footer -->
    <div style="text-align:center;padding:16px 0;font-size:11px;color:#9ca3af">
      Gerado automaticamente · DOT Portfolio Management
    </div>
  </div>
</body>
</html>
"""


# ── Envio ─────────────────────────────────────────────────────────────────────

def send_email(html: str, report_date: date):
    sender     = os.environ["EMAIL_SENDER"]
    password   = os.environ["EMAIL_PASSWORD"]
    recipients = [r.strip() for r in os.environ["EMAIL_RECIPIENTS"].split(",")]

    subject = f"DOT Portfolio · Resumo Semanal · {report_date.strftime('%d/%m/%Y')}"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = f"DOT Portfolio <{sender}>"
    msg["To"]      = ", ".join(recipients)
    msg.attach(MIMEText(html, "html"))

    context = ssl.create_default_context()
    with smtplib.SMTP("smtp.office365.com", 587) as server:
        server.ehlo()
        server.starttls(context=context)
        server.ehlo()
        server.login(sender, password)
        server.sendmail(sender, recipients, msg.as_string())

    print(f"✅ Email enviado para: {', '.join(recipients)}")


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    today = date.today()
    print(f"📊 Coletando dados ({today})...")

    g = collect_global()
    b = collect_brazil()
    d = collect_dot()

    print("✉️  Gerando email...")
    html = build_html(g, b, d, today)

    send_email(html, today)
