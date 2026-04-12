"""Shared dashboard component — used by both Global and Brazil portfolio pages."""

from datetime import date

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from app.database import get_db_for
from app.portfolio import engine, performance
from app.portfolio.engine import get_actual_weights
from app.market_data.cpi_provider import fetch_cpi_daily
from app.market_data.cdi_provider import fetch_cdi_daily
from app.market_data.ipca_provider import fetch_ipca_daily
from app.ui.styles import inject_dot_css

# DOT palette
_ACCENT   = "#FA9B5A"
_CHARCOAL = "#5E5E5E"
_GRAY     = "#929292"
_LIGHT    = "#D5D5D5"


def _fmt_pct(v, decimals=2):
    if v is None:
        return "—"
    sign = "+" if v >= 0 else ""
    return f"{sign}{v * 100:.{decimals}f}%"


@st.cache_data(ttl=300)
def _load_data(portfolio_key: str, cutoff_str: str = "") -> dict:
    cutoff = date.fromisoformat(cutoff_str) if cutoff_str else None
    db = get_db_for(portfolio_key)
    try:
        return dict(
            snap=performance.snapshot_series(db, cutoff=cutoff),
            daily=performance.daily_return_series(db, cutoff=cutoff),
            latest_idx=performance.latest_index_value(db, cutoff=cutoff),
            latest_day=performance.latest_daily_return(db, cutoff=cutoff),
            tot_ret=performance.total_return(db, cutoff=cutoff),
            mtd=performance.mtd_return(db, cutoff=cutoff),
            ytd=performance.ytd_return(db, cutoff=cutoff),
            asset_perf=performance.asset_performance(db, cutoff=cutoff),
        )
    finally:
        db.close()


def _apply_multiplier(series: "pd.Series", multiplier: float) -> "pd.Series":
    """Aplica multiplicador aos retornos diários e reacumula (base = 1.0)."""
    if multiplier == 1.0 or series.empty:
        return series
    import pandas as _pd
    daily = series.pct_change().fillna(0.0)
    result = (1.0 + daily * multiplier).cumprod()
    return result / result.iloc[0]


@st.cache_data(ttl=3600)
def _load_cpi(start_str: str, end_str: str = "", multiplier: float = 1.0) -> dict:
    start = date.fromisoformat(start_str)
    end   = date.fromisoformat(end_str) if end_str else date.today()
    series = fetch_cpi_daily(start, end)
    if series.empty:
        return {}
    series = _apply_multiplier(series, multiplier)
    return {str(k): v for k, v in series.items()}


@st.cache_data(ttl=3600)
def _load_cdi(start_str: str, end_str: str = "", multiplier: float = 1.0) -> dict:
    start = date.fromisoformat(start_str)
    end   = date.fromisoformat(end_str) if end_str else date.today()
    series = fetch_cdi_daily(start, end)
    if series.empty:
        return {}
    series = _apply_multiplier(series, multiplier)
    return {str(k): v for k, v in series.items()}


@st.cache_data(ttl=3600)
def _load_ipca(start_str: str, end_str: str = "", multiplier: float = 1.0) -> dict:
    start = date.fromisoformat(start_str)
    end   = date.fromisoformat(end_str) if end_str else date.today()
    series = fetch_ipca_daily(start, end)
    if series.empty:
        return {}
    series = _apply_multiplier(series, multiplier)
    return {str(k): v for k, v in series.items()}


@st.cache_data(ttl=300)
def _load_bucket_table(portfolio_key: str) -> pd.DataFrame:
    db = get_db_for(portfolio_key)
    try:
        today = date.today()

        # ── V2: lê de AssetSnapshot (fonte de verdade do motor de transações) ──
        try:
            from app.models import (
                Portfolio as _PortV2,
                AssetSnapshot as _AsSnap,
                DailySnapshot as _DaySnap,
            )
            port_v2 = db.query(_PortV2).filter_by(key=portfolio_key).first()
            if port_v2 is not None:
                day = (
                    db.query(_DaySnap)
                      .filter_by(portfolio_id=port_v2.id)
                      .order_by(_DaySnap.date.desc())
                      .first()
                )
                if day and day.nav and day.nav > 0:
                    snaps = (
                        db.query(_AsSnap)
                          .filter_by(portfolio_id=port_v2.id, date=day.date)
                          .all()
                    )
                    rows = []
                    for s in snaps:
                        if (s.asset
                                and s.quantity > 1e-9
                                and s.asset.data_source != "cash_account"):
                            w = (s.market_value / day.nav
                                 if s.market_value and day.nav > 0 else 0.0)
                            rows.append({
                                "bucket":        s.asset.bucket or "—",
                                "role":          s.asset.role or "beta",
                                "Asset":         s.asset.name,
                                "Ticker":        s.asset.ticker,
                                "target_weight": w,
                                "Weight":        w,
                            })
                    if rows:
                        return pd.DataFrame(rows)
        except Exception:
            pass  # fallback para legado

        # ── Legado: Allocation table ──────────────────────────────────────
        actual = get_actual_weights(db, today)
        active = engine.get_active_allocations(db, today)
        rows = []
        for a in active:
            rows.append({
                "bucket":        a.asset.bucket or "—",
                "role":          a.asset.role or "beta",
                "Asset":         a.asset.name,
                "Ticker":        a.asset.ticker,
                "target_weight": a.weight,
                "Weight":        actual.get(a.asset.ticker, a.weight),
            })
        return pd.DataFrame(rows)
    finally:
        db.close()


BUCKET_COLORS = {
    "Yield":            "#3b82f6",
    "Growth":           "#10b981",
    "Reserva de Valor": "#f59e0b",
    "Liquidez":         "#8b5cf6",
}


def render_dashboard(portfolio_cfg: dict) -> None:
    """Render the full portfolio dashboard for a given portfolio config."""

    key              = portfolio_cfg["key"]
    start_date       = portfolio_cfg["start_date"]
    buckets          = portfolio_cfg["buckets"]
    show_cpi         = portfolio_cfg.get("show_cpi", False)
    show_cdi         = portfolio_cfg.get("show_cdi", False)
    show_ipca        = portfolio_cfg.get("show_ipca", False)
    currency         = portfolio_cfg.get("currency", "USD")
    cpi_label        = portfolio_cfg.get("cpi_label", "CPI")
    cdi_label        = portfolio_cfg.get("cdi_label", "CDI acumulado")
    ipca_label       = portfolio_cfg.get("ipca_label", "IPCA acumulado")
    cpi_multiplier   = portfolio_cfg.get("cpi_multiplier", 1.0)
    ipca_multiplier  = portfolio_cfg.get("ipca_multiplier", 1.0)
    cpi_mult_label   = portfolio_cfg.get("cpi_mult_label",  f"{cpi_multiplier}× CPI")
    ipca_mult_label  = portfolio_cfg.get("ipca_mult_label", f"{ipca_multiplier}× IPCA")

    st.set_page_config(
        page_title=f"{portfolio_cfg['name']} · DOT",
        page_icon="⬤",
        layout="wide",
    )
    inject_dot_css()

    # Header + refresh button
    col_title, col_refresh = st.columns([7, 1])
    with col_title:
        st.markdown(
            f"<h1>{portfolio_cfg['flag']} {portfolio_cfg['name']}"
            f"<span class='dot-mark'></span></h1>"
            f"<p style='color:#ABABAB;font-size:0.8rem;margin-top:-8px;'>"
            f"Performance dashboard · {currency}</p>",
            unsafe_allow_html=True,
        )
    with col_refresh:
        st.markdown("<div style='padding-top:12px'></div>", unsafe_allow_html=True)
        refresh_clicked = st.button("↻ Refresh", use_container_width=True)

    if refresh_clicked:
        with st.spinner("Atualizando…"):
            db = get_db_for(key)
            try:
                from app.models import Portfolio as PortfolioModel, Transaction
                from app.engine.reprocessor import recompute_from as _recompute_v2
                from app.services.prices import refresh_prices_for_portfolio as _refresh_v2

                port_v2 = db.query(PortfolioModel).filter_by(key=key).first()
                has_txs = (
                    port_v2 is not None
                    and db.query(Transaction)
                         .filter_by(portfolio_id=port_v2.id, is_void=False)
                         .count() > 0
                )

                if has_txs:
                    # ── Motor v2 ──────────────────────────────────────────
                    # 1. Preços Yahoo → tabela legada (Price) para gráficos/perf
                    engine.refresh_prices(db, start=date.fromisoformat(start_date))
                    db.commit()

                    # 2. Preços Yahoo → tabela v2 (AssetPrice) para o reprocessor
                    #    Esta é a fonte de verdade para mark-to-market do motor v2.
                    _refresh_v2(db, port_v2.id,
                                start=date.fromisoformat(start_date))
                    db.commit()

                    # 3. Reconstrói snapshots usando preços v2 atualizados
                    _recompute_v2(db, port_v2.id, port_v2.base_date)
                    db.commit()
                else:
                    # ── Motor legado ──────────────────────────────────────
                    engine.refresh_prices(db, start=date.fromisoformat(start_date))
                    engine.build_snapshots(
                        db,
                        start_date_str=start_date,
                        base_value=portfolio_cfg.get("base_value", 1.0),
                    )
            finally:
                db.close()
            st.cache_data.clear()
            st.rerun()

    # ── Data de corte (sidebar) ───────────────────────────────────────────────
    with st.sidebar:
        st.markdown("---")
        st.markdown(
            "<div style='font-size:0.8rem;color:#ABABAB;margin-bottom:4px'>Visualizar até</div>",
            unsafe_allow_html=True,
        )
        cutoff_val = st.date_input(
            label="data_corte",
            value=None,
            min_value=date.fromisoformat(start_date),
            max_value=date.today(),
            key=f"cutoff_{key}",
            label_visibility="collapsed",
        )
        cutoff_str = str(cutoff_val) if cutoff_val else ""
        if cutoff_val:
            st.caption(f"📅 Até {cutoff_val.strftime('%d/%m/%Y')}")

    st.divider()

    # KPIs
    data = _load_data(key, cutoff_str)

    c2, c3, c4, c5 = st.columns(4)
    with c2:
        st.metric("Daily Return", _fmt_pct(data["latest_day"]))
    with c3:
        st.metric("MTD", _fmt_pct(data["mtd"]))
    with c4:
        st.metric("YTD", _fmt_pct(data["ytd"]))
    with c5:
        tot = data["tot_ret"]
        _bmark_lines = []
        def _kpi_bmark_line(raw: dict, label: str) -> str:
            if not raw or tot is None:
                return ""
            _bret = list(raw.values())[-1] - 1.0
            _d    = tot - _bret
            _s    = "+" if _d >= 0 else ""
            _col  = "#10b981" if _d >= 0 else "#ef4444"
            return (
                f"<span style='color:{_col};font-weight:600'>{_s}{_d*100:.2f}pp</span>"
                f"<span style='color:#ABABAB'> vs {label}</span>"
            )

        if tot is not None:
            if show_cdi:
                _raw = _load_cdi(start_date, cutoff_str)
                ln = _kpi_bmark_line(_raw, cdi_label)
                if ln: _bmark_lines.append(ln)
            if show_ipca:
                _raw = _load_ipca(start_date, cutoff_str)
                ln = _kpi_bmark_line(_raw, ipca_label)
                if ln: _bmark_lines.append(ln)
                if ipca_multiplier != 1.0:
                    _raw_m = _load_ipca(start_date, cutoff_str, ipca_multiplier)
                    ln = _kpi_bmark_line(_raw_m, ipca_mult_label)
                    if ln: _bmark_lines.append(ln)
            if show_cpi:
                _raw = _load_cpi(start_date, cutoff_str)
                ln = _kpi_bmark_line(_raw, cpi_label)
                if ln: _bmark_lines.append(ln)
                if cpi_multiplier != 1.0:
                    _raw_m = _load_cpi(start_date, cutoff_str, cpi_multiplier)
                    ln = _kpi_bmark_line(_raw_m, cpi_mult_label)
                    if ln: _bmark_lines.append(ln)

        _bmarks_html = (
            "<div style='margin-top:6px;display:flex;flex-direction:column;gap:2px'>"
            + "".join(f"<span style='font-size:0.72rem;line-height:1.5'>{l}</span>" for l in _bmark_lines)
            + "</div>"
        ) if _bmark_lines else ""

        st.markdown(
            f"<div style='"
            f"background:#F9F9F9;border-radius:8px;padding:12px 14px 10px;"
            f"border:1px solid #EFEFEF'>"
            f"<div style='font-size:0.8rem;color:#929292;margin-bottom:2px'>Since Inception</div>"
            f"<div style='font-size:1.6rem;font-weight:700;color:#1E1E1E;line-height:1.2'>"
            f"{_fmt_pct(tot) if tot is not None else '—'}</div>"
            f"{_bmarks_html}"
            f"</div>",
            unsafe_allow_html=True,
        )

    st.divider()

    # Portfolio Index chart
    benchmarks  = [l for l, flag in [(cpi_label, show_cpi), (cdi_label, show_cdi), (ipca_label, show_ipca)] if flag]
    vs_str      = f" vs {' e '.join(benchmarks)}" if benchmarks else ""
    chart_title = f"Portfolio Index{vs_str} (base = 1)"
    st.subheader(chart_title)

    snap = data["snap"]
    if snap.empty:
        st.info("Sem dados. Clique em **↻ Refresh**.")
    else:
        snap_df = snap.reset_index()
        snap_df.columns = ["Date", "Index"]

        # Normalizar para base 1 (independente da escala da cota)
        base_val = snap_df["Index"].iloc[0]
        if base_val and base_val != 0:
            snap_df["IndexNorm"] = snap_df["Index"] / base_val
        else:
            snap_df["IndexNorm"] = snap_df["Index"]

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=snap_df["Date"], y=snap_df["IndexNorm"],
            mode="lines", name=portfolio_cfg["name"],
            line=dict(color=_ACCENT, width=2.5),
        ))

        trading_dates = set(snap_df["Date"].astype(str))

        def _add_bench_trace(raw: dict, name: str, color: str, dash: str, align_to_trading=False):
            if not raw:
                return
            s = pd.Series(raw)
            s.index = pd.to_datetime(list(raw.keys())).date
            if align_to_trading:
                s = s[[d for d in s.index if str(d) in trading_dates]]
            if not s.empty:
                fig.add_trace(go.Scatter(
                    x=list(s.index), y=s.values,
                    mode="lines", name=name,
                    line=dict(color=color, width=1.6, dash=dash),
                ))

        if show_cpi:
            _add_bench_trace(_load_cpi(start_date, cutoff_str),
                             cpi_label, _GRAY, "dot", align_to_trading=True)
            if cpi_multiplier != 1.0:
                _add_bench_trace(_load_cpi(start_date, cutoff_str, cpi_multiplier),
                                 cpi_mult_label, "#f59e0b", "dash", align_to_trading=True)

        if show_cdi:
            _add_bench_trace(_load_cdi(start_date, cutoff_str),
                             cdi_label, _CHARCOAL, "dash")

        if show_ipca:
            _add_bench_trace(_load_ipca(start_date, cutoff_str),
                             ipca_label, "#6366f1", "dot")
            if ipca_multiplier != 1.0:
                _add_bench_trace(_load_ipca(start_date, cutoff_str, ipca_multiplier),
                                 ipca_mult_label, "#ec4899", "dash")

        fig.update_layout(
            height=360,
            margin=dict(l=0, r=0, t=10, b=0),
            yaxis_title="",
            xaxis_title="",
            plot_bgcolor="#FFFFFF",
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(family="Segoe UI, system-ui, sans-serif", color=_GRAY, size=12),
            legend=dict(
                orientation="h", yanchor="bottom", y=1.02,
                xanchor="left", x=0,
                font=dict(size=12, color=_GRAY),
            ),
            hovermode="x unified",
            xaxis=dict(
                showgrid=False,
                showline=True, linecolor=_LIGHT, linewidth=1,
                tickfont=dict(color=_GRAY, size=11),
            ),
            yaxis=dict(
                showgrid=True, gridcolor="#F0F0F0", gridwidth=1,
                showline=False,
                tickfont=dict(color=_GRAY, size=11),
                tickformat=".3f",
            ),
        )
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # Bucket breakdown
    st.subheader("Composição por Fator")

    asset_perf = data["asset_perf"]
    perf_lookup = {}
    if not asset_perf.empty:
        for _, row in asset_perf.iterrows():
            perf_lookup[row["ticker"]] = {
                "total_return":   row["total_return"],
                "daily_return":   row["daily_return"],
                "mtd_return":     row.get("mtd_return"),
                "ytd_return":     row.get("ytd_return"),
                "ret_12m":        row.get("ret_12m"),
                "start_date":     row.get("start_date"),
                "current_weight": row["current_weight"],
                "target_weight":  row.get("target_weight", row["current_weight"]),
                "value_add":      row.get("value_add"),
            }

    bucket_df = _load_bucket_table(key)

    # Carregar posições estratégicas configuradas
    from app.config import DATA_DIR
    import json as _json
    _bt_path = DATA_DIR / f"bucket_targets_{key}.json"
    _bucket_targets: dict = {}
    if _bt_path.exists():
        try:
            _bucket_targets = _json.loads(_bt_path.read_text())
        except Exception:
            pass

    for bucket in buckets:
        subset  = bucket_df[bucket_df["bucket"] == bucket] if not bucket_df.empty else pd.DataFrame()
        color   = BUCKET_COLORS.get(bucket, "#6b7280")
        total_w = subset["Weight"].sum() if not subset.empty else 0.0

        # ── Aggregate bucket returns (weighted average of assets with data) ──
        def _bucket_ret(metric: str) -> str:
            if subset.empty:
                return "—"
            num = denom = 0.0
            for _, row in subset.iterrows():
                p = perf_lookup.get(row["Ticker"], {})
                v = p.get(metric)
                if v is not None:
                    num   += row["Weight"] * v
                    denom += row["Weight"]
            if denom == 0:
                return "—"
            r = num / denom
            sign = "+" if r >= 0 else ""
            return f"{sign}{r * 100:.2f}%"

        b_daily = _bucket_ret("daily_return")
        b_mtd   = _bucket_ret("mtd_return")
        b_ytd   = _bucket_ret("ytd_return")
        b_12m   = _bucket_ret("ret_12m")

        # Posição atual = soma dos pesos reais dos ativos do bucket
        pos_atual_w = sum(
            perf_lookup.get(row["Ticker"], {}).get("current_weight", 0.0)
            for _, row in subset.iterrows()
        ) if not subset.empty else 0.0

        # Posição estratégica = configurada pelo usuário (ou "—" se não definida)
        pos_estrat = _bucket_targets.get(bucket)
        pos_estrat_str = (
            f"{pos_estrat * 100:.0f}%" if pos_estrat else "—"
        )

        st.markdown(
            f"<div class='bucket-header' style='color:{color};margin-bottom:4px'>"
            f"{bucket}"
            f"</div>"
            f"<div style='display:flex;gap:28px;margin-bottom:4px;font-size:0.82rem;color:#929292'>"
            f"<span>Posição Estratégica: <strong style='color:#5E5E5E'>{pos_estrat_str}</strong></span>"
            f"<span>Posição Atual: <strong style='color:#5E5E5E'>{pos_atual_w*100:.1f}%</strong></span>"
            f"</div>"
            f"<div style='display:flex;gap:24px;margin-bottom:10px;font-size:0.8rem;color:#929292'>"
            f"<span>Diário <strong style='color:#5E5E5E'>{b_daily}</strong></span>"
            f"<span>Mês &nbsp;<strong style='color:#5E5E5E'>{b_mtd}</strong></span>"
            f"<span>YTD &nbsp;<strong style='color:#5E5E5E'>{b_ytd}</strong></span>"
            f"<span>12M &nbsp;<strong style='color:#5E5E5E'>{b_12m}</strong></span>"
            f"</div>",
            unsafe_allow_html=True,
        )

        # ── Asset table (HTML para badge no nome) ────────────────────────────
        if not subset.empty:
            _BADGE = {
                "alpha": (
                    "<span style='display:inline-block;padding:1px 7px;"
                    "border-radius:99px;font-size:0.7rem;font-weight:600;"
                    "background:#FEF3E8;color:#FA9B5A;"
                    "border:1px solid #FA9B5A;margin-right:6px;vertical-align:middle'>"
                    "α</span>"
                ),
                "beta": (
                    "<span style='display:inline-block;padding:1px 7px;"
                    "border-radius:99px;font-size:0.7rem;font-weight:600;"
                    "background:#F4F4F4;color:#929292;"
                    "border:1px solid #D0D0D0;margin-right:6px;vertical-align:middle'>"
                    "β</span>"
                ),
            }

            # Cabeçalho
            th = (
                "<style>"
                ".at td,th{padding:6px 10px;font-size:0.83rem;vertical-align:middle;}"
                ".at th{color:#929292;font-weight:500;border-bottom:1px solid #E8E8E8;"
                "white-space:nowrap}"
                ".at td{border-bottom:1px solid #F4F4F4;white-space:nowrap}"
                ".at tr:last-child td{border-bottom:none}"
                "</style>"
                "<table class='at' style='width:100%;border-collapse:collapse'>"
                "<thead><tr>"
                "<th style='width:35%'>Ativo</th>"
                "<th>Ticker</th>"
                "<th>Posição Atual</th>"
                "<th>Diário</th><th>Mês</th><th>YTD</th>"
                "<th>12 Meses</th><th>Desde Início</th>"
                "<th title='Retorno do alpha − retorno do beta (desde início do alpha)'>Geração de Valor</th>"
                "<th>No portfólio desde</th>"
                "</tr></thead><tbody>"
            )

            rows_html = ""
            for _, row in subset.iterrows():
                p   = perf_lookup.get(row["Ticker"], {})
                sd  = p.get("start_date")
                badge = _BADGE.get((row["role"] or "beta").lower(), "")
                peso_atual = p.get("current_weight", row["Weight"])

                def _c(v):
                    if v is None or v == "—":
                        return "<span style='color:#ABABAB'>—</span>"
                    try:
                        fv = float(str(v).replace("%","").replace("+",""))
                        color = "#10b981" if fv > 0 else ("#ef4444" if fv < 0 else "#5E5E5E")
                        sign = "+" if fv > 0 else ""
                        return f"<span style='color:{color}'>{sign}{fv:.2f}%</span>"
                    except Exception:
                        return str(v)

                # Geração de Valor: shown only for alpha assets
                role_lower = (row.get("role") or "beta").lower()
                va = p.get("value_add")
                if role_lower == "alpha":
                    va_cell = _c(_fmt_pct(va)) if va is not None else "<span style='color:#ABABAB'>—</span>"
                else:
                    va_cell = "<span style='color:#ABABAB;font-size:0.75rem'>β</span>"

                rows_html += (
                    f"<tr>"
                    f"<td>{badge}{row['Asset']}</td>"
                    f"<td style='color:#929292'>{row['Ticker']}</td>"
                    f"<td><strong>{peso_atual*100:.1f}%</strong></td>"
                    f"<td>{_c(_fmt_pct(p.get('daily_return')))}</td>"
                    f"<td>{_c(_fmt_pct(p.get('mtd_return')))}</td>"
                    f"<td>{_c(_fmt_pct(p.get('ytd_return')))}</td>"
                    f"<td>{_c(_fmt_pct(p.get('ret_12m')))}</td>"
                    f"<td>{_c(_fmt_pct(p.get('total_return')))}</td>"
                    f"<td>{va_cell}</td>"
                    f"<td style='color:#ABABAB'>{str(sd) if sd else '—'}</td>"
                    f"</tr>"
                )

            st.markdown(th + rows_html + "</tbody></table>", unsafe_allow_html=True)
        else:
            st.caption("*Nenhum ativo neste fator*")

        st.markdown("<div style='margin-bottom:12px'></div>", unsafe_allow_html=True)

    st.divider()

    # Daily Returns bar chart
    st.subheader("Retornos Diários")
    daily = data["daily"]
    if not daily.empty:
        daily_df = daily.reset_index()
        daily_df.columns = ["Date", "Return"]
        daily_df["Return_pct"] = daily_df["Return"] * 100
        fig3 = go.Figure(go.Bar(
            x=daily_df["Date"], y=daily_df["Return_pct"],
            marker_color=daily_df["Return_pct"].apply(
                lambda x: _ACCENT if x >= 0 else _GRAY
            ),
            marker_line_width=0,
        ))
        fig3.update_layout(
            height=180,
            margin=dict(l=0, r=0, t=4, b=0),
            yaxis_title="", xaxis_title="",
            plot_bgcolor="#FFFFFF",
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(family="Segoe UI, system-ui, sans-serif", color=_GRAY, size=11),
            xaxis=dict(showgrid=False, tickfont=dict(color=_GRAY, size=10)),
            yaxis=dict(
                showgrid=True, gridcolor="#F0F0F0",
                tickfont=dict(color=_GRAY, size=10),
                ticksuffix="%",
            ),
        )
        st.plotly_chart(fig3, use_container_width=True)

