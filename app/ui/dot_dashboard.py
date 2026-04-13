"""Dashboard do DOT Portfolio combinado (Global USD + Brazil BRL→USD)."""

from datetime import date

import plotly.graph_objects as go
import streamlit as st

from app.portfolio.combined import (
    compute_dot_series,
    compute_global_usd_norm,
    compute_brazil_usd_norm,
    compute_blended_benchmark,
    _returns_from_series,
)
from app.ui.styles import inject_dot_css
from app.config import PORTFOLIOS

_ACCENT   = "#FA9B5A"
_CHARCOAL = "#5E5E5E"
_GRAY     = "#929292"
_LIGHT    = "#D5D5D5"
_BLUE     = "#3b82f6"
_GREEN    = "#10b981"

# Padrões — vêm do config
_CFG = PORTFOLIOS.get("dot", {})
_W_BRAZIL_DEFAULT  = int(_CFG.get("w_brazil",   0.50) * 100)
_W_GLOBAL_DEFAULT  = int(_CFG.get("w_global",   0.50) * 100)
_REBAL_FREQ_DEFAULT = _CFG.get("rebal_freq", "annual")
_REBAL_OPTIONS      = ["daily", "monthly", "annual"]
_REBAL_INDEX        = _REBAL_OPTIONS.index(_REBAL_FREQ_DEFAULT) if _REBAL_FREQ_DEFAULT in _REBAL_OPTIONS else 2


def _fmt_pct(v, decimals=2):
    if v is None:
        return "—"
    sign = "+" if v >= 0 else ""
    return f"{sign}{v * 100:.{decimals}f}%"


_BENCH_MULTIPLIER = _CFG.get("bench_multiplier", 1.5)
_BENCH_LABEL      = _CFG.get("bench_label", f"{_CFG.get('bench_multiplier', 1.5)}× CPI+IPCA (USD)")


@st.cache_data(ttl=300)
def _load_dot(cutoff_str: str = "", w_global: float = 0.5, w_brazil: float = 0.5,
              rebal_freq: str = "monthly") -> dict:
    cutoff = date.fromisoformat(cutoff_str) if cutoff_str else None
    dot    = compute_dot_series(cutoff=cutoff, w_brazil=w_brazil, w_global=w_global,
                                rebal_freq=rebal_freq)
    g_norm = compute_global_usd_norm(cutoff=cutoff)
    b_norm = compute_brazil_usd_norm(cutoff=cutoff)
    bench  = compute_blended_benchmark(cutoff=cutoff, w_brazil=w_brazil, w_global=w_global,
                                       multiplier=_BENCH_MULTIPLIER)
    stats  = _returns_from_series(dot)
    return dict(dot=dot, global_norm=g_norm, brazil_norm=b_norm, bench=bench, stats=stats)


def render_dot_dashboard() -> None:
    st.set_page_config(
        page_title="DOT Portfolio · DOT",
        page_icon="⬤",
        layout="wide",
    )
    inject_dot_css()

    # ── Header ────────────────────────────────────────────────────────────────
    st.markdown(
        "<h1>⬤ DOT Portfolio<span class='dot-mark'></span></h1>"
        "<p style='color:#ABABAB;font-size:0.8rem;margin-top:-8px;'>"
        "Global + Brazil · Performance combinada em USD</p>",
        unsafe_allow_html=True,
    )

    # ── Sidebar: apenas data de corte ────────────────────────────────────────
    brazil_start = PORTFOLIOS["brazil"]["start_date"]
    with st.sidebar:
        st.markdown("---")
        st.markdown(
            "<div style='font-size:0.8rem;color:#ABABAB;margin-bottom:4px'>Visualizar até</div>",
            unsafe_allow_html=True,
        )
        cutoff_val = st.date_input(
            label="data_corte_dot",
            value=None,
            min_value=date.fromisoformat(brazil_start),
            max_value=date.today(),
            key="cutoff_dot",
            label_visibility="collapsed",
        )
        cutoff_str = str(cutoff_val) if cutoff_val else ""
        if cutoff_val:
            st.caption(f"📅 Até {cutoff_val.strftime('%d/%m/%Y')}")

    st.divider()

    # ── KPIs + slider de composição ──────────────────────────────────────────
    kpi_cols = st.columns([1, 1, 1, 1, 0.05, 1.4])

    # Os pesos precisam ser lidos ANTES de carregar dados
    with kpi_cols[5]:
        st.markdown(
            "<div style='font-size:0.75rem;color:#929292;margin-bottom:2px'>Composição</div>",
            unsafe_allow_html=True,
        )
        w_global_pct = st.slider(
            "Global (%)", min_value=0, max_value=100,
            value=_W_GLOBAL_DEFAULT, step=5, key="dot_w_global",
            label_visibility="collapsed",
        )
        w_brazil_pct = 100 - w_global_pct
        st.markdown(
            f"<div style='font-size:0.8rem;color:#5E5E5E;margin-top:4px'>"
            f"🌍 Global: <b>{w_global_pct}%</b> &nbsp;|&nbsp; "
            f"🇧🇷 Brazil: <b>{w_brazil_pct}%</b></div>",
            unsafe_allow_html=True,
        )
        st.markdown(
            "<div style='font-size:0.75rem;color:#929292;margin-top:10px;margin-bottom:2px'>"
            "Rebalanceamento</div>",
            unsafe_allow_html=True,
        )
        rebal_freq = st.radio(
            "rebal",
            options=["daily", "monthly", "annual"],
            format_func=lambda x: {"daily": "Diário", "monthly": "Mensal", "annual": "Anual"}[x],
            index=_REBAL_INDEX,
            key="dot_rebal_freq",
            label_visibility="collapsed",
            horizontal=True,
        )

    w_global = w_global_pct / 100.0
    w_brazil = w_brazil_pct / 100.0

    # ── Carrega dados ─────────────────────────────────────────────────────────
    data  = _load_dot(cutoff_str, w_global, w_brazil, rebal_freq)
    stats = data["stats"]

    if data["dot"].empty:
        st.info("Sem dados suficientes. Verifique se ambos os portfolios têm dados.")
        return

    last_date = stats.get("last_date")
    if last_date:
        st.caption(f"Dados até {last_date.strftime('%d/%m/%Y')}")

    with kpi_cols[0]:
        st.metric("Daily Return", _fmt_pct(stats["daily"]))
    with kpi_cols[1]:
        st.metric("MTD", _fmt_pct(stats["mtd"]))
    with kpi_cols[2]:
        st.metric("YTD", _fmt_pct(stats["ytd"]))
    with kpi_cols[3]:
        tot   = stats["total"]
        bench = data["bench"]
        _bench_line = ""
        if tot is not None and not bench.empty:
            _bret = bench.iloc[-1] - 1.0
            _d    = tot - _bret
            _s    = "+" if _d >= 0 else ""
            _col  = "#10b981" if _d >= 0 else "#ef4444"
            _bench_line = (
                f"<div style='margin-top:6px;font-size:0.72rem;line-height:1.5'>"
                f"<span style='color:{_col};font-weight:600'>{_s}{_d*100:.2f}pp</span>"
                f"<span style='color:#ABABAB'> vs {_BENCH_LABEL}</span></div>"
            )
        st.markdown(
            f"<div style='background:#F9F9F9;border-radius:8px;padding:12px 14px 10px;"
            f"border:1px solid #EFEFEF'>"
            f"<div style='font-size:0.8rem;color:#929292;margin-bottom:2px'>Since Inception</div>"
            f"<div style='font-size:1.6rem;font-weight:700;color:#1E1E1E;line-height:1.2'>"
            f"{_fmt_pct(tot)}</div>"
            f"{_bench_line}"
            f"</div>",
            unsafe_allow_html=True,
        )

    st.divider()

    # ── Gráfico ───────────────────────────────────────────────────────────────
    st.subheader("DOT Portfolio vs Global vs Brazil (USD, base = 1)")

    dot_s  = data["dot"]
    g_norm = data["global_norm"]
    b_norm = data["brazil_norm"]

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=list(dot_s.index), y=dot_s.values,
        mode="lines", name="DOT Portfolio",
        line=dict(color=_ACCENT, width=2.5),
    ))
    if not g_norm.empty:
        fig.add_trace(go.Scatter(
            x=list(g_norm.index), y=g_norm.values,
            mode="lines", name="Global Portfolio (USD)",
            line=dict(color=_BLUE, width=1.6, dash="dash"),
        ))
    if not b_norm.empty:
        fig.add_trace(go.Scatter(
            x=list(b_norm.index), y=b_norm.values,
            mode="lines", name="Brazil Portfolio (USD)",
            line=dict(color=_GREEN, width=1.6, dash="dot"),
        ))

    bench = data["bench"]
    if not bench.empty:
        fig.add_trace(go.Scatter(
            x=list(bench.index), y=bench.values,
            mode="lines", name=_BENCH_LABEL,
            line=dict(color="#f59e0b", width=1.5, dash="dash"),
        ))

    fig.update_layout(
        height=380,
        margin=dict(l=0, r=0, t=10, b=0),
        plot_bgcolor="#FFFFFF",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Segoe UI, system-ui, sans-serif", color=_GRAY, size=12),
        legend=dict(
            orientation="h", yanchor="bottom", y=1.02,
            xanchor="left", x=0, font=dict(size=12, color=_GRAY),
        ),
        hovermode="x unified",
        xaxis=dict(showgrid=False, showline=True, linecolor=_LIGHT,
                   linewidth=1, tickfont=dict(color=_GRAY, size=11)),
        yaxis=dict(showgrid=True, gridcolor="#F0F0F0", gridwidth=1,
                   showline=False, tickfont=dict(color=_GRAY, size=11),
                   tickformat=".3f"),
    )
    st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ── Cards de composição ───────────────────────────────────────────────────
    st.subheader("Composição")
    ca, cb = st.columns(2)

    g_total = (g_norm.iloc[-1] - 1.0) if not g_norm.empty else None
    b_total = (b_norm.iloc[-1] - 1.0) if not b_norm.empty else None

    with ca:
        st.markdown(
            f"<div style='background:#F9F9F9;border-radius:8px;padding:14px 16px;"
            f"border:1px solid #EFEFEF'>"
            f"<div style='font-size:0.8rem;color:#929292'>🌍 Global Portfolio</div>"
            f"<div style='font-size:1.1rem;font-weight:700;color:#1E1E1E;margin-top:4px'>"
            f"{w_global_pct}% do DOT</div>"
            f"<div style='font-size:0.85rem;color:#5E5E5E;margin-top:2px'>"
            f"Since inception (USD): {_fmt_pct(g_total)}</div>"
            f"</div>",
            unsafe_allow_html=True,
        )
    with cb:
        st.markdown(
            f"<div style='background:#F9F9F9;border-radius:8px;padding:14px 16px;"
            f"border:1px solid #EFEFEF'>"
            f"<div style='font-size:0.8rem;color:#929292'>🇧🇷 Brazil Portfolio</div>"
            f"<div style='font-size:1.1rem;font-weight:700;color:#1E1E1E;margin-top:4px'>"
            f"{w_brazil_pct}% do DOT</div>"
            f"<div style='font-size:0.85rem;color:#5E5E5E;margin-top:2px'>"
            f"Since inception (USD): {_fmt_pct(b_total)}</div>"
            f"</div>",
            unsafe_allow_html=True,
        )
