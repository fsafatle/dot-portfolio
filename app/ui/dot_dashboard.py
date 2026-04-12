"""Dashboard do DOT Portfolio combinado (Global USD + Brazil BRL→USD)."""

from datetime import date
from typing import Optional

import plotly.graph_objects as go
import streamlit as st

from app.portfolio.combined import (
    compute_dot_series,
    compute_global_usd_norm,
    compute_brazil_usd_norm,
    _returns_from_series,
)
from app.ui.auth import show_login, show_logout_button
from app.ui.styles import inject_dot_css
from app.config import PORTFOLIOS

_ACCENT   = "#FA9B5A"
_CHARCOAL = "#5E5E5E"
_GRAY     = "#929292"
_LIGHT    = "#D5D5D5"
_BLUE     = "#3b82f6"
_GREEN    = "#10b981"

# Pesos configuráveis — vêm do config
_CFG = PORTFOLIOS.get("dot", {})
_W_BRAZIL = _CFG.get("w_brazil", 0.50)
_W_GLOBAL = _CFG.get("w_global", 0.50)


def _fmt_pct(v, decimals=2):
    if v is None:
        return "—"
    sign = "+" if v >= 0 else ""
    return f"{sign}{v * 100:.{decimals}f}%"


@st.cache_data(ttl=300)
def _load_dot(cutoff_str: str = "") -> dict:
    cutoff = date.fromisoformat(cutoff_str) if cutoff_str else None
    dot    = compute_dot_series(cutoff=cutoff, w_brazil=_W_BRAZIL, w_global=_W_GLOBAL)
    g_norm = compute_global_usd_norm(cutoff=cutoff)
    b_norm = compute_brazil_usd_norm(cutoff=cutoff)
    stats  = _returns_from_series(dot)
    return dict(dot=dot, global_norm=g_norm, brazil_norm=b_norm, stats=stats)


def render_dot_dashboard() -> None:
    show_login()
    show_logout_button()

    st.set_page_config(
        page_title="DOT Portfolio · DOT",
        page_icon="⬤",
        layout="wide",
    )
    inject_dot_css()

    # ── Header ────────────────────────────────────────────────────────────────
    st.markdown(
        f"<h1>⬤ DOT Portfolio<span class='dot-mark'></span></h1>"
        f"<p style='color:#ABABAB;font-size:0.8rem;margin-top:-8px;'>"
        f"Global + Brazil · Performance combinada em USD · "
        f"{int(_W_GLOBAL*100)}% Global / {int(_W_BRAZIL*100)}% Brazil</p>",
        unsafe_allow_html=True,
    )

    # ── Data de corte (sidebar) ───────────────────────────────────────────────
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

    # ── Carrega dados ─────────────────────────────────────────────────────────
    data  = _load_dot(cutoff_str)
    stats = data["stats"]

    if data["dot"].empty:
        st.info("Sem dados suficientes. Verifique se ambos os portfolios têm dados.")
        return

    last_date = stats.get("last_date")
    if last_date:
        st.caption(f"Dados até {last_date.strftime('%d/%m/%Y')}")

    # ── KPIs ──────────────────────────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Daily Return", _fmt_pct(stats["daily"]))
    with c2:
        st.metric("MTD", _fmt_pct(stats["mtd"]))
    with c3:
        st.metric("YTD", _fmt_pct(stats["ytd"]))
    with c4:
        tot = stats["total"]
        st.markdown(
            f"<div style='background:#F9F9F9;border-radius:8px;padding:12px 14px 10px;"
            f"border:1px solid #EFEFEF'>"
            f"<div style='font-size:0.8rem;color:#929292;margin-bottom:2px'>Since Inception</div>"
            f"<div style='font-size:1.6rem;font-weight:700;color:#1E1E1E;line-height:1.2'>"
            f"{_fmt_pct(tot)}</div>"
            f"</div>",
            unsafe_allow_html=True,
        )

    st.divider()

    # ── Gráfico ───────────────────────────────────────────────────────────────
    st.subheader("DOT Portfolio vs Global vs Brazil (USD, base = 1)")

    dot_s    = data["dot"]
    g_norm   = data["global_norm"]
    b_norm   = data["brazil_norm"]

    fig = go.Figure()

    # DOT combined
    fig.add_trace(go.Scatter(
        x=list(dot_s.index), y=dot_s.values,
        mode="lines", name="DOT Portfolio",
        line=dict(color=_ACCENT, width=2.5),
    ))

    # Global (USD)
    if not g_norm.empty:
        fig.add_trace(go.Scatter(
            x=list(g_norm.index), y=g_norm.values,
            mode="lines", name="Global Portfolio (USD)",
            line=dict(color=_BLUE, width=1.6, dash="dash"),
        ))

    # Brazil (USD)
    if not b_norm.empty:
        fig.add_trace(go.Scatter(
            x=list(b_norm.index), y=b_norm.values,
            mode="lines", name="Brazil Portfolio (USD)",
            line=dict(color=_GREEN, width=1.6, dash="dot"),
        ))

    fig.update_layout(
        height=380,
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

    # ── Resumo da composição ──────────────────────────────────────────────────
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
            f"{int(_W_GLOBAL*100)}% do DOT</div>"
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
            f"{int(_W_BRAZIL*100)}% do DOT</div>"
            f"<div style='font-size:0.85rem;color:#5E5E5E;margin-top:2px'>"
            f"Since inception (USD): {_fmt_pct(b_total)}</div>"
            f"</div>",
            unsafe_allow_html=True,
        )
