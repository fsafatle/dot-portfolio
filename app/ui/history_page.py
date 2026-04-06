"""Shared history page component."""

from datetime import date

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from app.database import get_db_for
from app.models import PortfolioSnapshot
from app.portfolio.engine import get_active_allocations, get_prices_df
from app.ui.styles import inject_dot_css


def render_history(portfolio_cfg: dict) -> None:
    key        = portfolio_cfg["key"]
    start_date = portfolio_cfg["start_date"]

    st.set_page_config(
        page_title=f"History · {portfolio_cfg['name']}",
        page_icon="⬤",
        layout="wide",
    )
    inject_dot_css()
    st.markdown(
        f"<h1>{portfolio_cfg['flag']} {portfolio_cfg['name']}"
        f"<span class='dot-mark'></span></h1>"
        f"<p style='color:#ABABAB;font-size:0.8rem;margin-top:-8px;'>Histórico de Performance</p>",
        unsafe_allow_html=True,
    )

    # ── Composition on a specific date ───────────────────────────────────────
    st.subheader("Composição em Data Específica")

    query_date = st.date_input(
        "Selecionar data",
        value=date.today(),
        min_value=date.fromisoformat(start_date),
        max_value=date.today(),
    )

    db = get_db_for(key)
    try:
        active_on_date = get_active_allocations(db, query_date)
        if active_on_date:
            rows = [
                {
                    "Ativo":      a.asset.name,
                    "Ticker":     a.asset.ticker,
                    "Classe":     a.asset.asset_class,
                    "Peso (%)":   f"{a.weight*100:.1f}%",
                    "Início":     str(a.start_date),
                    "Fim":        str(a.end_date) if a.end_date else "atual",
                }
                for a in active_on_date
            ]
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        else:
            st.warning(f"Nenhuma alocação ativa em {query_date}.")
    finally:
        db.close()

    st.divider()

    # ── Full snapshot time series ────────────────────────────────────────────
    st.subheader("Portfolio Index — Histórico Completo")

    db2 = get_db_for(key)
    try:
        snap_rows = (
            db2.query(PortfolioSnapshot)
            .order_by(PortfolioSnapshot.date)
            .all()
        )
        if snap_rows:
            snap_df = pd.DataFrame(
                {"Data": r.date, "Índice": r.index_value,
                 "Retorno Diário (%)": (r.daily_return or 0) * 100}
                for r in snap_rows
            )
            st.dataframe(
                snap_df.style.format({"Índice": "{:.4f}", "Retorno Diário (%)": "{:+.3f}%"}),
                use_container_width=True, hide_index=True,
            )
        else:
            st.info("Sem dados. Clique em Refresh no dashboard.")
    finally:
        db2.close()

    st.divider()

    # ── Asset price series ───────────────────────────────────────────────────
    st.subheader("Série de Preços por Ativo")

    db3 = get_db_for(key)
    try:
        start_dt   = date.fromisoformat(start_date)
        prices_df  = get_prices_df(db3, start_dt, date.today())
    finally:
        db3.close()

    if prices_df.empty:
        st.info("Sem preços. Faça Refresh no dashboard.")
    else:
        normalized = prices_df.div(prices_df.iloc[0]) * 100
        dot_palette = ["#FA9B5A", "#5E5E5E", "#929292", "#D5D5D5", "#C8774A"]
        fig2 = px.line(
            normalized.reset_index().rename(columns={"index": "Date"}),
            x="Date", y=normalized.columns.tolist(),
            labels={"value": "Preço normalizado (base=100)", "variable": "Ticker"},
            color_discrete_sequence=dot_palette,
        )
        fig2.update_layout(
            height=360,
            margin=dict(l=0, r=0, t=10, b=0),
            plot_bgcolor="#FFFFFF",
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(family="Segoe UI, system-ui, sans-serif", color="#929292", size=12),
            legend_title="Ticker",
            xaxis=dict(showgrid=False, linecolor="#D5D5D5", linewidth=1),
            yaxis=dict(showgrid=True, gridcolor="#F0F0F0"),
        )
        st.plotly_chart(fig2, use_container_width=True)

        with st.expander("Tabela de preços brutos"):
            st.dataframe(
                prices_df.reset_index().rename(columns={"index": "Date"}),
                use_container_width=True,
            )
