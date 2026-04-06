"""
History page — reconstruct portfolio on any past date, view time series.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import date, timedelta

import pandas as pd
import plotly.express as px
import streamlit as st

from app.database import get_db
from app.models import PortfolioSnapshot
from app.portfolio.engine import get_active_allocations, get_prices_df
from app.config import PORTFOLIO_START_DATE

st.set_page_config(page_title="History · DOT", layout="wide")
st.title("Portfolio History")


# ---------------------------------------------------------------------------
# Snapshot on a specific date
# ---------------------------------------------------------------------------
st.subheader("Composition on a Specific Date")

query_date = st.date_input(
    "Select date",
    value=date.today(),
    min_value=date.fromisoformat(PORTFOLIO_START_DATE),
    max_value=date.today(),
)

db = get_db()
try:
    active_on_date = get_active_allocations(db, query_date)
    if active_on_date:
        rows = [
            {
                "Asset": a.asset.name,
                "Ticker": a.asset.ticker,
                "Class": a.asset.asset_class,
                "Weight (%)": f"{a.weight*100:.1f}%",
                "Start date": str(a.start_date),
                "End date": str(a.end_date) if a.end_date else "current",
            }
            for a in active_on_date
        ]
        col_tbl, col_pie = st.columns([3, 2])
        with col_tbl:
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        with col_pie:
            pie_df = pd.DataFrame(
                [{"Asset": r["Asset"], "Weight": a.weight} for r, a in zip(rows, active_on_date)]
            )
            fig = px.pie(
                pie_df, names="Asset", values="Weight",
                color_discrete_sequence=px.colors.qualitative.Bold,
            )
            fig.update_traces(textposition="inside", textinfo="percent+label")
            fig.update_layout(height=260, margin=dict(l=0, r=0, t=10, b=0),
                              paper_bgcolor="rgba(0,0,0,0)", showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning(f"No active allocations found on {query_date}.")
finally:
    db.close()

st.divider()

# ---------------------------------------------------------------------------
# Full snapshot time series
# ---------------------------------------------------------------------------
st.subheader("Portfolio Index — Full History")

db2 = get_db()
try:
    snap_rows = (
        db2.query(PortfolioSnapshot)
        .order_by(PortfolioSnapshot.date)
        .all()
    )
    if snap_rows:
        snap_df = pd.DataFrame(
            {"Date": r.date, "Index": r.index_value, "Daily Return (%)": (r.daily_return or 0) * 100}
            for r in snap_rows
        )
        st.dataframe(
            snap_df.style.format({"Index": "{:.2f}", "Daily Return (%)": "{:+.3f}%"}),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No snapshot data available. Refresh prices on the dashboard.")
finally:
    db2.close()

st.divider()

# ---------------------------------------------------------------------------
# Asset price series
# ---------------------------------------------------------------------------
st.subheader("Asset Price Series")

db3 = get_db()
try:
    start_dt = date.fromisoformat(PORTFOLIO_START_DATE)
    prices_df = get_prices_df(db3, start_dt, date.today())
finally:
    db3.close()

if prices_df.empty:
    st.info("No price data. Refresh from the dashboard first.")
else:
    # Normalize to 100 on first available date
    normalized = prices_df.div(prices_df.iloc[0]) * 100

    fig2 = px.line(
        normalized.reset_index().rename(columns={"index": "Date"}),
        x="Date",
        y=normalized.columns.tolist(),
        labels={"value": "Normalized Price (base=100)", "variable": "Ticker"},
        color_discrete_sequence=px.colors.qualitative.Bold,
    )
    fig2.update_layout(
        height=380,
        margin=dict(l=0, r=0, t=10, b=0),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        legend_title="Ticker",
    )
    st.plotly_chart(fig2, use_container_width=True)

    with st.expander("Raw price table"):
        st.dataframe(prices_df.reset_index().rename(columns={"index": "Date"}),
                     use_container_width=True)
