"""
DOT Portfolio Management — Navigation router.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import streamlit as st

pg = st.navigation(
    {
        "🌍 Global Portfolio": [
            st.Page("pages/global_home.py",        title="Home"),
            st.Page("pages/global_allocations.py", title="Allocations"),
            st.Page("pages/global_cashflow.py",    title="Cash Flow"),
            st.Page("pages/global_history.py",     title="History"),
        ],
        "🇧🇷 Brazil Portfolio": [
            st.Page("pages/brazil_home.py",        title="Home"),
            st.Page("pages/brazil_allocations.py", title="Allocations"),
            st.Page("pages/brazil_cashflow.py",    title="Cash Flow"),
            st.Page("pages/brazil_history.py",     title="History"),
        ],
    }
)
pg.run()
