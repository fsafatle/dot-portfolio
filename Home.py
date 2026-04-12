"""
DOT Portfolio Management — Navigation router.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import streamlit as st
from app.ui.auth import show_login, show_logout_button

# ── Autenticação ──────────────────────────────────────────────────────────────
show_login()

# ── Navegação (só chega aqui se autenticado) ──────────────────────────────────
show_logout_button()

pg = st.navigation(
    {
        "⬤ DOT Portfolio": [
            st.Page("pages/dot_home.py", title="Home"),
        ],
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
