"""
DOT design system — CSS injection for Streamlit.

Brand reference: Deck DOT.pptx
  BG_LIGHT  #E8E8E8  warm light gray  (content backgrounds)
  BG_DARK   #5E5E5E  charcoal         (sidebar, section breaks)
  TEXT      #929292  mid gray         (all body text)
  ACCENT    #FA9B5A  coral-orange     (the only color — used sparingly)
  CARD      #FFFFFF  white            (card panels)
"""

DOT_CSS = """
<style>

/* ── Global ──────────────────────────────────────────────────────────────── */
html, body, [class*="css"] {
    font-family: 'Segoe UI', system-ui, -apple-system, sans-serif !important;
}
.stApp {
    background-color: #E8E8E8;
}
.block-container {
    padding-top: 1.8rem !important;
    padding-bottom: 2.5rem !important;
    max-width: 1280px;
}

/* ── Sidebar ─────────────────────────────────────────────────────────────── */
[data-testid="stSidebar"] {
    background-color: #5E5E5E !important;
    border-right: none !important;
}
[data-testid="stSidebar"] .stMarkdown,
[data-testid="stSidebar"] p,
[data-testid="stSidebar"] span,
[data-testid="stSidebar"] label {
    color: rgba(255,255,255,0.75) !important;
}
/* Navigation links */
[data-testid="stSidebarNav"] a {
    color: rgba(255,255,255,0.65) !important;
    font-size: 0.875rem;
    padding: 0.35rem 0.75rem;
    border-radius: 8px;
    transition: all 0.15s;
}
[data-testid="stSidebarNav"] a:hover {
    color: #FA9B5A !important;
    background: rgba(250,155,90,0.1);
}
[data-testid="stSidebarNav"] a[aria-selected="true"] {
    color: #FA9B5A !important;
    font-weight: 600;
    background: rgba(250,155,90,0.15);
}
/* Section group labels in sidebar */
[data-testid="stSidebarNavSeparator"],
[data-testid="stSidebarNavItems"] > div[role="separator"] + div {
    color: rgba(255,255,255,0.35) !important;
    font-size: 0.7rem !important;
    text-transform: uppercase !important;
    letter-spacing: 0.08em !important;
}

/* ── Headings ────────────────────────────────────────────────────────────── */
h1 {
    color: #5E5E5E !important;
    font-weight: 700 !important;
    font-size: 1.9rem !important;
    letter-spacing: -0.01em;
    margin-bottom: 0.1rem !important;
}
h2, h3 {
    color: #5E5E5E !important;
    font-weight: 600 !important;
}
h4 {
    color: #929292 !important;
    font-weight: 600 !important;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    font-size: 0.78rem !important;
}
p, li { color: #929292; }

/* ── KPI / Metric cards ──────────────────────────────────────────────────── */
[data-testid="stMetric"] {
    background: #FFFFFF;
    border-radius: 16px;
    padding: 1.1rem 1.3rem 1rem !important;
    box-shadow: 0 2px 14px rgba(0,0,0,0.07);
    border-left: 3px solid #FA9B5A;
    transition: box-shadow 0.2s;
}
[data-testid="stMetric"]:hover {
    box-shadow: 0 4px 20px rgba(0,0,0,0.11);
}
[data-testid="stMetricLabel"] > div {
    color: #ABABAB !important;
    font-size: 0.72rem !important;
    font-weight: 500 !important;
    text-transform: uppercase;
    letter-spacing: 0.07em;
}
[data-testid="stMetricValue"] {
    color: #5E5E5E !important;
    font-size: 1.75rem !important;
    font-weight: 700 !important;
    letter-spacing: -0.02em;
    line-height: 1.2;
}
[data-testid="stMetricDelta"] {
    font-size: 0.82rem !important;
    font-weight: 500 !important;
}

/* ── DataFrames / Tables ─────────────────────────────────────────────────── */
[data-testid="stDataFrame"] {
    background: #FFFFFF;
    border-radius: 14px;
    overflow: hidden;
    box-shadow: 0 2px 12px rgba(0,0,0,0.06);
    border: none !important;
}
[data-testid="stDataFrame"] table {
    border-collapse: collapse;
}
[data-testid="stDataFrame"] thead th {
    background: #F5F5F5 !important;
    color: #ABABAB !important;
    font-size: 0.72rem !important;
    font-weight: 600 !important;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    border-bottom: 1px solid #E8E8E8 !important;
}
[data-testid="stDataFrame"] tbody tr:hover {
    background: #FAFAFA !important;
}

/* ── Buttons (default = white — used for Refresh) ────────────────────────── */
.stButton > button {
    background-color: #FFFFFF !important;
    color: #5E5E5E !important;
    border: 1px solid #E0E0E0 !important;
    border-radius: 40px !important;
    font-weight: 500 !important;
    font-size: 0.85rem !important;
    padding: 0.45rem 1.4rem !important;
    letter-spacing: 0.02em;
    transition: box-shadow 0.2s, transform 0.1s;
    box-shadow: 0 1px 6px rgba(0,0,0,0.10);
}
.stButton > button:hover {
    background-color: #F5F5F5 !important;
    box-shadow: 0 2px 10px rgba(0,0,0,0.14) !important;
    transform: translateY(-1px);
}
.stButton > button:active {
    transform: translateY(0);
}

/* ── Form submit buttons ─────────────────────────────────────────────────── */
[data-testid="stFormSubmitButton"] > button {
    background-color: #5E5E5E !important;
    border-radius: 12px !important;
    font-size: 0.9rem !important;
    padding: 0.6rem 1.5rem !important;
    box-shadow: 0 2px 8px rgba(0,0,0,0.15);
}
[data-testid="stFormSubmitButton"] > button:hover {
    background-color: #4E4E4E !important;
    opacity: 1;
}

/* ── Form inputs ─────────────────────────────────────────────────────────── */
.stTextInput > div > div > input,
.stNumberInput > div > div > input,
.stDateInput > div > div > input {
    background: #FFFFFF !important;
    border-radius: 10px !important;
    border: 1.5px solid #E0E0E0 !important;
    color: #5E5E5E !important;
    font-size: 0.9rem !important;
    padding: 0.5rem 0.8rem !important;
    transition: border-color 0.15s;
}
.stTextInput > div > div > input:focus,
.stNumberInput > div > div > input:focus {
    border-color: #FA9B5A !important;
    box-shadow: 0 0 0 3px rgba(250,155,90,0.12) !important;
}
.stSelectbox > div > div {
    background: #FFFFFF !important;
    border-radius: 10px !important;
    border: 1.5px solid #E0E0E0 !important;
    color: #5E5E5E !important;
}

/* ── Checkboxes ──────────────────────────────────────────────────────────── */
.stCheckbox label span { color: #929292 !important; }

/* ── Expanders ───────────────────────────────────────────────────────────── */
[data-testid="stExpander"] {
    background: #FFFFFF;
    border-radius: 14px !important;
    border: 1px solid #E8E8E8 !important;
    box-shadow: 0 1px 8px rgba(0,0,0,0.05);
    overflow: hidden;
}
[data-testid="stExpander"] summary {
    color: #5E5E5E !important;
    font-weight: 600 !important;
    font-size: 0.9rem;
}

/* ── Divider ─────────────────────────────────────────────────────────────── */
hr {
    border: none !important;
    border-top: 1px solid #D8D8D8 !important;
    margin: 1.5rem 0 !important;
}

/* ── Caption / helper text ───────────────────────────────────────────────── */
.stCaption, [data-testid="stCaptionContainer"] {
    color: #ABABAB !important;
    font-size: 0.78rem !important;
}

/* ── Spinner ─────────────────────────────────────────────────────────────── */
.stSpinner > div {
    border-top-color: #FA9B5A !important;
}

/* ── Success / Error / Warning banners ───────────────────────────────────── */
[data-testid="stAlert"] {
    border-radius: 12px !important;
    font-size: 0.88rem;
}

/* ── Number input steppers ───────────────────────────────────────────────── */
.stNumberInput button {
    background: #F5F5F5 !important;
    border-radius: 6px !important;
    color: #5E5E5E !important;
}

/* ── Bucket section headers (custom class) ───────────────────────────────── */
.bucket-header {
    font-size: 0.72rem !important;
    font-weight: 700 !important;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    margin-top: 0.5rem;
    margin-bottom: 6px;
}

/* ── DOT brand dot (top-right corner) ───────────────────────────────────── */
.dot-mark {
    display: inline-block;
    width: 10px; height: 10px;
    background: #FA9B5A;
    border-radius: 50%;
    margin-left: 6px;
    vertical-align: middle;
}

/* ── Section pill tags ───────────────────────────────────────────────────── */
.pill-dark {
    display: inline-block;
    background: #929292;
    color: #FFFFFF;
    border-radius: 40px;
    padding: 2px 12px;
    font-size: 0.75rem;
    font-weight: 600;
    letter-spacing: 0.04em;
    margin-right: 4px;
}
.pill-light {
    display: inline-block;
    background: #F5F5F5;
    color: #5E5E5E;
    border-radius: 40px;
    border: 1px solid #E0E0E0;
    padding: 2px 12px;
    font-size: 0.75rem;
    font-weight: 500;
    margin-right: 4px;
}


</style>
"""


def inject_dot_css():
    """Call this at the top of every page to apply the DOT design system."""
    import streamlit as st
    st.markdown(DOT_CSS, unsafe_allow_html=True)
