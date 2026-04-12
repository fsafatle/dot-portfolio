from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / os.getenv("DATA_DIR", "data")
DATA_DIR.mkdir(exist_ok=True)

# ── Legacy globals (kept for backward-compat with scripts) ──────────────────
DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite:///{DATA_DIR}/portfolio.db")
PORTFOLIO_START_DATE = os.getenv("PORTFOLIO_START_DATE", "2025-12-31")
PORTFOLIO_BASE_VALUE = float(os.getenv("PORTFOLIO_BASE_VALUE", "1.0"))

BUCKETS = ["Yield", "Growth", "Reserva de Valor", "Liquidez"]
ROLES   = ["beta", "alpha"]

INITIAL_PORTFOLIO = [
    {
        "name": "Bloomberg Global Aggregate ETF",
        "ticker": "AGGU.L",
        "asset_class": "fixed_income",
        "bucket": "Yield",
        "role": "beta",
        "weight": 0.36,
        "data_source": "yahoo",
    },
    {
        "name": "Global Equities ACWI ETF",
        "ticker": "ACWI",
        "asset_class": "equity",
        "bucket": "Growth",
        "role": "beta",
        "weight": 0.54,
        "data_source": "yahoo",
    },
    {
        "name": "SPDR Gold Shares",
        "ticker": "GLD",
        "asset_class": "commodity",
        "bucket": "Reserva de Valor",
        "role": "beta",
        "weight": 0.025,
        "data_source": "yahoo",
    },
    {
        "name": "Bitcoin",
        "ticker": "BTC-USD",
        "asset_class": "crypto",
        "bucket": "Reserva de Valor",
        "role": "beta",
        "weight": 0.025,
        "data_source": "yahoo",
    },
    {
        "name": "WisdomTree Floating Rate Treasury ETF",
        "ticker": "TFLO",
        "asset_class": "cash_equivalent",
        "bucket": "Liquidez",
        "role": "beta",
        "weight": 0.05,
        "data_source": "yahoo",
    },
]

# ── Multi-portfolio registry ─────────────────────────────────────────────────
PORTFOLIOS: dict[str, dict] = {
    "dot": {
        "key":              "dot",
        "name":             "DOT Portfolio",
        "flag":             "⬤",
        "w_global":         0.50,   # peso do Global Portfolio (USD)
        "w_brazil":         0.50,   # peso do Brazil Portfolio (BRL→USD)
        "bench_multiplier": 1.5,    # multiplicador dos benchmarks CPI+IPCA
        "bench_label":      "1.5× CPI+IPCA (USD)",
    },
    "global": {
        "key":        "global",
        "name":       "Global Portfolio",
        "flag":       "🌍",
        "db_path":    "portfolio.db",   # existing DB — unchanged
        "start_date": "2025-12-31",
        "base_value": 1.0,
        "currency":   "USD",
        "buckets":    ["Yield", "Growth", "Reserva de Valor", "Liquidez"],
        "show_cpi":        True,
        "cpi_label":       "CPI EUA (acumulado)",
        "cpi_multiplier":  1.5,
        "cpi_mult_label":  "1.5× CPI EUA",
        "initial_portfolio": INITIAL_PORTFOLIO,
    },
    "brazil": {
        "key":        "brazil",
        "name":       "Brazil Portfolio",
        "flag":       "🇧🇷",
        "db_path":    "brazil.db",
        "start_date": "2026-01-02",
        "base_value": 1.0,
        "currency":   "BRL",
        "buckets":    ["Yield", "Growth", "Liquidez"],
        "show_cpi":   False,
        "show_cdi":   True,
        "cdi_label":  "CDI acumulado",
        "show_ipca":        True,
        "ipca_label":       "IPCA acumulado",
        "ipca_multiplier":  1.5,
        "ipca_mult_label":  "1.5× IPCA",
        "initial_portfolio": [
            {
                "name":        "IMA-B ETF",
                "ticker":      "IMAB11.SA",
                "asset_class": "fixed_income",
                "bucket":      "Yield",
                "role":        "beta",
                "weight":      0.80,
                "data_source": "yahoo",
            },
            {
                "name":        "Ibovespa ETF",
                "ticker":      "BOVA11.SA",
                "asset_class": "equity",
                "bucket":      "Growth",
                "role":        "beta",
                "weight":      0.20,
                "data_source": "yahoo",
            },
        ],
    },
}
