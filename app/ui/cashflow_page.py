"""
Caixa — entrada única de todos os movimentos do portfólio.

Motor v2: ledger de transações → reprocessor → NAV/cota/PnL corretos.

Tabs:
  ➕  Novo Movimento  — único ponto de entrada (aporte, realoc, resgate, renda)
  📊  Posições        — posição atual por ativo com PnL
  📅  Diário          — histórico de transações por dia + edição
  💰  Extrato         — ledger completo com saldo corrido
"""

from __future__ import annotations

from datetime import date
from itertools import groupby

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from app.database import get_db_for
from app.models import Asset, Portfolio, AssetPrice, DailySnapshot, AssetSnapshot
from app.engine.reprocessor import recompute_from
from app.services.ledger import (
    MOVEMENT_TYPE_LABELS,
    record_aporte, record_realocacao, record_resgate, record_rendimento,
    record_transaction, void_transaction,
    get_transactions,
)
from app.services.prices import (
    upsert_manual_price, get_latest_price, get_latest_price_date,
    refresh_prices_for_portfolio,
)
from app.ui.styles import inject_dot_css

# ── Constantes visuais ────────────────────────────────────────────────────────
_ACCENT = "#FA9B5A"
_GREEN  = "#10b981"
_RED    = "#ef4444"
_GRAY   = "#929292"
_LIGHT  = "#E8E8E8"

ASSET_CLASSES = ["fixed_income", "equity", "commodity",
                 "crypto", "cash_equivalent", "other"]

HIGH_LEVEL_MOVEMENTS = {
    "aporte":     "💰 Aporte + Investimento",
    "realocacao": "🔄 Realocação entre Ativos",
    "resgate":    "📤 Resgate",
    "rendimento": "🎁 Rendimento (dividendo / cupom)",
}


# ── Helpers de formatação ─────────────────────────────────────────────────────

def _cur(v, currency, d=2):
    if v is None:
        return "—"
    sym = "R$" if currency == "BRL" else "$"
    return f"{sym} {v:,.{d}f}"


def _pct(v, d=2):
    return "—" if v is None else f"{v * 100:.{d}f}%"


def _signed_color(v):
    if v is None or v == 0:
        return _GRAY
    return _GREEN if v > 0 else _RED


# ── Helpers de DB ─────────────────────────────────────────────────────────────

def _get_portfolio(key: str) -> Portfolio | None:
    db = get_db_for(key)
    try:
        return db.query(Portfolio).filter_by(key=key).first()
    finally:
        db.close()


def _active_assets(key: str) -> list[dict]:
    """Retorna ativos ativos vinculados ao portfólio."""
    db = get_db_for(key)
    try:
        port = db.query(Portfolio).filter_by(key=key).first()
        if port is None:
            return []
        assets = db.query(Asset).filter_by(
            portfolio_id=port.id, is_active=True
        ).all()
        return [
            {
                "id":          a.id,
                "ticker":      a.ticker,
                "name":        a.name,
                "asset_class": a.asset_class,
                "bucket":      a.bucket,
                "data_source": a.data_source,
            }
            for a in assets
        ]
    finally:
        db.close()


def _get_or_create_portfolio(key: str, cfg: dict) -> int:
    """Garante que existe um registro Portfolio. Retorna portfolio_id."""
    db = get_db_for(key)
    try:
        port = db.query(Portfolio).filter_by(key=key).first()
        if port is None:
            port = Portfolio(
                key=key,
                name=cfg["name"],
                currency=cfg.get("currency", "USD"),
                base_date=date.fromisoformat(cfg["start_date"]),
                initial_cash=float(cfg.get("initial_cash", 0.0)),
                cota_base=float(cfg.get("cota_base", 1000.0)),
            )
            db.add(port)
            db.commit()
        return port.id
    finally:
        db.close()


def _latest_snapshot(key: str) -> dict | None:
    db = get_db_for(key)
    try:
        port = db.query(Portfolio).filter_by(key=key).first()
        if port is None:
            return None
        snap = (db.query(DailySnapshot)
                  .filter_by(portfolio_id=port.id)
                  .order_by(DailySnapshot.date.desc())
                  .first())
        if snap is None:
            return None
        return {
            "date":           snap.date,
            "cash":           snap.cash,
            "invested_value": snap.invested_value,
            "nav":            snap.nav,
            "cota":           snap.cota,
            "daily_return":   snap.daily_return,
            "cum_return":     snap.cum_return,
        }
    finally:
        db.close()


def _asset_snapshots_latest(key: str) -> list[dict]:
    db = get_db_for(key)
    try:
        port = db.query(Portfolio).filter_by(key=key).first()
        if port is None:
            return []
        last = (db.query(DailySnapshot)
                  .filter_by(portfolio_id=port.id)
                  .order_by(DailySnapshot.date.desc())
                  .first())
        if last is None:
            return []
        snaps = (db.query(AssetSnapshot)
                   .filter_by(portfolio_id=port.id, date=last.date)
                   .all())
        return [
            {
                "ticker":        s.asset.ticker if s.asset else "?",
                "name":          s.asset.name   if s.asset else "?",
                "date":          s.date,
                "quantity":      s.quantity,
                "avg_cost":      s.avg_cost,
                "price":         s.price,
                "market_value":  s.market_value,
                "unrealized":    s.unrealized_pnl,
                "realized":      s.realized_pnl,
                "dividends":     s.dividends,
                "total_pnl":     s.total_pnl,
            }
            for s in snaps
            if s.quantity > 1e-9
        ]
    finally:
        db.close()


def _get_asset_price(key: str, asset_id: int, on_date: date) -> float | None:
    db = get_db_for(key)
    try:
        row = db.query(AssetPrice).filter_by(
            asset_id=asset_id, date=on_date
        ).first()
        if row:
            return row.price
        # fallback: último disponível
        last = (db.query(AssetPrice)
                  .filter(AssetPrice.asset_id == asset_id,
                          AssetPrice.date <= on_date)
                  .order_by(AssetPrice.date.desc())
                  .first())
        return last.price if last else None
    finally:
        db.close()


# ── Entrada principal ─────────────────────────────────────────────────────────

def render_cashflow(portfolio_cfg: dict) -> None:
    key       = portfolio_cfg["key"]
    currency  = portfolio_cfg.get("currency", "USD")
    buckets   = portfolio_cfg.get("buckets",
                                  ["Yield", "Growth", "Reserva de Valor", "Liquidez"])

    st.set_page_config(
        page_title=f"Caixa · {portfolio_cfg['name']}",
        page_icon="⬤", layout="wide",
    )
    inject_dot_css()

    # Garante que Portfolio existe no banco
    portfolio_id = _get_or_create_portfolio(key, portfolio_cfg)

    # Cabeçalho
    snap = _latest_snapshot(key)
    nav_str  = _cur(snap["nav"],  currency) if snap else "—"
    cota_str = f"{snap['cota']:.2f}" if snap else "—"
    ret_str  = _pct(snap["cum_return"]) if snap else "—"

    st.markdown(
        f"<h1>{portfolio_cfg['flag']} {portfolio_cfg['name']}</h1>"
        f"<p style='color:#ABABAB;font-size:0.8rem;margin-top:-8px;'>"
        f"NAV: <strong>{nav_str}</strong> &nbsp;·&nbsp; "
        f"Cota: <strong>{cota_str}</strong> &nbsp;·&nbsp; "
        f"Retorno: <strong>{ret_str}</strong></p>",
        unsafe_allow_html=True,
    )

    tab_novo, tab_pos, tab_diario, tab_extrato = st.tabs([
        "➕  Novo Movimento",
        "📊  Posições",
        "📅  Diário",
        "💰  Extrato",
    ])

    # ═══════════════════════════════════════════════════════════════════════
    # TAB 1 — NOVO MOVIMENTO
    # ═══════════════════════════════════════════════════════════════════════
    with tab_novo:
        col_type, col_date = st.columns([3, 2])
        with col_type:
            mv_label = st.selectbox(
                "Tipo de movimento",
                list(HIGH_LEVEL_MOVEMENTS.values()),
                key=f"{key}_mv_type",
            )
            mv_type = next(k for k, v in HIGH_LEVEL_MOVEMENTS.items()
                           if v == mv_label)
        with col_date:
            mv_date = st.date_input("Data", value=date.today(),
                                    key=f"{key}_mv_date")

        mv_desc = st.text_input(
            "Descrição (opcional)",
            placeholder="ex: primeiro aporte · alteração no portfolio jan/2026",
            key=f"{key}_mv_desc",
        )

        ativos    = _active_assets(key)
        portfolio_vazio = len(ativos) == 0
        st.divider()

        # ─────────────────────────────────────────────────────────────────
        # APORTE
        # ─────────────────────────────────────────────────────────────────
        if mv_type == "aporte":
            if portfolio_vazio:
                st.info("**Portfólio vazio.** Primeiro aporte — os ativos serão criados.")

            # ── Modo de entrada ──────────────────────────────────────────
            ap_mode = st.radio(
                "Modo de entrada",
                ["💲 Valor", "% do aporte"],
                horizontal=True,
                key=f"{key}_ap_mode",
            )
            ap_pct_mode = (ap_mode == "% do aporte")

            if ap_pct_mode:
                total_aporte = st.number_input(
                    f"Total do aporte ({currency})",
                    min_value=0.0, step=1000.0, format="%.2f",
                    key=f"{key}_ap_total",
                    help="Valor total sendo aportado. A % de cada ativo calcula o valor automaticamente.",
                )
            else:
                total_aporte = 0.0

            st.markdown("**Distribuição do aporte por ativo**")

            lines_key = f"{key}_ap_lines_{mv_date}"
            if lines_key not in st.session_state:
                st.session_state[lines_key] = [
                    {"ticker": a["ticker"], "name": a["name"],
                     "asset_id": a["id"], "value": 0.0, "quantity": None,
                     "is_new": False}
                    for a in ativos
                ]

            lines: list[dict] = st.session_state[lines_key]

            to_remove = None

            # ── Cabeçalho da tabela ──────────────────────────────────────
            if ap_pct_mode:
                h1, h2, h3, h4, h5 = st.columns([4, 1.5, 2, 2, 0.5])
                h1.markdown("<small style='color:#ABABAB'>Ativo</small>", unsafe_allow_html=True)
                h2.markdown("<small style='color:#ABABAB'>%</small>", unsafe_allow_html=True)
                h3.markdown(f"<small style='color:#ABABAB'>Valor ({currency})</small>", unsafe_allow_html=True)
                h4.markdown("<small style='color:#ABABAB'>Quantidade (auto)</small>", unsafe_allow_html=True)
            else:
                h1, h2, h3, h4 = st.columns([4, 2, 2, 0.5])
                h1.markdown("<small style='color:#ABABAB'>Ativo</small>", unsafe_allow_html=True)
                h2.markdown(f"<small style='color:#ABABAB'>Valor ({currency})</small>", unsafe_allow_html=True)
                h3.markdown("<small style='color:#ABABAB'>Quantidade</small>", unsafe_allow_html=True)

            for i, ln in enumerate(lines):
                price_hint = None
                if ln.get("asset_id"):
                    price_hint = _get_asset_price(key, ln["asset_id"], mv_date)

                label_color = _ACCENT if ln.get("is_new") else "inherit"
                asset_label = (
                    f"<div style='padding-top:6px;color:{label_color}'>"
                    f"{'★ ' if ln.get('is_new') else ''}{ln['name']} "
                    f"<span style='color:#ABABAB;font-size:0.8em'>({ln['ticker']})</span>"
                    f"</div>"
                )

                if ap_pct_mode:
                    c1, c2, c3, c4, c5 = st.columns([4, 1.5, 2, 2, 0.5])
                    with c1:
                        st.markdown(asset_label, unsafe_allow_html=True)
                    with c2:
                        pct_val = st.number_input(
                            f"% {ln['ticker']}", label_visibility="collapsed",
                            min_value=0.0, max_value=100.0, step=1.0, format="%.1f",
                            key=f"{key}_ap_pct_{i}_{mv_date}",
                        )
                        v = round(total_aporte * pct_val / 100, 2) if total_aporte > 0 else 0.0
                        lines[i]["value"] = v
                    with c3:
                        st.markdown(
                            f"<div style='padding-top:6px;font-weight:600'>"
                            f"{_cur(v, currency)}</div>",
                            unsafe_allow_html=True,
                        )
                    with c4:
                        auto_qty = round(v / price_hint, 6) if price_hint and v > 0 else 0.0
                        if price_hint:
                            st.markdown(
                                f"<div style='padding-top:6px;color:#5E5E5E'>"
                                f"{auto_qty:.4f}"
                                f"<span style='color:#ABABAB;font-size:0.78em'>"
                                f" @ {_cur(price_hint, currency, 4)}</span></div>",
                                unsafe_allow_html=True,
                            )
                        else:
                            st.markdown(
                                "<div style='padding-top:6px;color:#ABABAB'>sem preço</div>",
                                unsafe_allow_html=True,
                            )
                        lines[i]["quantity"] = auto_qty if auto_qty > 0 else None
                    with c5:
                        if st.button("✕", key=f"{key}_ap_rm_{i}_{mv_date}"):
                            to_remove = i
                else:
                    c1, c2, c3, c4 = st.columns([4, 2, 2, 0.5])
                    with c1:
                        st.markdown(asset_label, unsafe_allow_html=True)
                    with c2:
                        v = st.number_input(
                            ln["ticker"] + "_v", label_visibility="collapsed",
                            min_value=0.0, step=10.0, format="%.2f",
                            value=float(ln.get("value", 0.0)),
                            key=f"{key}_ap_v_{i}_{mv_date}",
                        )
                        lines[i]["value"] = v
                    with c3:
                        calc_qty = round(v / price_hint, 6) if price_hint and v > 0 else 0.0
                        q = st.number_input(
                            ln["ticker"] + "_q", label_visibility="collapsed",
                            min_value=0.0, step=1.0, format="%.4f",
                            value=float(lines[i].get("quantity") or calc_qty),
                            key=f"{key}_ap_q_{i}_{mv_date}",
                        )
                        lines[i]["quantity"] = q if q > 0 else None
                    with c4:
                        if st.button("✕", key=f"{key}_ap_rm_{i}_{mv_date}"):
                            to_remove = i

            if to_remove is not None:
                lines.pop(to_remove)
                st.session_state[lines_key] = lines
                st.rerun()

            # Adicionar ativo novo
            with st.expander("➕ Adicionar ativo", expanded=portfolio_vazio):
                na_c1, na_c2 = st.columns(2)
                with na_c1:
                    na_ticker = st.text_input(
                        "Ticker", placeholder="ex: IMAB11.SA",
                        key=f"{key}_ap_na_ticker",
                    ).strip().upper()
                    na_name   = st.text_input("Nome", key=f"{key}_ap_na_name")
                    na_source = st.selectbox("Fonte", ["yahoo", "manual"],
                                             key=f"{key}_ap_na_source")
                with na_c2:
                    na_class  = st.selectbox("Classe", ASSET_CLASSES,
                                             key=f"{key}_ap_na_class")
                    na_bucket = st.selectbox("Fator", buckets,
                                             key=f"{key}_ap_na_bucket")
                    na_role   = st.selectbox("Role", ["beta", "alpha"],
                                             key=f"{key}_ap_na_role")
                    na_price  = st.number_input(
                        f"Preço unitário ({currency})",
                        min_value=0.0, step=1.0, format="%.4f",
                        key=f"{key}_ap_na_price",
                        help="Preço de entrada para calcular quantidade.",
                    )

                if st.button("Incluir", key=f"{key}_ap_na_add"):
                    if not na_ticker:
                        st.error("Informe o ticker.")
                    elif any(ln["ticker"] == na_ticker for ln in lines):
                        st.error(f"{na_ticker} já está na lista.")
                    else:
                        lines.append({
                            "ticker":      na_ticker,
                            "name":        na_name or na_ticker,
                            "asset_id":    None,
                            "value":       0.0,
                            "quantity":    None,
                            "asset_class": na_class,
                            "bucket":      na_bucket,
                            "role":        na_role,
                            "data_source": na_source,
                            "entry_price": na_price if na_price > 0 else None,
                            "is_new":      True,
                        })
                        st.session_state[lines_key] = lines
                        st.rerun()

            total_inv = sum(ln.get("value", 0) for ln in lines)
            ok_ap     = total_inv > 0 and any(ln.get("value", 0) > 0 for ln in lines)

            st.markdown(
                f"<div style='margin:12px 0'>"
                f"<span style='font-weight:700;"
                f"color:{'#10b981' if total_inv > 0 else _GRAY};font-size:1.05em'>"
                f"Total: {_cur(total_inv, currency)}</span></div>",
                unsafe_allow_html=True,
            )

            if st.button("💾 Confirmar Aporte", key=f"{key}_ap_save",
                         use_container_width=True, disabled=not ok_ap):
                db = get_db_for(key)
                try:
                    port = db.query(Portfolio).filter_by(key=key).first()
                    pid  = port.id

                    buy_lines_exec = []
                    for ln in lines:
                        if not ln.get("value", 0) > 0:
                            continue

                        # Resolver asset_id (cria ativo novo se necessário)
                        aid = ln.get("asset_id")
                        if aid is None:
                            a = Asset(
                                portfolio_id = pid,
                                name         = ln["name"],
                                ticker       = ln["ticker"],
                                asset_class  = ln.get("asset_class", "other"),
                                data_source  = ln.get("data_source", "manual"),
                                bucket       = ln.get("bucket"),
                                role         = ln.get("role", "beta"),
                                is_active    = True,
                            )
                            db.add(a)
                            db.flush()
                            aid = a.id

                        # Resolver qty e price
                        qty = ln.get("quantity")
                        if not qty or qty <= 0:
                            # Tenta calcular pelo preço
                            ap = (db.query(AssetPrice)
                                    .filter(AssetPrice.asset_id == aid,
                                            AssetPrice.date <= mv_date)
                                    .order_by(AssetPrice.date.desc())
                                    .first())
                            p = ap.price if ap else ln.get("entry_price")
                            if p and p > 0:
                                qty = ln["value"] / p
                            else:
                                st.error(
                                    f"Sem preço para {ln['ticker']} em {mv_date}. "
                                    "Informe a quantidade manualmente."
                                )
                                db.rollback()
                                break
                        else:
                            p = ln["value"] / qty

                        buy_lines_exec.append({
                            "asset_id": aid,
                            "ticker":   ln["ticker"],
                            "value":    ln["value"],
                            "quantity": qty,
                            "price":    p,
                        })
                    else:
                        if buy_lines_exec:
                            with st.spinner("Registrando aporte…"):
                                record_aporte(
                                    db, pid, mv_date,
                                    buy_lines_exec,
                                    description=mv_desc or "Aporte",
                                )
                                db.commit()
                            st.session_state.pop(lines_key, None)
                            st.cache_data.clear()
                            st.success(
                                f"✓ Aporte de {_cur(total_inv, currency)} registrado."
                            )
                            st.rerun()
                except ValueError as exc:
                    db.rollback()
                    st.error(str(exc))
                except Exception as exc:
                    db.rollback()
                    st.error(f"Erro: {exc}")
                    raise
                finally:
                    db.close()

        # ─────────────────────────────────────────────────────────────────
        # REALOCAÇÃO
        # ─────────────────────────────────────────────────────────────────
        elif mv_type == "realocacao":
            if portfolio_vazio:
                st.warning("Portfólio vazio — faça um Aporte primeiro.")
            else:
                # ── Modo de entrada ──────────────────────────────────────
                nav_ref = snap["nav"] if snap else 0.0
                rl_mode = st.radio(
                    "Modo de entrada",
                    ["💲 Valor", "% do portfólio"],
                    horizontal=True,
                    key=f"{key}_rl_mode",
                )
                rl_pct_mode = (rl_mode == "% do portfólio")

                if rl_pct_mode and nav_ref > 0:
                    st.caption(
                        f"Base de cálculo: NAV = **{_cur(nav_ref, currency)}**  "
                        f"— A % inserida é aplicada sobre este valor."
                    )
                elif rl_pct_mode:
                    st.warning("NAV não disponível — use modo por valor.")
                    rl_pct_mode = False

                col_vende, col_compra = st.columns(2)

                with col_vende:
                    st.markdown("**📉 Vender**")
                    rl_sell: dict[int, dict] = {}
                    for a in ativos:
                        p = _get_asset_price(key, a["id"], mv_date) or 0.0
                        if rl_pct_mode:
                            pct_s = st.number_input(
                                f"{a['ticker']} %",
                                min_value=0.0, max_value=100.0, step=0.5, format="%.2f",
                                key=f"{key}_rl_spct_{a['id']}",
                                help=f"% do portfólio a vender de {a['ticker']}",
                            )
                            v = round(nav_ref * pct_s / 100, 2) if pct_s > 0 else 0.0
                            auto_q = round(v / p, 6) if p > 0 and v > 0 else 0.0
                            if pct_s > 0:
                                st.markdown(
                                    f"<small style='color:#929292'>"
                                    f"{_cur(v, currency)} · {auto_q:.4f} un"
                                    f"{'  @ ' + _cur(p, currency, 4) if p > 0 else ''}"
                                    f"</small>",
                                    unsafe_allow_html=True,
                                )
                            q = auto_q
                        else:
                            c1, c2 = st.columns([2, 2])
                            with c1:
                                v = st.number_input(
                                    f"{a['ticker']}",
                                    label_visibility="visible",
                                    min_value=0.0, step=10.0, format="%.2f",
                                    key=f"{key}_rl_sv_{a['id']}",
                                )
                            with c2:
                                default_q = round(v / p, 6) if p > 0 and v > 0 else 0.0
                                q = st.number_input(
                                    "Qtd",
                                    label_visibility="visible",
                                    min_value=0.0, step=1.0, format="%.4f",
                                    value=default_q,
                                    key=f"{key}_rl_sq_{a['id']}",
                                )
                        if v > 0:
                            rl_sell[a["id"]] = {
                                "asset_id": a["id"], "ticker": a["ticker"],
                                "value": v, "quantity": q if q > 0 else (v / p if p > 0 else 0),
                                "price": p,
                            }

                with col_compra:
                    st.markdown("**📈 Comprar**")
                    rl_buy: dict[int, dict] = {}
                    for a in ativos:
                        p = _get_asset_price(key, a["id"], mv_date) or 0.0
                        if rl_pct_mode:
                            pct_b = st.number_input(
                                f"{a['ticker']} %",
                                min_value=0.0, max_value=100.0, step=0.5, format="%.2f",
                                key=f"{key}_rl_bpct_{a['id']}",
                                help=f"% do portfólio a comprar de {a['ticker']}",
                            )
                            v = round(nav_ref * pct_b / 100, 2) if pct_b > 0 else 0.0
                            auto_q = round(v / p, 6) if p > 0 and v > 0 else 0.0
                            if pct_b > 0:
                                st.markdown(
                                    f"<small style='color:#929292'>"
                                    f"{_cur(v, currency)} · {auto_q:.4f} un"
                                    f"{'  @ ' + _cur(p, currency, 4) if p > 0 else ''}"
                                    f"</small>",
                                    unsafe_allow_html=True,
                                )
                            q = auto_q
                        else:
                            c1, c2 = st.columns([2, 2])
                            with c1:
                                v = st.number_input(
                                    f"{a['ticker']}",
                                    min_value=0.0, step=10.0, format="%.2f",
                                    key=f"{key}_rl_bv_{a['id']}",
                                    label_visibility="visible",
                                )
                            with c2:
                                default_q = round(v / p, 6) if p > 0 and v > 0 else 0.0
                                q = st.number_input(
                                    "Qtd",
                                    min_value=0.0, step=1.0, format="%.4f",
                                    value=default_q,
                                    key=f"{key}_rl_bq_{a['id']}",
                                    label_visibility="visible",
                                )
                        if v > 0:
                            rl_buy[a["id"]] = {
                                "asset_id": a["id"], "ticker": a["ticker"],
                                "value": v, "quantity": q if q > 0 else (v / p if p > 0 else 0),
                                "price": p,
                            }

                    # Novo ativo na compra
                    with st.expander("➕ Comprar ativo novo"):
                        na_c1, na_c2 = st.columns(2)
                        with na_c1:
                            rl_na_ticker = st.text_input(
                                "Ticker", key=f"{key}_rl_na_ticker",
                                placeholder="ex: FIIM11.SA",
                            ).strip().upper()
                            rl_na_name = st.text_input("Nome", key=f"{key}_rl_na_name")
                        with na_c2:
                            rl_na_class  = st.selectbox("Classe", ASSET_CLASSES,
                                                        key=f"{key}_rl_na_class")
                            rl_na_bucket = st.selectbox("Fator", buckets,
                                                        key=f"{key}_rl_na_bucket")
                            rl_na_source = st.selectbox("Fonte", ["manual", "yahoo"],
                                                        key=f"{key}_rl_na_source")
                            rl_na_role   = st.selectbox("Role", ["beta", "alpha"],
                                                        key=f"{key}_rl_na_role")

                        st.divider()

                        # Modo de dimensionamento
                        rl_na_mode = st.radio(
                            "Tamanho da posição",
                            ["💲 Valor", "% do portfólio"],
                            horizontal=True,
                            key=f"{key}_rl_na_mode",
                        )

                        na_p1, na_p2 = st.columns(2)
                        with na_p1:
                            rl_na_price = st.number_input(
                                f"Preço unitário ({currency})",
                                min_value=0.0, step=1.0, format="%.4f",
                                key=f"{key}_rl_na_price",
                            )
                        with na_p2:
                            if rl_na_mode == "% do portfólio":
                                rl_na_pct = st.number_input(
                                    "% do portfólio",
                                    min_value=0.0, max_value=100.0,
                                    step=0.5, format="%.2f",
                                    key=f"{key}_rl_na_pct",
                                    help=f"Base: NAV = {_cur(nav_ref, currency)}",
                                )
                                rl_na_val = round(nav_ref * rl_na_pct / 100, 2) if nav_ref > 0 else 0.0
                                if rl_na_val > 0:
                                    st.markdown(
                                        f"<div style='padding-top:28px;font-size:0.9rem;"
                                        f"color:#5E5E5E;font-weight:600'>"
                                        f"= {_cur(rl_na_val, currency)}</div>",
                                        unsafe_allow_html=True,
                                    )
                            else:
                                rl_na_val = st.number_input(
                                    f"Valor ({currency})",
                                    min_value=0.0, step=10.0, format="%.2f",
                                    key=f"{key}_rl_na_val",
                                )

                        rl_na_qty = (
                            round(rl_na_val / rl_na_price, 6)
                            if rl_na_price > 0 and rl_na_val > 0 else 0.0
                        )
                        if rl_na_val > 0 and rl_na_price > 0:
                            st.caption(
                                f"Quantidade calculada: **{rl_na_qty:.4f} unidades** "
                                f"@ {_cur(rl_na_price, currency, 4)}"
                            )

                total_sell_rl = sum(ln["value"] for ln in rl_sell.values())
                total_buy_rl  = sum(ln["value"] for ln in rl_buy.values())
                if rl_na_val > 0 and rl_na_ticker and rl_na_qty > 0:
                    total_buy_rl += rl_na_val
                diff_rl = total_sell_rl - total_buy_rl
                ok_rl   = abs(diff_rl) < 0.01 and total_sell_rl > 0

                # Warn when new asset has value but price is missing
                if rl_na_val > 0 and rl_na_ticker and rl_na_qty == 0:
                    st.warning("⚠️ Informe o Preço unitário do novo ativo para calcular a quantidade.")

                st.markdown(
                    f"<div style='margin:10px 0'>"
                    f"Vendas: <strong>{_cur(total_sell_rl, currency)}</strong> &nbsp;"
                    f"Compras: <strong>{_cur(total_buy_rl, currency)}</strong> &nbsp;"
                    f"<span style='color:{'#10b981' if ok_rl else _RED};font-weight:700'>"
                    f"{'✓ Balanceado' if ok_rl else f'Dif: {_cur(diff_rl, currency)}'}"
                    f"</span></div>",
                    unsafe_allow_html=True,
                )

                if st.button("💾 Confirmar Realocação", key=f"{key}_rl_save",
                             use_container_width=True, disabled=not ok_rl):
                    db = get_db_for(key)
                    try:
                        port = db.query(Portfolio).filter_by(key=key).first()
                        pid  = port.id

                        sell_exec = list(rl_sell.values())
                        buy_exec  = list(rl_buy.values())

                        if rl_na_val > 0 and rl_na_ticker and rl_na_qty > 0:
                            # Cria ativo novo
                            a_new = (db.query(Asset)
                                       .filter_by(ticker=rl_na_ticker)
                                       .first())
                            if a_new is None:
                                a_new = Asset(
                                    portfolio_id=pid,
                                    ticker=rl_na_ticker,
                                    name=rl_na_name or rl_na_ticker,
                                    asset_class=rl_na_class,
                                    data_source=rl_na_source,
                                    bucket=rl_na_bucket,
                                    role=rl_na_role,
                                    is_active=True,
                                )
                                db.add(a_new)
                                db.flush()

                            # Semeia preço inicial no AssetPrice (v2) e Price (legado)
                            # para que o reprocessor e asset_performance encontrem o ativo
                            if rl_na_price > 0:
                                from app.models import AssetPrice as _AP, Price as _Price
                                # AssetPrice (v2)
                                existing_ap = (db.query(_AP)
                                               .filter_by(asset_id=a_new.id, date=mv_date)
                                               .first())
                                if existing_ap:
                                    existing_ap.price = rl_na_price
                                else:
                                    db.add(_AP(
                                        asset_id=a_new.id,
                                        date=mv_date,
                                        price=rl_na_price,
                                        source="manual",
                                    ))
                                # Price (legado — necessário para asset_performance)
                                existing_p = (db.query(_Price)
                                              .filter_by(asset_id=a_new.id, date=mv_date)
                                              .first())
                                if existing_p:
                                    existing_p.close_price = rl_na_price
                                else:
                                    db.add(_Price(
                                        asset_id=a_new.id,
                                        date=mv_date,
                                        close_price=rl_na_price,
                                        source="manual",
                                    ))
                                db.flush()

                            buy_exec.append({
                                "asset_id": a_new.id,
                                "ticker":   rl_na_ticker,
                                "value":    rl_na_val,
                                "quantity": rl_na_qty,
                                "price":    rl_na_price,
                            })

                        with st.spinner("Registrando realocação…"):
                            record_realocacao(
                                db, pid, mv_date,
                                sell_lines=sell_exec,
                                buy_lines=buy_exec,
                                description=mv_desc or "Realocação",
                            )
                            db.commit()
                        st.cache_data.clear()
                        st.success("✓ Realocação registrada.")
                        st.rerun()
                    except ValueError as exc:
                        db.rollback()
                        st.error(str(exc))
                    except Exception as exc:
                        db.rollback()
                        st.error(f"Erro: {exc}")
                        raise
                    finally:
                        db.close()

        # ─────────────────────────────────────────────────────────────────
        # RESGATE
        # ─────────────────────────────────────────────────────────────────
        elif mv_type == "resgate":
            if portfolio_vazio:
                st.warning("Portfólio vazio.")
            else:
                st.markdown("**Ativos a resgatar**")
                rg_lines: dict[int, dict] = {}
                for a in ativos:
                    p = _get_asset_price(key, a["id"], mv_date) or 0.0
                    c1, c2, c3 = st.columns([4, 2, 2])
                    with c1:
                        st.markdown(
                            f"<div style='padding-top:6px'>{a['name']} "
                            f"<span style='color:#ABABAB'>({a['ticker']})</span></div>",
                            unsafe_allow_html=True,
                        )
                    with c2:
                        v = st.number_input(
                            f"val_{a['id']}", label_visibility="collapsed",
                            min_value=0.0, step=10.0, format="%.2f",
                            key=f"{key}_rg_v_{a['id']}_{mv_date}",
                        )
                    with c3:
                        default_q = round(v / p, 6) if p > 0 and v > 0 else 0.0
                        q = st.number_input(
                            f"qty_{a['id']}", label_visibility="collapsed",
                            min_value=0.0, step=1.0, format="%.4f",
                            value=default_q,
                            key=f"{key}_rg_q_{a['id']}_{mv_date}",
                        )
                    if v > 0:
                        rg_lines[a["id"]] = {
                            "asset_id": a["id"], "ticker": a["ticker"],
                            "value": v,
                            "quantity": q if q > 0 else (v / p if p > 0 else 0),
                            "price": p,
                        }

                total_rg = sum(ln["value"] for ln in rg_lines.values())
                ok_rg    = total_rg > 0

                if st.button("💾 Confirmar Resgate", key=f"{key}_rg_save",
                             use_container_width=True, disabled=not ok_rg):
                    db = get_db_for(key)
                    try:
                        port = db.query(Portfolio).filter_by(key=key).first()
                        with st.spinner("Registrando resgate…"):
                            record_resgate(
                                db, port.id, mv_date,
                                sell_lines=list(rg_lines.values()),
                                description=mv_desc or "Resgate",
                            )
                            db.commit()
                        st.cache_data.clear()
                        st.success(f"✓ Resgate de {_cur(total_rg, currency)} registrado.")
                        st.rerun()
                    except ValueError as exc:
                        db.rollback()
                        st.error(str(exc))
                    except Exception as exc:
                        db.rollback()
                        st.error(f"Erro: {exc}")
                        raise
                    finally:
                        db.close()

        # ─────────────────────────────────────────────────────────────────
        # RENDIMENTO
        # ─────────────────────────────────────────────────────────────────
        elif mv_type == "rendimento":
            if portfolio_vazio:
                st.warning("Portfólio vazio.")
            else:
                labels  = [f"{a['name']} ({a['ticker']})" for a in ativos]
                by_lbl  = {f"{a['name']} ({a['ticker']})": a for a in ativos}
                rd_lbl  = st.selectbox("Ativo pagador", labels, key=f"{key}_rd_asset")
                rd_asset = by_lbl[rd_lbl]
                rd_sub  = st.radio("Tipo", ["Dividendo", "Cupom / Juros"],
                                   horizontal=True, key=f"{key}_rd_sub")
                rd_type = "dividendo" if "Dividendo" in rd_sub else "cupom"
                rd_amt  = st.number_input(
                    f"Valor recebido ({currency})",
                    min_value=0.01, step=10.0, format="%.2f",
                    key=f"{key}_rd_amt",
                )
                st.caption("O valor entra no caixa. Pesos não são alterados.")

                if st.button("💾 Registrar Rendimento", key=f"{key}_rd_save",
                             use_container_width=True):
                    db = get_db_for(key)
                    try:
                        port = db.query(Portfolio).filter_by(key=key).first()
                        record_rendimento(
                            db, port.id, mv_date,
                            asset_id=rd_asset["id"],
                            income_type=rd_type,
                            value=rd_amt,
                            description=mv_desc or f"{rd_type} {rd_asset['ticker']}",
                        )
                        db.commit()
                        st.cache_data.clear()
                        st.success(
                            f"✓ {rd_type.capitalize()} de {_cur(rd_amt, currency)} registrado."
                        )
                        st.rerun()
                    except ValueError as exc:
                        db.rollback()
                        st.error(str(exc))
                    except Exception as exc:
                        db.rollback()
                        st.error(f"Erro: {exc}")
                        raise
                    finally:
                        db.close()

        # Preço manual (para ativos ilíquidos ou sem cotação)
        st.divider()
        with st.expander("🏷️ Inserir preço manual de ativo", expanded=False):
            if not ativos:
                st.caption("Nenhum ativo cadastrado.")
            else:
                mp_labels = [f"{a['name']} ({a['ticker']})" for a in ativos]
                mp_sel    = st.selectbox("Ativo", mp_labels, key=f"{key}_mp_asset")
                mp_asset  = next(a for a in ativos
                                 if f"{a['name']} ({a['ticker']})" == mp_sel)
                mp_price  = st.number_input(
                    f"Preço ({currency})", min_value=0.0001, step=1.0,
                    format="%.4f", key=f"{key}_mp_price"
                )
                mp_date   = st.date_input("Data", value=date.today(),
                                          key=f"{key}_mp_date")

                if st.button("Salvar preço", key=f"{key}_mp_save"):
                    db = get_db_for(key)
                    try:
                        port = db.query(Portfolio).filter_by(key=key).first()
                        upsert_manual_price(db, mp_asset["id"], mp_date, mp_price)
                        db.flush()
                        recompute_from(db, port.id, mp_date)
                        db.commit()
                        st.cache_data.clear()
                        st.success(
                            f"✓ Preço de {mp_asset['ticker']} em {mp_date}: "
                            f"{_cur(mp_price, currency)}"
                        )
                        st.rerun()
                    except Exception as exc:
                        db.rollback()
                        st.error(f"Erro: {exc}")
                    finally:
                        db.close()

    # ═══════════════════════════════════════════════════════════════════════
    # TAB 2 — POSIÇÕES + PnL
    # ═══════════════════════════════════════════════════════════════════════
    with tab_pos:
        asset_snaps = _asset_snapshots_latest(key)

        if not asset_snaps:
            st.info("Nenhuma posição. Registre um **Aporte** primeiro.")
        else:
            snap = _latest_snapshot(key)
            snap_date = snap["date"] if snap else "—"

            st.caption(f"Última atualização: {snap_date}")

            rows = []
            for s in sorted(asset_snaps, key=lambda x: -x["market_value"]):
                total_invested = s["quantity"] * s["avg_cost"]
                total_ret_pct  = (s["total_pnl"] / total_invested
                                  if total_invested > 0 else None)
                rows.append({
                    "Ativo":          f"{s['name']} ({s['ticker']})",
                    "Qtd":            f"{s['quantity']:.4f}",
                    "Custo Médio":    _cur(s["avg_cost"], currency, 4),
                    "Preço atual":    _cur(s["price"], currency, 4),
                    "Val. Mercado":   _cur(s["market_value"], currency),
                    "PnL não-real.":  _cur(s["unrealized"], currency),
                    "PnL realizado":  _cur(s["realized"], currency),
                    "Dividendos":     _cur(s["dividends"], currency),
                    "PnL total":      _cur(s["total_pnl"], currency),
                    "Retorno":        _pct(total_ret_pct),
                })

            df = pd.DataFrame(rows)
            st.dataframe(df, use_container_width=True, hide_index=True)

            # Totais
            total_mkt  = sum(s["market_value"]  for s in asset_snaps)
            total_unrl = sum(s["unrealized"]     for s in asset_snaps)
            total_rel  = sum(s["realized"]       for s in asset_snaps)
            total_div  = sum(s["dividends"]      for s in asset_snaps)
            total_pnl  = sum(s["total_pnl"]      for s in asset_snaps)

            st.divider()
            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("Val. de Mercado",  _cur(total_mkt,  currency))
            c2.metric("PnL Não Real.",    _cur(total_unrl, currency),
                      delta=f"{total_unrl:+.2f}")
            c3.metric("PnL Realizado",    _cur(total_rel,  currency))
            c4.metric("Dividendos/Cupons",_cur(total_div,  currency))
            c5.metric("PnL Total",        _cur(total_pnl,  currency))

    # ═══════════════════════════════════════════════════════════════════════
    # TAB 3 — DIÁRIO (transações por dia + edição)
    # ═══════════════════════════════════════════════════════════════════════
    with tab_diario:
        edit_key = f"{key}_editing_tx_id"

        db = get_db_for(key)
        try:
            port = db.query(Portfolio).filter_by(key=key).first()
            if port is None:
                st.info("Portfólio não inicializado.")
            else:
                all_txs = get_transactions(db, port.id, include_void=False)
                tx_data = [
                    {
                        "id":       tx.id,
                        "date":     tx.date,
                        "type":     tx.type,
                        "ticker":   tx.asset.ticker if tx.asset else "—",
                        "name":     tx.asset.name   if tx.asset else "—",
                        "asset_id": tx.asset_id,
                        "qty":      tx.quantity,
                        "price":    tx.price,
                        "value":    tx.value,
                        "impact":   tx.cash_impact,
                        "desc":     tx.description or "",
                    }
                    for tx in all_txs
                ]
        finally:
            db.close()

        if not tx_data:
            st.info("Nenhuma transação. Use **➕ Novo Movimento**.")
        else:
            tx_data.sort(key=lambda t: (t["date"], t["id"]), reverse=True)

            # ── Formulário de edição ─────────────────────────────────────
            editing_id = st.session_state.get(edit_key)
            if editing_id is not None:
                tx_ed = next((t for t in tx_data if t["id"] == editing_id), None)
                if tx_ed:
                    st.warning(f"✏️ Editando transação **#{editing_id}** "
                               f"({tx_ed['type']} · {tx_ed['date']})")

                    ed_c1, ed_c2, ed_c3 = st.columns(3)
                    with ed_c1:
                        ed_val = st.number_input(
                            "Valor", min_value=0.0, step=10.0, format="%.2f",
                            value=float(tx_ed["value"] or 0),
                            key=f"{key}_ed_val",
                        )
                    with ed_c2:
                        ed_qty = st.number_input(
                            "Quantidade", min_value=0.0, step=1.0, format="%.4f",
                            value=float(tx_ed["qty"] or 0),
                            key=f"{key}_ed_qty",
                        )
                    with ed_c3:
                        ed_price = st.number_input(
                            "Preço unit.", min_value=0.0, step=0.01, format="%.4f",
                            value=float(tx_ed["price"] or 0),
                            key=f"{key}_ed_price",
                        )
                    ed_desc = st.text_input(
                        "Descrição", value=tx_ed["desc"],
                        key=f"{key}_ed_desc",
                    )

                    col_s, col_c = st.columns(2)
                    with col_s:
                        if st.button("💾 Salvar correção",
                                     key=f"{key}_ed_save",
                                     use_container_width=True):
                            db = get_db_for(key)
                            try:
                                from app.services.ledger import edit_transaction
                                with st.spinner("Aplicando correção…"):
                                    edit_transaction(
                                        db, editing_id,
                                        value=ed_val,
                                        quantity=ed_qty or None,
                                        price=ed_price or None,
                                        description=ed_desc,
                                    )
                                    db.commit()
                                st.session_state.pop(edit_key, None)
                                st.cache_data.clear()
                                st.success("✓ Transação corrigida e portfólio recalculado.")
                                st.rerun()
                            except ValueError as exc:
                                db.rollback()
                                st.error(str(exc))
                            except Exception as exc:
                                db.rollback()
                                st.error(f"Erro: {exc}")
                                raise
                            finally:
                                db.close()
                    with col_c:
                        if st.button("✕ Cancelar", key=f"{key}_ed_cancel",
                                     use_container_width=True):
                            st.session_state.pop(edit_key, None)
                            st.rerun()
                    st.divider()

            # ── Lista de transações por dia ──────────────────────────────
            type_icons = {
                "aporte":     "💰",  "retirada":  "📤",
                "compra":     "📈",  "venda":     "📉",
                "dividendo":  "🎁",  "cupom":     "🎁",
                "amortizacao":"🔄",  "despesa":   "💸",
                "ajuste_preco":"🔧", "ajuste_qty":"🔧",
            }

            void_confirm_key = f"{key}_void_confirm_id"

            for d, grp in groupby(tx_data, key=lambda t: t["date"]):
                day_txs = list(grp)
                st.markdown(
                    f"<div style='font-weight:700;border-left:4px solid {_ACCENT};"
                    f"padding-left:10px;margin:20px 0 6px'>📅 {d}</div>",
                    unsafe_allow_html=True,
                )
                for tx in day_txs:
                    icon  = type_icons.get(tx["type"], "·")
                    label = f"{icon} {tx['type'].capitalize()}"
                    if tx["ticker"] != "—":
                        label += f" · {tx['ticker']}"
                    label += f" · {_cur(tx['value'], currency)}"

                    is_editing     = (editing_id == tx["id"])
                    is_confirming  = (st.session_state.get(void_confirm_key) == tx["id"])

                    with st.expander(
                        f"{'✏️ ' if is_editing else ''}{'🗑️ ' if is_confirming else ''}{label}"
                        + (f"  —  {tx['desc']}" if tx["desc"] else ""),
                        expanded=is_confirming,
                    ):
                        meta_col, btn_col = st.columns([6, 1])
                        with meta_col:
                            parts = [f"ID #{tx['id']}"]
                            if tx["qty"]:
                                parts.append(f"qty {tx['qty']:.4f}")
                            if tx["price"]:
                                parts.append(f"@ {_cur(tx['price'], currency, 4)}")
                            impact_color = _GREEN if tx["impact"] >= 0 else _RED
                            parts.append(
                                f"<span style='color:{impact_color}'>"
                                f"caixa {tx['impact']:+.2f}</span>"
                            )
                            st.markdown(
                                " · ".join(parts),
                                unsafe_allow_html=True,
                            )
                        with btn_col:
                            if not is_editing and not is_confirming:
                                bcol1, bcol2 = st.columns(2)
                                with bcol1:
                                    if st.button("✏️", key=f"{key}_edit_{tx['id']}",
                                                 help="Editar transação"):
                                        st.session_state[edit_key] = tx["id"]
                                        st.rerun()
                                with bcol2:
                                    if st.button("🗑", key=f"{key}_void_req_{tx['id']}",
                                                 help="Anular transação"):
                                        st.session_state[void_confirm_key] = tx["id"]
                                        st.rerun()
                            elif is_editing:
                                st.caption("editando")

                        # ── Confirmação de anulação ──────────────────────
                        if is_confirming:
                            st.warning(
                                f"⚠️ Anular **#{tx['id']} · {tx['type']} · "
                                f"{_cur(tx['value'], currency)}**?  \n"
                                "O portfólio será recalculado a partir desta data."
                            )
                            conf_yes, conf_no = st.columns(2)
                            with conf_yes:
                                if st.button(
                                    "✓ Confirmar anulação",
                                    key=f"{key}_void_yes_{tx['id']}",
                                    type="primary",
                                    use_container_width=True,
                                ):
                                    db2 = get_db_for(key)
                                    try:
                                        with st.spinner("Anulando e recalculando…"):
                                            void_transaction(
                                                db2, tx["id"],
                                                note="anulação manual pelo usuário",
                                            )
                                            db2.commit()
                                        st.session_state.pop(void_confirm_key, None)
                                        st.cache_data.clear()
                                        st.success(
                                            f"✓ Transação #{tx['id']} anulada. "
                                            "Snapshots recalculados."
                                        )
                                        st.rerun()
                                    except Exception as exc:
                                        db2.rollback()
                                        st.error(f"Erro: {exc}")
                                    finally:
                                        db2.close()
                            with conf_no:
                                if st.button(
                                    "✕ Cancelar",
                                    key=f"{key}_void_no_{tx['id']}",
                                    use_container_width=True,
                                ):
                                    st.session_state.pop(void_confirm_key, None)
                                    st.rerun()

    # ═══════════════════════════════════════════════════════════════════════
    # TAB 4 — EXTRATO (ledger completo)
    # ═══════════════════════════════════════════════════════════════════════
    with tab_extrato:
        db = get_db_for(key)
        try:
            port = db.query(Portfolio).filter_by(key=key).first()
            if port is None:
                st.info("Portfólio não inicializado.")
            else:
                all_txs_ext = get_transactions(db, port.id, include_void=False)
                ext_data = sorted(
                    [
                        {
                            "date":   tx.date,
                            "type":   tx.type,
                            "ticker": tx.asset.ticker if tx.asset else "—",
                            "value":  tx.value,
                            "impact": tx.cash_impact,
                            "qty":    tx.quantity,
                            "price":  tx.price,
                            "desc":   tx.description or "",
                        }
                        for tx in all_txs_ext
                    ],
                    key=lambda t: (t["date"], ),
                )

                # Running cash balance
                snap_ext = _latest_snapshot(key)
                bal_label = _cur(snap_ext["cash"], currency) if snap_ext else "—"
                st.markdown(
                    f"<div style='font-size:0.73rem;color:#ABABAB'>Saldo em Caixa</div>"
                    f"<div style='font-size:2rem;font-weight:800;color:#10b981'>"
                    f"{bal_label}</div>",
                    unsafe_allow_html=True,
                )

                if not ext_data:
                    st.info("Nenhum lançamento.")
                else:
                    running = 0.0
                    rows = []
                    for t in ext_data:
                        running += t["impact"]
                        rows.append({
                            "Data":       str(t["date"]),
                            "Tipo":       t["type"].capitalize(),
                            "Ativo":      t["ticker"],
                            "Valor":      _cur(t["value"], currency),
                            "Impacto":    f"{t['impact']:+.2f}",
                            "Saldo após": _cur(running, currency),
                            "Qtd":        f"{t['qty']:.4f}" if t["qty"] else "—",
                            "Preço":      _cur(t["price"], currency, 4) if t["price"] else "—",
                            "Obs.":       t["desc"],
                        })
                    rows_rev = list(reversed(rows))

                    st.dataframe(
                        pd.DataFrame(rows_rev),
                        use_container_width=True,
                        hide_index=True,
                    )
        finally:
            db.close()
