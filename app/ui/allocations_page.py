"""
Allocations page — gestão de posições do portfolio.

Tabs:
  📊 Posições          — visão atual + diagnóstico de erros
  🔄 Nova Movimentação — criar novo evento de alteração no portfolio
  📋 Cotação Manual    — upload Excel ou lançamento avulso
  📝 Movimentações     — histórico de eventos, editável por data
"""

from datetime import date, timedelta
import hashlib
import io
import json

import pandas as pd
import streamlit as st

from app.database import get_db_for
from app.models import Asset, AllocationLog, Allocation, Price
from app.portfolio.engine import (
    update_allocations,
    get_active_allocations,
    get_actual_weights,
    refresh_prices,
    build_snapshots,
    upsert_manual_price,
    get_prices_df,
)
from app.ui.styles import inject_dot_css


# ─────────────────────────────────────────────────────────────────────────────
# helpers
# ─────────────────────────────────────────────────────────────────────────────

from app.config import DATA_DIR


# ── Bucket targets (posição estratégica por categoria) ────────────────────────

def _bucket_targets_path(key: str):
    return DATA_DIR / f"bucket_targets_{key}.json"

def _load_bucket_targets(key: str, buckets: list) -> dict:
    """Retorna {bucket: peso_estratégico} como float 0–1."""
    p = _bucket_targets_path(key)
    if p.exists():
        try:
            data = json.loads(p.read_text())
            return {b: float(data.get(b, 0.0)) for b in buckets}
        except Exception:
            pass
    return {b: 0.0 for b in buckets}

def _save_bucket_targets(key: str, targets: dict) -> None:
    _bucket_targets_path(key).write_text(json.dumps(targets))


# ── Acknowledged alerts ───────────────────────────────────────────────────────

def _ack_path(key: str):
    return DATA_DIR / f"acknowledged_{key}.json"

def _load_acked(key: str) -> set:
    p = _ack_path(key)
    if p.exists():
        try:
            return set(json.loads(p.read_text()))
        except Exception:
            return set()
    return set()

def _save_acked(key: str, acked: set) -> None:
    _ack_path(key).write_text(json.dumps(list(acked)))

def _problem_id(p: dict) -> str:
    """Stable hash for a problem dict."""
    raw = f"{p['tipo']}|{p['data']}|{p['detalhe']}"
    return hashlib.md5(raw.encode()).hexdigest()[:12]

def _pct(v):
    if v is None:
        return "—"
    return f"{v * 100:.2f}%"


def _load_positions(key: str) -> list[dict]:
    db = get_db_for(key)
    try:
        today  = date.today()
        actual = get_actual_weights(db, today)

        # ── V2: lê de AssetSnapshot (fonte de verdade do motor de transações) ──
        try:
            from app.models import (
                Portfolio as _PV2,
                AssetSnapshot as _AS,
                DailySnapshot as _DS,
            )
            port_v2 = db.query(_PV2).filter_by(key=key).first()
            if port_v2 is not None:
                day = (
                    db.query(_DS)
                      .filter_by(portfolio_id=port_v2.id)
                      .order_by(_DS.date.desc())
                      .first()
                )
                if day and day.nav and day.nav > 0:
                    snaps = (
                        db.query(_AS)
                          .filter_by(portfolio_id=port_v2.id, date=day.date)
                          .all()
                    )
                    rows = []
                    seen_ids: set[int] = set()
                    for s in snaps:
                        if (s.asset is None
                                or s.quantity <= 1e-9
                                or s.asset.data_source == "cash_account"
                                or s.asset_id in seen_ids):
                            continue
                        seen_ids.add(s.asset_id)
                        w_actual = (s.market_value / day.nav
                                    if s.market_value and day.nav > 0 else 0.0)
                        # Peso-alvo: só da Allocation explícita.
                        # NÃO usa actual como fallback — misturar os dois sistemas
                        # faz o total ultrapassar 100% quando há ativos sem Allocation.
                        from app.models import Allocation as _Alloc
                        alloc = (
                            db.query(_Alloc)
                              .filter(
                                  _Alloc.asset_id == s.asset_id,
                                  _Alloc.end_date.is_(None),
                              )
                              .first()
                        )
                        rows.append({
                            "asset_id":    s.asset_id,
                            "ticker":      s.asset.ticker,
                            "name":        s.asset.name,
                            "bucket":      s.asset.bucket or "—",
                            "role":        s.asset.role or "beta",
                            "asset_class": s.asset.asset_class,
                            "data_source": s.asset.data_source,
                            "target_w":    alloc.weight if alloc else None,
                            "actual_w":    w_actual,
                            "no_price":    (s.market_value or 0) < 1e-6 and s.quantity > 1e-9,
                        })
                    if rows:
                        return rows
        except Exception:
            pass  # fallback para legado

        # ── Legado: Allocation table ──────────────────────────────────────
        active = get_active_allocations(db, today)
        return [
            {
                "asset_id":    a.asset_id,
                "ticker":      a.asset.ticker,
                "name":        a.asset.name,
                "bucket":      a.asset.bucket or "—",
                "role":        a.asset.role or "beta",
                "asset_class": a.asset.asset_class,
                "data_source": a.asset.data_source,
                "target_w":    a.weight,
                "actual_w":    actual.get(a.asset.ticker, a.weight),
                "no_price":    False,
            }
            for a in active
        ]
    finally:
        db.close()


def _run_diagnostics(key: str, start_date_str: str) -> list[dict]:
    """
    Retorna lista de problemas encontrados:
    - Datas em que o total de pesos-alvo != 100%
    - Datas em que algum ativo ativo não tem cotação
    """
    problems = []
    db = get_db_for(key)
    try:
        from app.models import PortfolioSnapshot
        from sqlalchemy import func

        start = date.fromisoformat(start_date_str)
        today = date.today()

        # 1. Verificar se pesos somam 100% em cada período de alocação
        all_allocs = db.query(Allocation).order_by(Allocation.start_date).all()
        # Coletar todas as start_dates únicas
        rebalance_dates = sorted(set(a.start_date for a in all_allocs))
        for rd in rebalance_dates:
            active = get_active_allocations(db, rd)
            total = sum(a.weight for a in active)
            if abs(total - 1.0) > 0.005:
                problems.append({
                    "tipo":    "⚠️ Pesos != 100%",
                    "data":    str(rd),
                    "detalhe": f"Total = {total*100:.2f}% (diferença: {(total-1)*100:+.2f}%)"
                })

        # 2. Verificar preços faltando para ativos ativos
        active_today = get_active_allocations(db, today)
        prices_df = get_prices_df(db, start, today)

        if not prices_df.empty:
            prices_df_ffill = prices_df.copy()  # não ffill — queremos ver gaps reais
            for a in active_today:
                ticker = a.asset.ticker
                if a.asset.data_source == "manual":
                    continue  # manual verificado separadamente abaixo (check 4)
                if ticker not in prices_df.columns:
                    problems.append({
                        "tipo":    "❌ Sem preços",
                        "data":    str(start),
                        "detalhe": f"{ticker} não tem nenhuma cotação no sistema"
                    })
                    continue
                col = prices_df[ticker]
                missing_dates = col[col.isna()].index.tolist()
                if missing_dates:
                    problems.append({
                        "tipo":    "⚠️ Cotações faltando",
                        "data":    f"{missing_dates[0]} → {missing_dates[-1]}",
                        "detalhe": f"{ticker}: {len(missing_dates)} dia(s) sem cotação"
                    })

        # 3b. Verificar ativos v2 (AssetSnapshot) sem cotação em AssetPrice
        # Ativos manuais (fundos, privado) sem cotação fazem o reprocessor usar
        # o preço da transação como fallback — o usuário deve ser alertado.
        try:
            from app.models import (
                Portfolio as _PV2,
                AssetSnapshot as _AS,
                DailySnapshot as _DS,
                AssetPrice as _AP,
            )
            port_v2 = db.query(_PV2).filter_by(key=key).first()
            if port_v2 is not None:
                # Pega o snapshot mais recente
                latest_day = (
                    db.query(_DS)
                      .filter_by(portfolio_id=port_v2.id)
                      .order_by(_DS.date.desc())
                      .first()
                )
                if latest_day:
                    active_snaps = (
                        db.query(_AS)
                          .filter_by(portfolio_id=port_v2.id,
                                     date=latest_day.date)
                          .all()
                    )
                    for s in active_snaps:
                        if s.asset is None or s.quantity <= 1e-9:
                            continue
                        if s.asset.data_source == "cash_account":
                            continue

                        # Conta quantas cotações existem em AssetPrice
                        n_prices = (
                            db.query(_AP)
                              .filter_by(asset_id=s.asset_id)
                              .count()
                        )
                        last_price = (
                            db.query(_AP)
                              .filter_by(asset_id=s.asset_id)
                              .order_by(_AP.date.desc())
                              .first()
                        )

                        if n_prices == 0:
                            problems.append({
                                "tipo":    "❌ Sem cotação — usando preço de compra",
                                "data":    str(latest_day.date),
                                "detalhe": (
                                    f"{s.asset.ticker} ({s.asset.name}) não tem cotações "
                                    f"em asset_prices. O reprocessor usa o preço da transação "
                                    f"como fallback — snapshots podem estar incorretos. "
                                    f"Insira cotações na aba Cotação Manual."
                                ),
                            })
                        elif last_price and (today - last_price.date).days > 5:
                            # Cotação desatualizada (> 5 dias úteis)
                            if s.asset.data_source == "manual":
                                problems.append({
                                    "tipo":    "⚠️ Cotação desatualizada",
                                    "data":    str(last_price.date),
                                    "detalhe": (
                                        f"{s.asset.ticker} ({s.asset.name}): última cotação "
                                        f"em {last_price.date} ({(today - last_price.date).days} dias atrás). "
                                        f"Atualize via Cotação Manual."
                                    ),
                                })
        except Exception:
            pass  # não bloquear diagnóstico se v2 não disponível

        # 3. Verificar se há linhas duplicadas abertas (end_date IS NULL) para o mesmo ativo
        open_allocs = db.query(Allocation).filter(Allocation.end_date.is_(None)).all()
        by_asset: dict[int, list] = {}
        for a in open_allocs:
            by_asset.setdefault(a.asset_id, []).append(a)
        for asset_id, rows in by_asset.items():
            if len(rows) > 1:
                asset = db.query(Asset).filter_by(id=asset_id).first()
                ticker = asset.ticker if asset else str(asset_id)
                total_w = sum(r.weight for r in rows)
                problems.append({
                    "tipo":    "❌ Alocação duplicada",
                    "data":    "hoje",
                    "detalhe": f"{ticker} tem {len(rows)} linhas abertas (total={total_w*100:.2f}%)"
                })

    finally:
        db.close()
    return problems


# ─────────────────────────────────────────────────────────────────────────────
# page
# ─────────────────────────────────────────────────────────────────────────────

def render_allocations(portfolio_cfg: dict) -> None:
    key        = portfolio_cfg["key"]
    buckets    = portfolio_cfg["buckets"]
    start_str  = portfolio_cfg["start_date"]

    st.set_page_config(
        page_title=f"Allocations · {portfolio_cfg['name']}",
        page_icon="⬤",
        layout="wide",
    )
    inject_dot_css()
    st.markdown(
        f"<h1>{portfolio_cfg['flag']} {portfolio_cfg['name']}</h1>"
        f"<p style='color:#ABABAB;font-size:0.8rem;margin-top:-8px;'>Gestão de Posições</p>",
        unsafe_allow_html=True,
    )

    positions = _load_positions(key)

    tab_pos, tab_pnl, tab_manual = st.tabs([
        "📊  Posições",
        "📈  PnL",
        "📋  Cotação Manual",
    ])

    # ═══════════════════════════════════════════════════════════════════════
    # TAB 1 — POSIÇÕES + DIAGNÓSTICO
    # ═══════════════════════════════════════════════════════════════════════
    with tab_pos:

        # ── Diagnóstico de erros ───────────────────────────────────────────
        all_problems = _run_diagnostics(key, start_str)
        acked        = _load_acked(key)

        # Separar ativos vs autorizados
        active_problems = [p for p in all_problems if _problem_id(p) not in acked]
        acked_problems  = [p for p in all_problems if _problem_id(p) in acked]

        if active_problems:
            st.error(f"**{len(active_problems)} problema(s) ativo(s):**")
            for p in active_problems:
                pid = _problem_id(p)
                col_txt, col_btn1, col_btn2 = st.columns([6, 1.5, 1.5])
                with col_txt:
                    st.markdown(
                        f"**{p['tipo']}** &nbsp;·&nbsp; `{p['data']}`  \n"
                        f"<span style='color:#929292;font-size:0.85em'>{p['detalhe']}</span>",
                        unsafe_allow_html=True,
                    )
                with col_btn1:
                    # Reparo automático só para duplicatas
                    if "duplicada" in p["tipo"]:
                        if st.button("🔧 Reparar", key=f"{key}_repair_{pid}"):
                            db = get_db_for(key)
                            try:
                                open_allocs = db.query(Allocation).filter(Allocation.end_date.is_(None)).all()
                                by_asset: dict[int, list] = {}
                                for a in open_allocs:
                                    by_asset.setdefault(a.asset_id, []).append(a)
                                fixed = 0
                                for asset_id, rows in by_asset.items():
                                    if len(rows) > 1:
                                        keeper = sorted(rows, key=lambda r: (r.start_date, r.id), reverse=True)[0]
                                        for dup in rows:
                                            if dup.id != keeper.id:
                                                dup.end_date = keeper.start_date
                                                fixed += 1
                                db.commit()
                                build_snapshots(db, force_rebuild=True, start_date_str=start_str,
                                                base_value=portfolio_cfg["base_value"])
                                st.cache_data.clear()
                                st.success(f"✓ {fixed} duplicata(s) reparada(s).")
                                st.rerun()
                            finally:
                                db.close()
                with col_btn2:
                    if st.button("✓ Autorizar", key=f"{key}_ack_{pid}",
                                 help="Reconhecer este problema e ocultar o alerta"):
                        acked.add(pid)
                        _save_acked(key, acked)
                        st.rerun()

                st.markdown("<hr style='margin:6px 0;border-color:#E8E8E8'>", unsafe_allow_html=True)

        elif all_problems:
            # Só há problemas autorizados — mostrar aviso suave
            st.info(f"✓ Sem alertas ativos. {len(acked_problems)} problema(s) autorizado(s).")
        else:
            st.success("✓ Nenhum problema encontrado.")

        # Painel de problemas autorizados (colapsável)
        if acked_problems:
            with st.expander(f"🔕 Alertas autorizados ({len(acked_problems)})", expanded=False):
                for p in acked_problems:
                    pid = _problem_id(p)
                    col_txt, col_btn = st.columns([7, 1.5])
                    with col_txt:
                        st.markdown(
                            f"~~{p['tipo']}~~ &nbsp;·&nbsp; `{p['data']}`  \n"
                            f"<span style='color:#ABABAB;font-size:0.85em'>{p['detalhe']}</span>",
                            unsafe_allow_html=True,
                        )
                    with col_btn:
                        if st.button("↩ Reativar", key=f"{key}_unack_{pid}",
                                     help="Remover autorização e reativar o alerta"):
                            acked.discard(pid)
                            _save_acked(key, acked)
                            st.rerun()
                    st.markdown("<hr style='margin:4px 0;border-color:#E8E8E8'>", unsafe_allow_html=True)

        if all_problems:
            st.divider()

        # ── Posição Estratégica por bucket ────────────────────────────────
        bucket_targets = _load_bucket_targets(key, buckets)
        with st.expander("⚙️ Posição Estratégica por categoria", expanded=False):
            st.caption(
                "Defina o percentual estratégico alvo de cada categoria. "
                "Este valor aparece no dashboard ao lado da posição atual."
            )
            new_targets = {}
            cols = st.columns(len(buckets))
            for i, bucket in enumerate(buckets):
                with cols[i]:
                    new_targets[bucket] = st.number_input(
                        bucket,
                        min_value=0.0,
                        max_value=100.0,
                        value=round(bucket_targets.get(bucket, 0.0) * 100, 1),
                        step=1.0,
                        format="%.1f",
                        key=f"{key}_bt_{bucket}",
                    )
            total_bt = sum(new_targets.values())
            color_bt = "#10b981" if abs(total_bt - 100.0) < 0.1 else "#ef4444"
            st.markdown(
                f"<span style='font-size:0.85rem;color:{color_bt}'>"
                f"Total: {total_bt:.1f}%"
                f"{'  ✓' if abs(total_bt - 100.0) < 0.1 else '  — deve somar 100%'}"
                f"</span>",
                unsafe_allow_html=True,
            )
            if st.button("💾 Salvar posições estratégicas", key=f"{key}_bt_save",
                         use_container_width=True):
                _save_bucket_targets(key, {b: v / 100.0 for b, v in new_targets.items()})
                st.success("✓ Posições estratégicas salvas.")
                st.rerun()

        st.divider()

        # ── Tabela de posições ─────────────────────────────────────────────
        if not positions:
            st.info("Nenhuma posição ativa.")
        else:
            bucket_order = {b: i for i, b in enumerate(buckets)}
            sorted_pos   = sorted(
                positions,
                key=lambda r: (bucket_order.get(r["bucket"], 99), r["role"], r["name"])
            )
            n_no_price = sum(1 for p in positions if p.get("no_price"))

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

            th = (
                "<style>"
                ".ap td,th{padding:6px 12px;font-size:0.83rem;vertical-align:middle;}"
                ".ap th{color:#929292;font-weight:500;border-bottom:1px solid #E8E8E8;"
                "white-space:nowrap}"
                ".ap td{border-bottom:1px solid #F4F4F4;white-space:nowrap}"
                ".ap tr:last-child td{border-bottom:none}"
                "</style>"
                "<table class='ap' style='width:100%;border-collapse:collapse'>"
                "<thead><tr>"
                "<th style='width:40%'>Ativo</th>"
                "<th>Ticker</th>"
                "<th>Fator</th>"
                "<th>Posição Atual</th>"
                "<th>Fonte</th>"
                "</tr></thead><tbody>"
            )

            rows_html = ""
            for p in sorted_pos:
                no_price = p.get("no_price", False)
                badge    = _BADGE.get((p["role"] or "beta").lower(), "")
                peso     = (
                    "<span style='color:#f59e0b'>⚠️ sem cotação</span>"
                    if no_price
                    else f"<span style='color:#5E5E5E'>{_pct(p['actual_w'])}</span>"
                )
                rows_html += (
                    f"<tr>"
                    f"<td>{badge}{p['name']}</td>"
                    f"<td style='color:#929292'>{p['ticker']}</td>"
                    f"<td style='color:#929292'>{p['bucket']}</td>"
                    f"<td>{peso}</td>"
                    f"<td style='color:#ABABAB;font-size:0.8rem'>{p['data_source']}</td>"
                    f"</tr>"
                )

            st.markdown(
                th + rows_html + "</tbody></table>",
                unsafe_allow_html=True,
            )
            st.markdown("<div style='margin-bottom:8px'></div>", unsafe_allow_html=True)

            total_actual_priced = sum(
                p["actual_w"] for p in positions if not p.get("no_price")
            )
            c_act = "green" if abs(total_actual_priced - 1.0) < 0.005 else "red"

            if n_no_price > 0:
                st.markdown(
                    f"<span style='color:#5E5E5E;font-weight:700'>"
                    f"Posição atual (c/ cotação): {total_actual_priced*100:.2f}%</span>"
                    f"&nbsp;&nbsp;"
                    f"<span style='color:#f59e0b;font-size:0.85rem'>"
                    f"⚠️ {n_no_price} ativo(s) sem cotação — insira em Cotação Manual</span>",
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    f"<span style='color:{c_act};font-weight:700'>"
                    f"Posição atual total: {total_actual_priced*100:.2f}%</span>",
                    unsafe_allow_html=True,
                )

    # ═══════════════════════════════════════════════════════════════════════
    # TAB 2 — PnL
    # ═══════════════════════════════════════════════════════════════════════
    with tab_pnl:
        from app.models import (
            Portfolio as _PV2,
            DailySnapshot as _DS,
            AssetSnapshot as _AS,
            Transaction as _TX,
        )
        import plotly.graph_objects as _go

        cur = portfolio_cfg.get("currency", "BRL")
        sym = "R$" if cur == "BRL" else "$"

        def _fc(v):
            """Format currency value."""
            if v is None:
                return "—"
            sign = "+" if v > 0 else ""
            return f"{sign}{sym} {v:,.2f}"

        def _fp(v, decimals=2):
            """Format percentage."""
            if v is None:
                return "—"
            sign = "+" if v >= 0 else ""
            return f"{sign}{v * 100:.{decimals}f}%"

        db_pnl = get_db_for(key)
        try:
            port_v2 = db_pnl.query(_PV2).filter_by(key=key).first()
            if port_v2 is None:
                st.info("Portfólio v2 não encontrado. Registre movimentações primeiro.")
            else:
                daily_snaps = (
                    db_pnl.query(_DS)
                      .filter_by(portfolio_id=port_v2.id)
                      .order_by(_DS.date)
                      .all()
                )
                asset_snaps = (
                    db_pnl.query(_AS)
                      .filter_by(portfolio_id=port_v2.id)
                      .order_by(_AS.date, _AS.asset_id)
                      .all()
                )

                if not daily_snaps:
                    st.info("Nenhum snapshot disponível. Execute um Refresh na Home.")
                else:
                    pnl_port, pnl_ativos = st.tabs(["🏦 Portfólio", "📋 Ativos"])

                    # ── SUB-TAB: Portfólio ────────────────────────────────
                    with pnl_port:
                        # Build portfolio PnL table
                        port_rows = []
                        prev_nav = None
                        for i, s in enumerate(daily_snaps):
                            nav_open = prev_nav if prev_nav is not None else s.nav
                            # daily PnL (R$) ex-flow = nav_close - net_flow - nav_open
                            if prev_nav is not None and prev_nav > 0:
                                daily_pnl_brl = (s.nav - (s.net_flow or 0.0)) - prev_nav
                            else:
                                daily_pnl_brl = 0.0

                            port_rows.append({
                                "Data":           str(s.date),
                                "NAV":            s.nav,
                                "Caixa":          s.cash,
                                "Investido":      s.invested_value,
                                "Fluxo Líq.":     s.net_flow or 0.0,
                                "Retorno (%)":    s.daily_return,
                                "PnL Dia (R$)":   daily_pnl_brl,
                                "Cota":           s.cota,
                                "Ret. Acum. (%)": s.cum_return,
                            })
                            prev_nav = s.nav

                        port_df = pd.DataFrame(port_rows)

                        # KPI cards at top
                        if len(daily_snaps) >= 1:
                            latest = daily_snaps[-1]
                            first  = daily_snaps[0]
                            total_pnl = (latest.nav - (latest.nav / (1 + (latest.cum_return or 0))))
                            k1, k2, k3, k4, k5 = st.columns(5)
                            k1.metric("NAV Atual", f"{sym} {latest.nav:,.2f}")
                            k2.metric("Caixa", f"{sym} {latest.cash:,.2f}")
                            k3.metric("Investido", f"{sym} {latest.invested_value:,.2f}")
                            k4.metric("Ret. Acumulado", _fp(latest.cum_return))
                            k5.metric("Cota", f"{latest.cota:.4f}")
                            st.divider()

                        # NAV + Cota chart
                        if len(daily_snaps) > 1:
                            dates_c = [s.date for s in daily_snaps]
                            navs_c  = [s.nav   for s in daily_snaps]
                            cotas_c = [s.cota  for s in daily_snaps]

                            fig_pnl = _go.Figure()
                            fig_pnl.add_trace(_go.Scatter(
                                x=dates_c, y=navs_c,
                                name="NAV (R$)", mode="lines",
                                line=dict(color="#FA9B5A", width=2),
                                fill="tozeroy",
                                fillcolor="rgba(250,155,90,0.08)",
                                yaxis="y1",
                            ))
                            fig_pnl.add_trace(_go.Scatter(
                                x=dates_c, y=cotas_c,
                                name="Cota", mode="lines",
                                line=dict(color="#3b82f6", width=1.5, dash="dot"),
                                yaxis="y2",
                            ))
                            fig_pnl.update_layout(
                                height=220,
                                margin=dict(l=0, r=0, t=4, b=0),
                                plot_bgcolor="#FFFFFF",
                                paper_bgcolor="rgba(0,0,0,0)",
                                xaxis=dict(showgrid=False,
                                           tickfont=dict(size=10, color="#929292")),
                                yaxis=dict(title="NAV", showgrid=True,
                                           gridcolor="#F4F4F4",
                                           tickfont=dict(size=10, color="#929292")),
                                yaxis2=dict(title="Cota", overlaying="y", side="right",
                                            tickfont=dict(size=10, color="#3b82f6"),
                                            showgrid=False),
                                legend=dict(orientation="h", y=1.05, x=0,
                                            font=dict(size=11, color="#929292")),
                                hovermode="x unified",
                            )
                            st.plotly_chart(fig_pnl, use_container_width=True,
                                            key=f"{key}_pnl_nav_chart")
                            st.divider()

                        # Table: format for display
                        disp_df = pd.DataFrame({
                            "Data":            port_df["Data"],
                            "NAV":             port_df["NAV"].apply(lambda v: f"{sym} {v:,.2f}"),
                            "Caixa":           port_df["Caixa"].apply(lambda v: f"{sym} {v:,.2f}"),
                            "Investido":       port_df["Investido"].apply(lambda v: f"{sym} {v:,.2f}"),
                            "Fluxo Externo":   port_df["Fluxo Líq."].apply(
                                lambda v: f"{sym} {v:+,.2f}" if abs(v) > 0.01 else "—"),
                            "PnL Dia":         port_df["PnL Dia (R$)"].apply(
                                lambda v: f"{sym} {v:+,.2f}" if abs(v) > 0.01 else "—"),
                            "Retorno Dia":     port_df["Retorno (%)"].apply(
                                lambda v: f"{v*100:+.3f}%" if v is not None else "—"),
                            "Cota":            port_df["Cota"].apply(lambda v: f"{v:.4f}"),
                            "Ret. Acumulado":  port_df["Ret. Acum. (%)"].apply(
                                lambda v: f"{v*100:+.2f}%" if v is not None else "—"),
                        })
                        # Show newest first
                        st.dataframe(
                            disp_df.iloc[::-1].reset_index(drop=True),
                            use_container_width=True,
                            hide_index=True,
                        )

                        # Drill-down: transactions on selected date
                        st.divider()
                        st.markdown("##### 🔍 Transações por data")
                        all_dates_pnl = sorted(set(str(s.date) for s in daily_snaps), reverse=True)
                        sel_date_str = st.selectbox(
                            "Selecione uma data para ver as transações:",
                            all_dates_pnl,
                            key=f"{key}_pnl_drill_date",
                        )
                        if sel_date_str:
                            sel_d = date.fromisoformat(sel_date_str)
                            txs = (
                                db_pnl.query(_TX)
                                  .filter_by(portfolio_id=port_v2.id,
                                             date=sel_d, is_void=False)
                                  .order_by(_TX.id)
                                  .all()
                            )
                            if not txs:
                                st.info(f"Nenhuma transação em {sel_date_str}.")
                            else:
                                tx_rows = []
                                for t in txs:
                                    tx_rows.append({
                                        "Tipo":         t.type,
                                        "Ativo":        t.asset.name if t.asset else "—",
                                        "Ticker":       t.asset.ticker if t.asset else "—",
                                        "Qtd":          f"{t.quantity:.4f}" if t.quantity else "—",
                                        "PU":           f"{sym} {t.price:.4f}" if t.price else "—",
                                        "Valor":        f"{sym} {t.value:,.2f}",
                                        "Cash Impact":  f"{sym} {t.cash_impact:+,.2f}",
                                        "Descrição":    t.description or "—",
                                    })
                                st.dataframe(
                                    pd.DataFrame(tx_rows),
                                    use_container_width=True,
                                    hide_index=True,
                                )
                            # Also show DailySnapshot for that date
                            day_snap = next(
                                (s for s in daily_snaps if s.date == sel_d), None
                            )
                            if day_snap:
                                prev_snap = next(
                                    (s for s in daily_snaps if s.date < sel_d), None
                                )
                                nav_open_d = prev_snap.nav if prev_snap else day_snap.nav
                                pnl_d = (day_snap.nav - (day_snap.net_flow or 0)) - nav_open_d
                                st.markdown(
                                    f"<div style='margin-top:10px;padding:12px 16px;"
                                    f"background:#F8F8F8;border-radius:8px;"
                                    f"font-size:0.85rem;color:#5E5E5E'>"
                                    f"<b>Resumo do dia {sel_date_str}</b><br>"
                                    f"NAV abertura: <b>{sym} {nav_open_d:,.2f}</b> &nbsp;·&nbsp; "
                                    f"Fluxo externo: <b>{sym} {(day_snap.net_flow or 0):+,.2f}</b> &nbsp;·&nbsp; "
                                    f"NAV fechamento: <b>{sym} {day_snap.nav:,.2f}</b><br>"
                                    f"PnL ex-fluxo: <b style='color:{'#10b981' if pnl_d >= 0 else '#ef4444'}'>"
                                    f"{sym} {pnl_d:+,.2f}</b> &nbsp;·&nbsp; "
                                    f"Retorno: <b style='color:{'#10b981' if (day_snap.daily_return or 0) >= 0 else '#ef4444'}'>"
                                    f"{(day_snap.daily_return or 0)*100:+.3f}%</b>"
                                    f"</div>",
                                    unsafe_allow_html=True,
                                )

                    # ── SUB-TAB: Ativos ───────────────────────────────────
                    with pnl_ativos:
                        if not asset_snaps:
                            st.info("Nenhum snapshot de ativo disponível.")
                        else:
                            # Collect unique assets
                            asset_ids_in_snaps = sorted(
                                set(s.asset_id for s in asset_snaps
                                    if s.asset and s.asset.data_source != "cash_account")
                            )
                            asset_map = {
                                s.asset_id: s.asset
                                for s in asset_snaps
                                if s.asset and s.asset.data_source != "cash_account"
                            }

                            # Filter controls
                            fc1, fc2, fc3 = st.columns([2, 2, 3])
                            with fc1:
                                asset_options = ["Todos os ativos"] + [
                                    f"{asset_map[aid].ticker} — {asset_map[aid].name}"
                                    for aid in asset_ids_in_snaps
                                    if aid in asset_map
                                ]
                                sel_asset_pnl = st.selectbox(
                                    "Ativo", asset_options,
                                    key=f"{key}_pnl_asset_sel",
                                )
                            with fc2:
                                snap_dates = sorted(
                                    set(s.date for s in asset_snaps), reverse=True
                                )
                                date_from_pnl = st.date_input(
                                    "De", value=snap_dates[-1] if snap_dates else date.today(),
                                    key=f"{key}_pnl_from",
                                )
                                date_to_pnl = st.date_input(
                                    "Até", value=snap_dates[0] if snap_dates else date.today(),
                                    key=f"{key}_pnl_to",
                                )

                            # Filter selected asset id
                            sel_aid_filter = None
                            if sel_asset_pnl != "Todos os ativos":
                                sel_ticker = sel_asset_pnl.split(" — ")[0]
                                sel_aid_filter = next(
                                    (aid for aid, a in asset_map.items()
                                     if a.ticker == sel_ticker),
                                    None,
                                )

                            # Get daily NAVs for weight calculation
                            nav_by_date = {s.date: s.nav for s in daily_snaps}

                            # Build asset PnL rows
                            asset_rows = []
                            prev_mv: dict[int, float] = {}
                            for s in sorted(asset_snaps, key=lambda x: (x.date, x.asset_id)):
                                if s.asset is None or s.asset.data_source == "cash_account":
                                    continue
                                if not (date_from_pnl <= s.date <= date_to_pnl):
                                    continue
                                if sel_aid_filter and s.asset_id != sel_aid_filter:
                                    continue

                                nav_d = nav_by_date.get(s.date, 1.0)
                                weight_d = (s.market_value / nav_d
                                            if s.market_value and nav_d > 0 else 0.0)

                                # Daily PnL per asset = change in market value
                                # (ignoring buys/sells on that day for simplicity)
                                prev = prev_mv.get(s.asset_id)
                                daily_asset_pnl = (
                                    (s.market_value or 0) - prev
                                    if prev is not None else None
                                )

                                asset_rows.append({
                                    "Data":           str(s.date),
                                    "Ativo":          s.asset.name,
                                    "Ticker":         s.asset.ticker,
                                    "Qtd":            f"{s.quantity:.4f}",
                                    "PU":             f"{sym} {s.price:.4f}" if s.price else "—",
                                    "Valor Mercado":  f"{sym} {(s.market_value or 0):,.2f}",
                                    "Custo Médio":    f"{sym} {s.avg_cost:.4f}" if s.avg_cost else "—",
                                    "PnL Não Real.":  f"{sym} {(s.unrealized_pnl or 0):+,.2f}",
                                    "PnL Realizado":  f"{sym} {(s.realized_pnl or 0):+,.2f}",
                                    "PnL Total":      f"{sym} {(s.total_pnl or 0):+,.2f}",
                                    "PnL Dia":        (
                                        f"{sym} {daily_asset_pnl:+,.2f}"
                                        if daily_asset_pnl is not None else "—"
                                    ),
                                    "Peso %":         f"{weight_d*100:.2f}%",
                                })
                                prev_mv[s.asset_id] = s.market_value or 0

                            if not asset_rows:
                                st.info("Nenhum dado no intervalo selecionado.")
                            else:
                                # Summary metrics for selected filter
                                if sel_aid_filter is None:
                                    # latest day totals
                                    latest_d = max(s.date for s in asset_snaps
                                                   if s.asset and s.asset.data_source != "cash_account")
                                    lat_snaps = [s for s in asset_snaps
                                                 if s.date == latest_d and s.asset
                                                 and s.asset.data_source != "cash_account"]
                                    total_mv  = sum(s.market_value or 0 for s in lat_snaps)
                                    total_unr = sum(s.unrealized_pnl or 0 for s in lat_snaps)
                                    total_rea = sum(s.realized_pnl or 0 for s in lat_snaps)
                                    total_tot = sum(s.total_pnl or 0 for s in lat_snaps)
                                    m1, m2, m3, m4 = st.columns(4)
                                    m1.metric("Valor Investido", f"{sym} {total_mv:,.2f}")
                                    m2.metric("PnL Não Realizado", f"{sym} {total_unr:+,.2f}")
                                    m3.metric("PnL Realizado", f"{sym} {total_rea:+,.2f}")
                                    m4.metric("PnL Total", f"{sym} {total_tot:+,.2f}")
                                    st.divider()

                                st.dataframe(
                                    pd.DataFrame(asset_rows).iloc[::-1].reset_index(drop=True),
                                    use_container_width=True,
                                    hide_index=True,
                                )

        finally:
            db_pnl.close()

    # ═══════════════════════════════════════════════════════════════════════
    # TAB 3 — COTAÇÃO MANUAL
    # ═══════════════════════════════════════════════════════════════════════
    with tab_manual:
        from app.models import AssetPrice, Portfolio as PortfolioModel
        from app.engine.reprocessor import recompute_from as _recompute_from
        from app.services.prices import upsert_manual_price as _upsert_v2

        st.markdown("#### Cotação Manual")

        # ── Selecionar ativo ──────────────────────────────────────────────
        db = get_db_for(key)
        try:
            _all_assets = (db.query(Asset)
                             .filter(Asset.is_active == True,
                                     Asset.data_source.notin_(["cash_account"]))
                             .all())
            all_asset_list = [
                {"id": a.id, "name": a.name, "ticker": a.ticker, "source": a.data_source}
                for a in _all_assets
            ]
        finally:
            db.close()

        if not all_asset_list:
            st.info("Nenhum ativo cadastrado neste portfólio.")
        else:
            asset_labels   = [f"{a['name']} ({a['ticker']})" for a in all_asset_list]
            asset_by_label = {f"{a['name']} ({a['ticker']})": a for a in all_asset_list}
            sel_label = st.selectbox("Ativo", asset_labels, key=f"{key}_manual_asset")
            sel_asset = asset_by_label[sel_label]

            # ── Sub-tabs ─────────────────────────────────────────────────
            sub_hist, sub_insert, sub_delete = st.tabs([
                "📋 Histórico de cotações",
                "➕ Inserir cotação",
                "🗑️ Deletar cotações",
            ])

            # ── HISTÓRICO DE COTAÇÕES ────────────────────────────────────
            with sub_hist:
                db = get_db_for(key)
                try:
                    port      = db.query(PortfolioModel).filter_by(key=key).first()
                    hist_rows = (
                        db.query(AssetPrice)
                          .filter_by(asset_id=sel_asset["id"])
                          .order_by(AssetPrice.date.desc())
                          .all()
                    )
                    hist_data = [
                        {
                            "Data":    str(r.date),
                            "Cotação": round(r.price, 4),
                            "Fonte":   r.source,
                        }
                        for r in hist_rows
                    ]
                finally:
                    db.close()

                if not hist_data:
                    st.info("Nenhuma cotação registrada para este ativo.")
                else:
                    st.markdown(
                        f"**{len(hist_data)} cotações registradas para "
                        f"{sel_asset['ticker']}**"
                    )
                    st.dataframe(
                        pd.DataFrame(hist_data),
                        use_container_width=True,
                        hide_index=True,
                    )

                st.divider()

            # ── INSERIR COTAÇÃO ──────────────────────────────────────────
            with sub_insert:
                # ── Lançar cotação avulsa ────────────────────────────────
                st.markdown("**Lançar cotação avulsa:**")
                with st.form(f"{key}_single_price_form"):
                    sc1, sc2 = st.columns(2)
                    with sc1:
                        s_date  = st.date_input("Data", value=date.today())
                    with sc2:
                        s_price = st.number_input(
                            "Cotação", min_value=0.0, step=0.01, format="%.4f"
                        )
                    submitted_single = st.form_submit_button(
                        "Lançar", use_container_width=True
                    )

                if submitted_single:
                    if s_price <= 0:
                        st.error("Cotação deve ser > 0.")
                    else:
                        db = get_db_for(key)
                        try:
                            port = db.query(PortfolioModel).filter_by(key=key).first()
                            _upsert_v2(db, sel_asset["id"], s_date, s_price)
                            upsert_manual_price(db, sel_asset["id"], s_date, s_price)
                            db.flush()
                            if port:
                                _recompute_from(db, port.id, s_date)
                            db.commit()
                            st.success(
                                f"✓ {sel_asset['ticker']} · {s_date} · {s_price:.4f}  "
                                "— snapshots recalculados."
                            )
                            st.cache_data.clear()
                        except Exception as exc:
                            db.rollback()
                            st.error(f"Erro: {exc}")
                        finally:
                            db.close()

                st.divider()

                # ── Importar via Excel ───────────────────────────────────
                st.markdown("**Importar via Excel:**")
                st.caption("Col A = data · Col B = cotação · sem cabeçalho")
                uploaded = st.file_uploader(
                    "Selecionar .xlsx",
                    type=["xlsx"], key=f"{key}_xlsx_upload",
                )
                if uploaded:
                    import openpyxl
                    from datetime import datetime as dt_
                    wb = openpyxl.load_workbook(io.BytesIO(uploaded.read()), data_only=True)
                    ws = wb.active
                    parsed, errors = [], []
                    for i, row in enumerate(ws.iter_rows(values_only=True), 1):
                        raw_d, raw_p = row[0], row[1]
                        if isinstance(raw_d, dt_):
                            d = raw_d.date()
                        elif isinstance(raw_d, date):
                            d = raw_d
                        elif isinstance(raw_d, str):
                            try:
                                d = date.fromisoformat(raw_d[:10])
                            except Exception:
                                errors.append(f"L{i}: data")
                                continue
                        else:
                            errors.append(f"L{i}: data inválida")
                            continue
                        try:
                            p = float(raw_p)
                            if p <= 0:
                                raise ValueError
                        except Exception:
                            errors.append(f"L{i}: preço '{raw_p}'")
                            continue
                        parsed.append((d, p))

                    if errors:
                        st.warning(
                            f"{len(errors)} linha(s) ignorada(s): "
                            + " · ".join(errors[:5])
                        )
                    if parsed:
                        all_d = sorted(r[0] for r in parsed)
                        biz   = pd.bdate_range(all_d[0], all_d[-1])
                        miss  = [x.date() for x in biz if x.date() not in set(all_d)]
                        st.success(f"✓ {len(parsed)} cotações lidas.")
                        if miss:
                            st.warning(f"⚠️ {len(miss)} dia(s) útil(s) sem cotação:")
                            st.dataframe(
                                pd.DataFrame({"Datas faltando": [str(d) for d in miss]}),
                                use_container_width=True, hide_index=True,
                            )
                        with st.expander(f"Preview ({len(parsed)} linhas)"):
                            st.dataframe(
                                pd.DataFrame([
                                    {"Data": str(d), "Cotação": f"{p:.4f}"}
                                    for d, p in sorted(parsed)
                                ]),
                                use_container_width=True, hide_index=True,
                            )
                        if st.button(
                            f"⬆ Importar {len(parsed)} cotações → {sel_asset['ticker']}",
                            key=f"{key}_xl_ok",
                        ):
                            db = get_db_for(key)
                            try:
                                port     = db.query(PortfolioModel).filter_by(key=key).first()
                                earliest = min(d for d, _ in parsed)
                                for d, p in parsed:
                                    # Grava na tabela nova (asset_prices)
                                    _upsert_v2(db, sel_asset["id"], d, p)
                                    # Mantém compatibilidade com tabela legada
                                    upsert_manual_price(db, sel_asset["id"], d, p)
                                db.flush()
                                if port:
                                    _recompute_from(db, port.id, earliest)
                                db.commit()
                                st.success(
                                    f"✓ {len(parsed)} cotações importadas "
                                    "e snapshots recalculados."
                                )
                                st.cache_data.clear()
                                st.rerun()
                            except Exception as exc:
                                db.rollback()
                                st.error(f"Erro: {exc}")
                            finally:
                                db.close()

            # ── DELETAR COTAÇÕES ─────────────────────────────────────────
            with sub_delete:
                st.caption(
                    "Selecione um intervalo de datas para remover as cotações. "
                    "Os snapshots serão recalculados automaticamente."
                )

                # Carregar cotações existentes do ativo
                db = get_db_for(key)
                try:
                    price_rows = (
                        db.query(AssetPrice)
                          .filter_by(asset_id=sel_asset["id"])
                          .order_by(AssetPrice.date.desc())
                          .all()
                    )
                    price_data = [
                        {"id": r.id, "date": r.date,
                         "price": r.price, "source": r.source}
                        for r in price_rows
                    ]
                finally:
                    db.close()

                if not price_data:
                    st.info("Nenhuma cotação registrada para este ativo.")
                else:
                    st.markdown(
                        f"**{len(price_data)} cotações registradas para "
                        f"{sel_asset['ticker']}**"
                    )

                    # ── Deletar por intervalo de datas ───────────────────
                    st.markdown("**Deletar por intervalo:**")
                    d_col1, d_col2 = st.columns(2)
                    min_date = min(r["date"] for r in price_data)
                    max_date = max(r["date"] for r in price_data)
                    with d_col1:
                        del_from = st.date_input(
                            "De", value=max_date,
                            min_value=min_date, max_value=max_date,
                            key=f"{key}_del_from",
                        )
                    with d_col2:
                        del_to = st.date_input(
                            "Até", value=max_date,
                            min_value=min_date, max_value=max_date,
                            key=f"{key}_del_to",
                        )

                    # Contar quantas serão deletadas
                    to_delete_count = sum(
                        1 for r in price_data if del_from <= r["date"] <= del_to
                    )

                    if to_delete_count > 0:
                        st.warning(
                            f"⚠️ {to_delete_count} cotação(ões) serão removidas "
                            f"({del_from} → {del_to})."
                        )
                    else:
                        st.info("Nenhuma cotação no intervalo selecionado.")

                    if st.button(
                        f"🗑️ Deletar {to_delete_count} cotação(ões)",
                        key=f"{key}_del_range",
                        disabled=to_delete_count == 0,
                        type="primary",
                    ):
                        db = get_db_for(key)
                        try:
                            port = db.query(PortfolioModel).filter_by(key=key).first()

                            # Deletar de asset_prices (nova)
                            deleted_new = (
                                db.query(AssetPrice)
                                  .filter(
                                      AssetPrice.asset_id == sel_asset["id"],
                                      AssetPrice.date >= del_from,
                                      AssetPrice.date <= del_to,
                                  )
                                  .delete(synchronize_session=False)
                            )

                            # Deletar de prices (legada)
                            deleted_leg = (
                                db.query(Price)
                                  .filter(
                                      Price.asset_id == sel_asset["id"],
                                      Price.date >= del_from,
                                      Price.date <= del_to,
                                  )
                                  .delete(synchronize_session=False)
                            )

                            db.flush()
                            if port:
                                _recompute_from(db, port.id, del_from)
                            db.commit()
                            st.success(
                                f"✓ {deleted_new} cotações removidas. "
                                "Snapshots recalculados."
                            )
                            st.cache_data.clear()
                            st.rerun()
                        except Exception as exc:
                            db.rollback()
                            st.error(f"Erro: {exc}")
                        finally:
                            db.close()

                    st.divider()

                    # ── Deletar linha por linha ──────────────────────────
                    st.markdown("**Ou selecione cotações individuais:**")

                    # Mostrar apenas as últimas 60 (evitar scroll infinito)
                    display_rows = price_data[:60]
                    for r in display_rows:
                        col_d, col_p, col_s, col_btn = st.columns([3, 2, 2, 1])
                        col_d.markdown(
                            f"<div style='padding-top:6px'>{r['date']}</div>",
                            unsafe_allow_html=True,
                        )
                        col_p.markdown(
                            f"<div style='padding-top:6px'>{r['price']:.4f}</div>",
                            unsafe_allow_html=True,
                        )
                        col_s.markdown(
                            f"<div style='padding-top:6px;color:#ABABAB;"
                            f"font-size:0.85em'>{r['source']}</div>",
                            unsafe_allow_html=True,
                        )
                        if col_btn.button(
                            "🗑", key=f"{key}_del_row_{r['id']}",
                            help=f"Deletar {r['date']}",
                        ):
                            db = get_db_for(key)
                            try:
                                port = db.query(PortfolioModel).filter_by(key=key).first()
                                db.query(AssetPrice).filter_by(id=r["id"]).delete()
                                db.query(Price).filter(
                                    Price.asset_id == sel_asset["id"],
                                    Price.date == r["date"],
                                ).delete(synchronize_session=False)
                                db.flush()
                                if port:
                                    _recompute_from(db, port.id, r["date"])
                                db.commit()
                                st.cache_data.clear()
                                st.rerun()
                            except Exception as exc:
                                db.rollback()
                                st.error(f"Erro: {exc}")
                            finally:
                                db.close()

                    if len(price_data) > 60:
                        st.caption(
                            f"Mostrando 60 de {len(price_data)} cotações. "
                            "Use deletar por intervalo para remover em massa."
                        )
