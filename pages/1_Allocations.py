"""
Allocations — Ferramenta de rebalanceamento e cadastro de ativos.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import date

import pandas as pd
import streamlit as st

from app.database import get_db
from app.models import Asset, Allocation, AllocationLog
from app.portfolio.engine import update_allocations, get_active_allocations, get_actual_weights
from app.config import BUCKETS, ROLES

st.set_page_config(page_title="Allocations · DOT", layout="wide")
st.title("Gestão de Alocações")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_active():
    db = get_db()
    try:
        today = date.today()
        active = get_active_allocations(db, today)
        actual = get_actual_weights(db, today)
        rows = []
        for a in active:
            rows.append({
                "ticker":     a.asset.ticker,
                "name":       a.asset.name,
                "bucket":     a.asset.bucket or "—",
                "asset_class":a.asset.asset_class,
                "target_pct": round(a.weight * 100, 4),
                "actual_pct": round(actual.get(a.asset.ticker, a.weight) * 100, 2),
                "asset_id":   a.asset_id,
            })
        return rows
    finally:
        db.close()


def _load_all_assets():
    db = get_db()
    try:
        return db.query(Asset).filter_by(is_active=True).order_by(Asset.name).all()
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Alocação atual
# ---------------------------------------------------------------------------
st.subheader("Alocação Atual")

active_rows = _load_active()
if active_rows:
    df_current = pd.DataFrame(active_rows)
    disp = df_current[["bucket", "name", "ticker", "target_pct", "actual_pct"]].copy()
    disp.columns = ["Bucket", "Ativo", "Ticker", "Peso-Alvo (%)", "Peso Atual (%)"]
    st.dataframe(disp, use_container_width=True, hide_index=True)
    total_target = sum(r["target_pct"] for r in active_rows)
    color = "green" if abs(total_target - 100.0) < 0.05 else "red"
    st.markdown(
        f"<span style='color:{color}; font-weight:700'>Total peso-alvo: {total_target:.1f}%</span>",
        unsafe_allow_html=True,
    )
else:
    st.info("Nenhuma alocação ativa.")

st.divider()

# ---------------------------------------------------------------------------
# Rebalancear — edição direta de todos os pesos
# ---------------------------------------------------------------------------
st.subheader("Rebalancear Portfólio")
st.caption(
    "Edite diretamente o peso de cada ativo. O total deve somar 100% para salvar. "
    "Para remover um ativo, coloque 0%. Para adicionar um novo, use a seção abaixo."
)

all_assets = _load_all_assets()
ticker_to_name = {a.ticker: a.name for a in all_assets}

# Agrupa ativos ativos por bucket para exibição ordenada
bucket_order = {b: i for i, b in enumerate(BUCKETS)}
sorted_active = sorted(
    active_rows,
    key=lambda r: (bucket_order.get(r["bucket"], 99), r["name"])
)

with st.form("rebalance_form"):
    eff_date = st.date_input("Data efetiva do rebalanceamento", value=date.today())
    notes_input = st.text_input(
        "Observação (opcional)",
        placeholder="ex: Redução em equities após reunião de comitê"
    )

    st.markdown("---")
    st.markdown("#### Pesos por ativo (%)")

    new_weights_input: dict[str, float] = {}

    # Renderiza um número editável por ativo, agrupado por bucket
    current_bucket = None
    for row in sorted_active:
        if row["bucket"] != current_bucket:
            current_bucket = row["bucket"]
            st.markdown(f"**{current_bucket}**")

        col_name, col_input = st.columns([4, 1])
        with col_name:
            st.markdown(
                f"<div style='padding-top:8px'>{row['name']} "
                f"<span style='color:gray;font-size:0.85em'>({row['ticker']})</span></div>",
                unsafe_allow_html=True,
            )
        with col_input:
            val = st.number_input(
                label=row["ticker"],
                min_value=0.0,
                max_value=100.0,
                value=float(row["target_pct"]),
                step=0.5,
                format="%.1f",
                key=f"w_{row['ticker']}",
                label_visibility="collapsed",
            )
        new_weights_input[row["ticker"]] = val

    # Opção de adicionar novo ativo ao rebalanceamento
    st.markdown("---")
    st.markdown("**Incluir novo ativo neste rebalanceamento** *(opcional)*")
    inactive_assets = [a for a in all_assets if a.ticker not in {r["ticker"] for r in active_rows}]
    if inactive_assets:
        add_new = st.checkbox("Adicionar ativo ao portfólio")
        if add_new:
            asset_map_new = {f"{a.name} ({a.ticker})": a.ticker for a in inactive_assets}
            c1, c2 = st.columns([3, 1])
            with c1:
                new_asset_choice = st.selectbox("Ativo a incluir", options=list(asset_map_new.keys()))
            with c2:
                new_asset_pct = st.number_input(
                    "Peso (%)", min_value=0.0, max_value=100.0,
                    value=0.0, step=0.5, format="%.1f", key="new_asset_pct"
                )
            new_ticker = asset_map_new[new_asset_choice]
            if new_asset_pct > 0:
                new_weights_input[new_ticker] = new_asset_pct

    st.markdown("---")

    # Preview da nova alocação — só ativos com peso > 0
    new_alloc_pct = {t: p for t, p in new_weights_input.items() if p > 0}
    total_new = sum(new_alloc_pct.values())

    st.markdown("#### Preview da nova alocação")
    if new_alloc_pct:
        preview_rows = [
            {
                "Ativo":    ticker_to_name.get(t, t),
                "Ticker":   t,
                "Peso (%)": f"{p:.1f}%",
            }
            for t, p in sorted(new_alloc_pct.items(), key=lambda x: -x[1])
        ]
        st.dataframe(pd.DataFrame(preview_rows), use_container_width=True, hide_index=True)

    color = "green" if abs(total_new - 100.0) < 0.01 else "red"
    st.markdown(
        f"<span style='color:{color}; font-weight:700; font-size:1.1em'>Total: {total_new:.1f}%</span>",
        unsafe_allow_html=True,
    )

    save = st.form_submit_button("Salvar rebalanceamento", use_container_width=True)
    if save:
        if abs(total_new - 100.0) > 0.01:
            st.error(f"Os pesos precisam somar 100% (atual: {total_new:.1f}%). Ajuste antes de salvar.")
        else:
            db = get_db()
            try:
                weights_decimal = {t: round(p / 100, 6) for t, p in new_alloc_pct.items()}
                update_allocations(db, weights_decimal, eff_date, notes=notes_input)
                st.success(f"Rebalanceamento salvo com data efetiva {eff_date}.")
                st.cache_data.clear()
                st.rerun()
            except Exception as exc:
                st.error(f"Erro: {exc}")
            finally:
                db.close()

st.divider()

# ---------------------------------------------------------------------------
# Cadastrar novo ativo no sistema
# ---------------------------------------------------------------------------
with st.expander("Cadastrar novo ativo no sistema"):
    with st.form("add_asset_form"):
        c1, c2 = st.columns(2)
        with c1:
            new_name   = st.text_input("Nome do ativo", placeholder="ex: Emerging Markets ETF")
            new_ticker = st.text_input("Ticker (Yahoo Finance)", placeholder="ex: EEM")
        with c2:
            new_class  = st.selectbox("Classe", ["equity", "fixed_income", "commodity", "crypto", "cash_equivalent", "other"])
            new_source = st.selectbox("Fonte de dados", ["yahoo", "yahoo_rate", "manual"])
        c3, c4 = st.columns(2)
        with c3:
            new_bucket = st.selectbox("Bucket", BUCKETS)
        with c4:
            new_role   = st.selectbox("Role", ROLES)
        new_notes = st.text_input("Notas (opcional)")
        if st.form_submit_button("Cadastrar ativo"):
            if not new_name or not new_ticker:
                st.error("Nome e ticker são obrigatórios.")
            else:
                db = get_db()
                try:
                    if db.query(Asset).filter_by(ticker=new_ticker.strip().upper()).first():
                        st.warning(f"Ticker {new_ticker} já existe.")
                    else:
                        db.add(Asset(
                            name=new_name.strip(),
                            ticker=new_ticker.strip().upper(),
                            asset_class=new_class,
                            data_source=new_source,
                            bucket=new_bucket,
                            role=new_role,
                            notes=new_notes.strip() or None,
                        ))
                        db.commit()
                        st.success(f"Ativo '{new_name}' cadastrado no bucket {new_bucket} como {new_role}.")
                finally:
                    db.close()

st.divider()

# ---------------------------------------------------------------------------
# Log de movimentações
# ---------------------------------------------------------------------------
st.subheader("Log de Movimentações")
db = get_db()
try:
    log_rows = (
        db.query(AllocationLog)
        .join(Asset)
        .order_by(AllocationLog.effective_date.desc(), AllocationLog.created_at.desc())
        .limit(100)
        .all()
    )
    if log_rows:
        log_data = [
            {
                "Data":        str(r.effective_date),
                "Ativo":       r.asset.name,
                "Ticker":      r.asset.ticker,
                "Ação":        r.action,
                "Peso Antigo": f"{r.old_weight*100:.1f}%" if r.old_weight is not None else "—",
                "Peso Novo":   f"{r.new_weight*100:.1f}%" if r.new_weight is not None else "—",
                "Obs":         r.notes or "",
            }
            for r in log_rows
        ]
        st.dataframe(pd.DataFrame(log_data), use_container_width=True, hide_index=True)
    else:
        st.info("Nenhum registro ainda.")
finally:
    db.close()
