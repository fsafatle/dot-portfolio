# DOT Portfolio Management

Sistema de gerenciamento de portfólio multiativos com histórico completo de alocações, atualização automática de preços e dashboard de performance.

---

## Arquitetura

```
dot-portfolio/
├── app/
│   ├── config.py               # Configurações e portfólio inicial
│   ├── database.py             # SQLAlchemy / SQLite
│   ├── models.py               # Modelos de dados (5 tabelas)
│   ├── market_data/
│   │   ├── base.py             # Interface abstrata de market data
│   │   └── yahoo_provider.py   # Provider Yahoo Finance (yfinance)
│   └── portfolio/
│       ├── engine.py           # Lógica core: preços, alocações, snapshots
│       └── performance.py      # Cálculos: retorno, MTD, YTD, por ativo
├── pages/
│   ├── 1_Allocations.py        # Gerenciar alocações
│   └── 2_History.py            # Histórico e séries temporais
├── scripts/
│   ├── init_db.py              # Cria tabelas e seed inicial
│   └── fetch_prices.py         # Atualiza preços e snapshots
├── data/
│   └── portfolio.db            # SQLite (gerado automaticamente)
└── Home.py                     # Dashboard principal (Streamlit)
```

## Modelo de dados

| Tabela              | Descrição                                               |
|---------------------|---------------------------------------------------------|
| `assets`            | Cadastro de ativos (ticker, classe, fonte de dados)     |
| `allocations`       | Alocações versionadas (start/end date por registro)     |
| `prices`            | Cache de preços diários por ativo                       |
| `portfolio_snapshots` | Índice diário do portfólio (base = 1000)             |
| `allocation_log`    | Log auditável de toda mudança de alocação               |

## Portfólio inicial (01/01/2026)

| Ativo                          | Ticker   | Peso |
|--------------------------------|----------|------|
| Bloomberg Global Aggregate ETF | AGGU.L   | 32%  |
| Global Equities ACWI ETF       | ACWI     | 48%  |
| SPDR Gold Shares               | GLD      | 5%   |
| Bitcoin                        | BTC-USD  | 5%   |
| T-Bill 3 Month                 | ^IRX     | 10%  |

> `AGGU.L` = iShares Core Global Aggregate Bond UCITS ETF (USD Hedged) — proxy para Bloomberg Global Agg.
> `^IRX` = taxa anualizada do T-Bill 13 semanas, convertida para NAV simulado via capitalização diária (252 dias úteis/ano).

---

## Rodar localmente

### 1. Instalar dependências
```bash
pip3 install -r requirements.txt
```

### 2. Inicializar o banco e seed do portfólio
```bash
python3 -m scripts.init_db
```

### 3. Buscar preços de mercado e construir snapshots
```bash
python3 -m scripts.fetch_prices
```

### 4. Rodar o dashboard
```bash
# Adicionar Streamlit ao PATH (Mac):
export PATH="$HOME/Library/Python/3.9/bin:$PATH"

streamlit run Home.py
```

O app abre em `http://localhost:8501`.

---

## Uso diário

Para atualizar os preços e recalcular o índice:
```bash
python3 -m scripts.fetch_prices
```

Ou clique em **Refresh data** no dashboard.

---

## Páginas

| Página        | URL                    | Função                                             |
|---------------|------------------------|----------------------------------------------------|
| Dashboard     | `/` (Home)             | KPIs, gráfico de índice, tabela de ativos          |
| Allocations   | `/Allocations`         | Adicionar ativos, registrar mudanças de alocação   |
| History       | `/History`             | Composição em data específica, série de preços     |

---

## Lógica de rentabilidade

**Retorno diário do portfólio:**
```
R_portfolio(t) = Σ  weight_i × (price_i(t) / price_i(t-1) - 1)
```

**Índice cumulativo (base 1000):**
```
Index(t) = Index(t-1) × (1 + R_portfolio(t))
```

**T-Bill:** taxa anualizada convertida para retorno diário:
```
daily_return = (1 + rate_annual / 100)^(1/252) - 1
```

---

## Evoluir o sistema

O código foi desenhado para expansão. Próximos passos sugeridos:

- **Aportes e resgates**: adicionar tabela `cashflows` e ajustar cálculo de retorno (TWRR ou MWRR)
- **Benchmarks**: adicionar tickers de benchmark (ex: SPY, ^GSPC) e comparar no dashboard
- **Múltiplos portfólios**: adicionar campo `portfolio_id` nas tabelas
- **Rebalanceamento automático**: trigger quando drift > threshold
- **Outro provider de market data**: implementar `MarketDataProvider` (interface em `app/market_data/base.py`) e trocar na engine
