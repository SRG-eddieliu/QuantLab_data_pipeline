# QuantLab
Quant research and portfolio construction framework for multi-factor equity strategies. Includes full pipeline from data ingestion, signal/factor research, alpha modeling, risk modeling, portfolio optimization, and backtesting. Designed to scale across alphas and strategies.

## Project Roadmap

This repo is organized to mimic an institutional quant hedge fund stack, end-to-end:

> **Raw Data → Signals & Factors → Alpha Models → Risk → Portfolio / Strategies → Backtesting & Attribution**

---

### Phase 0 – Repo & Infra Setup (Foundation)

**Goal:** Clean, extensible skeleton for the whole platform.

- Set up single monorepo structure (`src/`, `config/`, `data_raw/`, `data_processed/`, `data_meta/`, `outputs/`, `notebooks/`, `tests/`).
- Add shared infra:
  - Config loader (YAML/JSON)
  - Logging utilities
  - Basic project settings (universe = S&P 500, date range = 2000-01-01 to 2025-01-01).
- Define core abstract interfaces:
  - `DataHandler`
  - (later) `RiskModel`, `AlphaModel`, `PortfolioOptimizer`, `Strategy`


## 📂 Repository Structure

```
quant-platform/
  config/
  data_raw/
  data_processed/
  data_meta/
  src/
    infra/
    data_pipeline/
    research/
      signals/
      factors/
      alpha_models/
      diagnostics/
    risk/
    portfolio/
    strategies/
    backtest/
    utils/
  notebooks/
  outputs/
  tests/
```


---

### **Phase 1 — Data Platform (Data Collection + Unified API)**

**Goal:** Build reproducible unified data layer used by QR / PM / Backtest modules.

#### **Mandatory Datasets (stored as Parquet in `data_processed/`)**

- **Data model & storage:**
  - `data_raw/`: vendor / WRDS / API dumps.
  - `data_processed/`: normalized Parquet tables:
 
| Dataset | Description |
|--------|------------|
| `prices_daily` | OHLCV, adj close, volume |
| `returns_daily` | Daily returns incl. delisting returns |
| `sp500_membership` | Daily membership flag for S&P500 universe |
| `assets_meta` | asset_id, ticker, sector, industry, currency |
| `fundamentals_quarterly` | Compustat-style fundamentals (revenue, earnings, book, cashflows, debt, etc.) |
| `macro_timeseries` | CPI, unemployment, industrial production, credit spreads, etc. |
| `risk_free` | Short rate (3M T-Bill / OIS) |
| `style_factor_returns` | Market, SMB, HML, MOM, Quality, LowVol, etc. |
| `benchmarks` | S&P500 index price & returns |

#### **Planned / backlog**
- Analyst estimates & revisions (IBES)
- Analyst ratings
- Alt/sentiment datasets (later)
- Intraday / microstructure (DataBento)

#### **DataHandler API**
_All research must access data only through this interface_

```python
get_prices(...)
get_returns(...)
get_fundamentals(...)
get_universe(...)
get_macro(...)
get_style_factor_returns(...)
get_benchmark_returns(...)
```

- **Update logic:**
  - Initial full history build (2000-01-01 to 2025-01-01).
  - Ability to extend history on an ad-hoc basis (append new data, keep delists, expand data window).

---

### Phase 2 – Signals & Factors (QR Layer I)

**Goal:** Turn raw data into standardized, cross-sectional **factors**.

- Define **Signal** objects (raw informational features, e.g. returns, earnings surprise).
- Define **Factor** objects:
  - Built from signals using consistent cleaning:
    - Winsorization, z-scoring, optional sector/style neutralization.
  - Output as `(date, asset_id) → factor_value` panels.
- Build initial factor library (e.g.):
  - Momentum (12-1 month)
  - Value (book-to-price, earnings yield)
  - Quality (ROE, margins, leverage)
  - Volatility / liquidity measures
- All factor code must use `DataHandler` (no direct data access elsewhere).

#### Example Factor Library entries

| Factor Name | Description |
|--------------|-------------|
| `MOM_12_1` | 12-month return excluding last month (momentum) |
| `VAL_B2P` | Book-to-price (value) |
| `VAL_EY` | Earnings yield |
| `QUAL_ROE` | Return on equity (quality) |
| `VOL_20D` | Realized 20-day volatility |
| `LIQ_ADV` | Average dollar volume (liquidity) |

Factor output format:
```
(date, asset_id) → factor_value
```

Diagnostics tools:
- Factor IC (Information Coefficient)
- Quantile spreads / long-short portfolios
- Rolling IC / subperiod robustness
- Turnover & capacity metrics

---

### Phase 3 – Alpha Models & Diagnostics (QR Layer II)

**Goal:** Combine factors into **alpha forecasts** and rigorously evaluate them.

- Define **AlphaModel** interface:
  - Single-factor and multi-factor alphas (expected residual return per stock).
  - Store alpha outputs to `outputs/alphas/`.
- Build **Alpha Diagnostics** module:
  - IC/IR (Information Coefficient / Information Ratio).
  - Long-short factor portfolios (top vs bottom quantiles).
  - Rolling and subperiod performance.
  - Turnover & rough cost estimates.
  - Basic robustness tests (universe, parameter sensitivity).
- Maintain an **Alpha Library**:
  - Metadata per alpha: family (value/momentum/quality/…),
    IC stats, correlations, turnover, capacity hints.

| Alpha Name | Definition |
|------------|------------|
| `ALPHA_MOM_12_1` | $begin:math:text$\\alpha_{i,t} = MOM\\_12\\_1\\_{i,t}$end:math:text$ |
| `ALPHA_VAL_B2P` | Basic value alpha |
| `ALPHA_VAL_MOM_QUAL_V1` | $begin:math:text$\\alpha_{i,t} = 0.4 VAL + 0.3 MOM + 0.3 QUAL$end:math:text$ |
| `ALPHA_ML_COMBO` | ML regression using factor features |

#### Alpha Diagnostics
- IC / IR statistics
- Long-short top vs bottom testing
- Rolling robustness curves
- Alpha correlation & redundancy
- Turnover / cost / capacity impacts


---

### Phase 4 – Risk Model (Risk Layer)

**Goal:** Provide risk estimates for portfolio construction and attribution.

- Implement initial **RiskModel**:
  - Historical covariance matrix from daily returns (e.g. EWMA / rolling window).
  - Market beta per stock.
  - Style factor loadings derived from factors (e.g. value, size, momentum, quality).
- Expose methods:
  - `get_covariance(universe, date or window)`
  - `get_betas(universe, date)`
  - `get_style_exposures(universe, date)`
 
#### **Historical covariance**
$begin:math:display$
\\Sigma = \\frac{1}{T-1}\\sum_{t=1}^T (r_t - \\bar{r})(r_t - \\bar{r})^\\top
$end:math:display$

EWMA:
$begin:math:display$
\\Sigma_t = \\lambda\\Sigma_{t-1} + (1-\\lambda)(r_t - \\bar{r})(r_t - \\bar{r})^\\top
$end:math:display$

#### **Factor risk model**
$begin:math:display$
r_t = B f_t + \\varepsilon_t
$end:math:display$

$begin:math:display$
\\text{Cov}(r) = B \\Sigma_f B^\\top + D
$end:math:display$

- $begin:math:text$B$end:math:text$ = factor loadings
- $begin:math:text$f_t$end:math:text$ = style factor returns
- $begin:math:text$D$end:math:text$ = idiosyncratic variances

APIs:
```python
get_covariance(...)
get_betas(...)
get_style_exposures(...)
get_factor_covariance(...)
get_idiosyncratic_risk(...)
```

---

### Phase 5 – Portfolio Optimizer & Strategies (PM Layer)

**Goal:** Turn combined alphas + risk model into **strategy-specific target weights**.

- **Alpha combination:**
  - `AlphaCombiner` to blend multiple alpha streams into a combined expected return vector μ (with PM-defined weights, possibly regime-dependent).
- **Portfolio optimizer:**
  - Implement mean-variance style optimizer:
    - Objective: maximize `μᵀw − λ wᵀΣw`.
    - Plug in constraints from strategies.
- **Strategy layer:**
  - Encapsulate PM-style constraints as `Strategy` objects, e.g.:
    - **Market-neutral L/S:** net = 0, beta ≈ 0, optional sector/style neutrality, gross leverage targets.
    - **Smart beta (long-only):** sum(w)=1, w≥0, tracking error & sector/style exposure limits vs benchmark.
    - **130/30:** long=130%, short=30%, net=100%, plus risk constraints.
    - **Factor timing:** regime-dependent alpha weights, same optimizer underneath.
- Output: target stock-level weights `w*` for each strategy and date.

---

### Phase 6 – Backtesting, Attribution & Reporting

**Goal:** Simulate strategies end-to-end and understand performance drivers.

- **Backtest engine:**
  - Simulate portfolio over time:
    - On each rebalance: combine alphas → μ, optimize → w*, apply transaction costs and slippage, update NAV.
  - Track: positions, PnL, turnover, exposures.
- **Metrics & analytics:**
  - Cumulative and annualized returns, volatility, Sharpe/Sortino.
  - Max drawdown, hit rate, turnover, capacity proxies.
  - Exposure tracking (sector, country, style, beta).
- **Performance attribution:**
  - Decompose portfolio returns into:
    - Market beta contribution.
    - Style factor contributions (value, momentum, etc.).
    - Residual alpha.
- **Reporting:**
  - Save backtest summaries, alpha reports, plots to `outputs/`.
  - Optional: notebooks / dashboards for QR and PM views.

---

### Phase 7 – Backlog / Future Enhancements

(Not in scope immediately, but designed for.)

- Microstructure & intraday data (DataBento) for cost and execution modelling.
- Short interest, borrow fees, and more realistic L/S constraints.
- Alternative & sentiment data (news, social, ESG, etc.).
- Cloud storage backend (e.g. S3) for `data_raw/` and `data_processed/`.
- Distributed compute / faster backtesting for large universes or intraday data.
