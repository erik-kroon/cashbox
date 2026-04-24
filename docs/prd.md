# Cashbox Production PRD v1.0

## 1. Product definition

**Cashbox** is a production-grade autonomous prediction-market research and execution platform. It lets an LLM agent continuously search Polymarket for exploitable inefficiencies, run controlled experiments, validate strategies with deterministic backtests and live paper trading, and submit only policy-compliant trade intents to a separately controlled execution system.

Cashbox is not a single trading bot. It is a governed quant-research and execution operating system.

Core principle:

```text
Agent proposes.
Data records.
Backtester verifies.
Evaluator promotes.
Paper trading confirms.
Risk gateway constrains.
Signer executes only approved orders.
Human governs capital.
```

## 2. Product goals

Cashbox must:

1. Continuously ingest and preserve Polymarket market data, order books, trades, wallet activity, metadata, and resolutions.
2. Let an LLM research agent inspect market structure, generate hypotheses, create strategy configs, run experiments, and write reports.
3. Enforce point-in-time historical simulation with realistic fees, latency, slippage, partial fills, precision constraints, and stale-book rejection.
4. Promote strategies through deterministic gates: research → backtest → walk-forward → paper → tiny live → scaled live.
5. Provide a live execution path where no LLM or research process can bypass risk limits.
6. Make every decision auditable: data input, strategy config, model version, backtest result, promotion decision, order intent, risk verdict, order submission, fill, cancellation, reconciliation, and PnL.
7. Survive process crashes, network failures, stale WebSockets, exchange API instability, bad model outputs, and malicious/compromised tools.

## 3. Non-goals

Cashbox must not:

1. Give Hermes, Clawdbot, or any LLM direct access to private keys, exchange credentials, wallet credentials, production shell, or raw live order APIs.
2. Allow the agent to change risk limits, policy files, signer configuration, deployment manifests, or production secrets.
3. Treat an LLM’s reasoning as proof of profitability.
4. Treat a backtest as sufficient for capital allocation without paper/live drift checks.
5. Depend on a single Polymarket strategy family.
6. Optimize for maximum trade count.
7. Attempt to bypass jurisdictional restrictions, geoblocking, or platform terms.
8. Use unreviewed third-party agent skills/plugins in the production trust zone.

## 4. Production-grade architectural decision summary

| Area               | Production decision                                                                                                         |
| ------------------ | --------------------------------------------------------------------------------------------------------------------------- |
| Agent runtime      | Hermes Agent as research/operator shell only                                                                                |
| Agent integration  | Cashbox MCP/HTTP Tool API with explicit capability tokens                                                                   |
| Workflow engine    | Temporal for durable scheduled and long-running workflows                                                                   |
| Trading engine     | NautilusTrader for backtesting, paper, and live execution                                                                   |
| Polymarket adapter | Prefer Nautilus Rust Polymarket path where feature-complete; maintain adapter conformance tests across Python/Rust surfaces |
| Event bus          | Redpanda or Kafka-compatible broker for market/trade/order/risk events                                                      |
| State DB           | Postgres for transactional state and audit metadata                                                                         |
| Analytics DB       | ClickHouse for order book, trade, feature, backtest, and telemetry analytics                                                |
| Data lake          | Object storage with immutable Parquet datasets and manifest/version metadata                                                |
| Cache              | Redis/Valkey for ephemeral hot state only; never source of truth                                                            |
| Policy engine      | OPA/Rego for risk and permission policy decisions, plus application-level invariant checks                                  |
| Secrets            | Vault plus isolated signer service; no secrets in agent runtime                                                             |
| Observability      | OpenTelemetry traces/metrics/logs; Prometheus/Grafana/Loki or equivalent backend                                            |
| Deployment         | Kubernetes for production; Docker Compose only for local development                                                        |
| Trust model        | Separate research, data, execution, signer, and observability trust zones                                                   |

## 5. Trust-zone architecture

Cashbox is divided into five trust zones.

### 5.1 Research zone

Contains:

- Hermes Agent
- agent sandbox
- experiment authoring
- research notebooks
- non-production code generation
- report generation

Allowed:

- Read sanitized market/history data through Cashbox Tool API.
- Create experiments.
- Run backtests through controlled jobs.
- Request paper strategy runs.
- Create live trade intents for approved strategies.
- Request halts.

Forbidden:

- Direct live order submission.
- Key access.
- Risk policy edits.
- Production deployment edits.
- Shell access to execution/signer hosts.

### 5.2 Data zone

Contains:

- market data ingestion
- raw event recording
- order book history
- trade history
- wallet activity
- resolution ingestion
- feature builder
- data quality monitor

The data zone is append-first. Raw payloads are preserved before parsing. Parsed and normalized data are derived products.

### 5.3 Research compute zone

Contains:

- backtest runner
- walk-forward runner
- sensitivity-analysis runner
- feature-replay jobs
- strategy simulation jobs

No production secrets. No live order permissions.

### 5.4 Execution zone

Contains:

- risk gateway
- paper executor
- live executor
- order state machine
- position reconciliation
- cancel-all service

The execution zone accepts only structured, validated trade intents and emits auditable risk decisions.

### 5.5 Signer zone

Contains:

- order signer
- wallet/API credential derivation
- signing key custody
- Vault integration
- optional HSM/hardware wallet integration

Only the risk gateway can request signing. The signer does not accept requests from the agent or research services.

## 6. High-level architecture

```text
                         ┌────────────────────────┐
                         │ Hermes Agent            │
                         │ Research operator       │
                         └───────────┬────────────┘
                                     │ MCP / HTTPS
                                     v
                         ┌────────────────────────┐
                         │ Cashbox Tool API        │
                         │ Capability-gated tools  │
                         └───────────┬────────────┘
                                     │
      ┌──────────────────────────────┼──────────────────────────────┐
      v                              v                              v
┌───────────────┐           ┌────────────────┐            ┌─────────────────┐
│ Data Platform  │           │ Research Engine │            │ Governance API   │
│ raw + features │           │ experiments     │            │ approvals/audit  │
└───────┬───────┘           └───────┬────────┘            └───────┬─────────┘
        │                           │                              │
        v                           v                              v
┌───────────────┐           ┌────────────────┐            ┌─────────────────┐
│ ClickHouse     │           │ Nautilus       │            │ Policy Engine    │
│ analytics      │           │ backtest/paper │            │ OPA + invariants │
└───────┬───────┘           └───────┬────────┘            └───────┬─────────┘
        │                           │                              │
        └───────────────────────────┼──────────────────────────────┘
                                    v
                          ┌───────────────────────┐
                          │ Risk Gateway           │
                          │ only live order path   │
                          └───────────┬───────────┘
                                      │
                                      v
                          ┌───────────────────────┐
                          │ Signer Service         │
                          │ isolated credentials   │
                          └───────────┬───────────┘
                                      │
                                      v
                          ┌───────────────────────┐
                          │ Nautilus Live Executor │
                          │ Polymarket CLOB        │
                          └───────────────────────┘
```

## 7. Core production services

### 7.1 `cashbox-agent-gateway`

Purpose: expose only approved Cashbox tools to Hermes.

Responsibilities:

- Implement MCP and/or HTTPS tool endpoints.
- Authenticate Hermes with scoped service credentials.
- Enforce tool-level authorization.
- Rate-limit tool calls.
- Sanitize tool inputs.
- Record every tool call, arguments hash, response hash, user/session identity, and timestamp.
- Block prompt/tool attempts to access secrets, shell, risk files, or production hosts.

Key design decision: Hermes never calls internal services directly. It calls the agent gateway.

### 7.2 `market-data-ingestor`

Purpose: ingest raw market data and preserve it immutably.

Inputs:

- Polymarket Gamma market metadata.
- CLOB order books.
- CLOB trades.
- WebSocket market channel.
- WebSocket user/channel events for controlled accounts.
- Wallet activity / leaderboard data where available.
- External reference feeds for strategy families that require them.

Responsibilities:

- Timestamp receive time with monotonic and wall-clock timestamps.
- Store raw payloads before parsing.
- Normalize events into canonical schemas.
- Publish events to Redpanda topics.
- Write immutable Parquet to object storage.
- Write queryable data into ClickHouse.
- Detect gaps, stale streams, hash changes, malformed payloads, and schema drift.

### 7.3 `market-catalog`

Purpose: maintain canonical market metadata.

Responsibilities:

- Token ID → market ID mapping.
- Market ID → event ID mapping.
- Outcome sets.
- Binary vs multi-outcome classification.
- Neg-risk flags.
- Tick size and precision metadata.
- Fee category.
- Resolution source.
- Start/end/resolution times.
- Active/closed/resolved status.
- Market relation graph.

### 7.4 `feature-builder`

Purpose: compute point-in-time features.

Feature families:

- top-of-book spread
- full-book depth
- imbalance
- order book slope
- volatility of market price
- time to resolution
- category-level liquidity
- fee-adjusted edge
- opportunity decay
- wallet-flow strength
- delayed-copy profitability
- related-market constraints
- neg-risk basket prices
- external fair value deltas

All features must include:

- source event timestamps
- computation timestamp
- feature version
- code version
- source dataset version

### 7.5 `experiment-service`

Purpose: manage strategy hypotheses and configs.

Responsibilities:

- Store hypotheses.
- Store strategy configs.
- Validate config schemas.
- Track code versions.
- Track dataset versions.
- Track generated-by metadata.
- Track experiment status lifecycle.
- Prevent mutable overwrites of historical experiment configs.

Experiment lifecycle:

```text
DRAFT
→ VALIDATED_CONFIG
→ BACKTEST_QUEUED
→ BACKTESTED
→ WALK_FORWARD_TESTED
→ PAPER_ELIGIBLE
→ PAPER_RUNNING
→ PAPER_PASSED
→ TINY_LIVE_ELIGIBLE
→ TINY_LIVE_RUNNING
→ SCALE_REVIEW
→ PRODUCTION_APPROVED
→ DISABLED / REJECTED / RETIRED
```

### 7.6 `backtest-runner`

Purpose: execute deterministic simulations.

Responsibilities:

- Run Nautilus backtests from immutable configs.
- Replay point-in-time market data.
- Model fees.
- Model order latency.
- Model stale-book rejection.
- Model partial fills.
- Model precision constraints.
- Model strategy-specific execution semantics.
- Emit reproducible result artifacts.

Hard requirement: a backtest is invalid if it uses post-resolution data, future market metadata, randomly shuffled train/test splits, missing fee model, or unversioned datasets.

### 7.7 `walk-forward-runner`

Purpose: test robustness across time and market regimes.

Responsibilities:

- Run rolling train/validate/test windows.
- Keep final holdout locked.
- Penalize parameter instability.
- Detect strategy decay.
- Compare with baseline strategies.

### 7.8 `evaluator`

Purpose: judge experiments deterministically.

Responsibilities:

- Compute standardized metrics.
- Apply promotion gates.
- Detect overfitting.
- Detect insufficient sample size.
- Detect unrealistic fill assumptions.
- Detect backtest/paper/live drift.
- Explain rejections.
- Generate promotion recommendations.

The evaluator, not Hermes, controls promotion status.

### 7.9 `paper-executor`

Purpose: run approved strategies against live market data without capital.

Responsibilities:

- Simulate live order submission.
- Simulate fills from current book.
- Track missed fills.
- Track price movement after simulated fill.
- Track adverse selection.
- Compare paper behavior to backtest assumptions.
- Emit paper PnL and fill-quality metrics.

### 7.10 `risk-gateway`

Purpose: enforce all live-trading policy.

Responsibilities:

- Accept trade intents only from authenticated services.
- Validate strategy approval status.
- Validate market eligibility.
- Validate order type.
- Validate time in force.
- Validate price and quantity precision.
- Validate book freshness.
- Validate account state.
- Validate exposure limits.
- Validate loss limits.
- Validate event/correlation limits.
- Validate signer health.
- Call OPA for policy decision.
- Apply application-level invariant checks after OPA.
- Emit allow/reject decision.
- Forward approved order to signer/executor.

Important: OPA can decide policy, but application code must also enforce domain invariants that are too latency-sensitive or stateful for pure policy files.

### 7.11 `signer-service`

Purpose: isolate private key and signing operations.

Responsibilities:

- Load private key from Vault or HSM-backed secret source.
- Derive or hold API credentials where required.
- Sign only approved order payloads from risk gateway.
- Refuse unsigned or unapproved payloads.
- Rate-limit signing.
- Maintain audit logs.
- Never expose key material.

### 7.12 `live-executor`

Purpose: interact with Nautilus and Polymarket CLOB.

Responsibilities:

- Submit approved orders.
- Cancel orders.
- Query open orders.
- Track fills.
- Reconcile order state.
- Reconcile positions.
- Emit execution events.
- Handle heartbeat, reconnects, retryable errors, and fail-safe shutdown.

### 7.13 `governance-service`

Purpose: manage approvals and production policy lifecycle.

Responsibilities:

- Human approvals for strategy promotion.
- Human approvals for capital limit changes.
- Policy versioning.
- Emergency halt management.
- Audit log review.
- Role-based access control.

### 7.14 `dashboard`

Purpose: read-only and controlled admin UI.

Views:

- market data health
- active strategies
- experiment leaderboard
- backtest/paper/live drift
- order intents
- risk rejections
- exposure
- PnL
- open orders
- system health
- audit trail
- agent reports

Dashboard write actions must be limited to human-approved governance actions, not raw database edits.

## 8. Cashbox Tool API for Hermes

Hermes gets a curated tool surface. Tools are grouped into permission classes.

### 8.1 Market discovery tools

```python
list_active_markets(filters)
get_market_metadata(market_id)
get_market_tokens(market_id)
find_related_markets(query, filters)
get_market_relation_graph(market_id)
classify_market(market_id)
get_resolution_status(market_id)
```

### 8.2 Market data tools

```python
get_order_book(token_id)
get_order_book_history(token_id, start, end, depth, granularity)
get_trade_history(market_id, start, end)
get_market_timeseries(market_id, start, end, fields)
get_opportunity_decay(opportunity_id, horizons_ms)
query_research_dataset(sql, dataset_id)
```

### 8.3 Wallet-flow tools

```python
rank_wallets(period, filters)
get_wallet_profile(wallet)
get_wallet_trades(wallet, start, end)
detect_wash_like_wallets(wallets)
backtest_delayed_wallet_copy(wallet, delays_ms, filters)
get_wallet_cluster(wallet)
get_wallet_signal_quality(wallet, filters)
```

### 8.4 Strategy/experiment tools

```python
list_strategy_families()
get_strategy_template(strategy_family)
create_experiment(hypothesis, strategy_family, config)
validate_strategy_config(config)
clone_experiment(experiment_id, modifications)
attach_research_note(experiment_id, markdown)
list_experiments(filters)
get_experiment(experiment_id)
```

### 8.5 Backtest tools

```python
run_backtest(experiment_id, dataset_id, assumptions)
run_walk_forward(experiment_id, schedule)
run_sensitivity_analysis(experiment_id, parameter_grid)
compare_experiments(filters, metrics)
get_backtest_artifacts(run_id)
explain_backtest_failure(run_id)
```

### 8.6 Evaluation tools

```python
score_experiment(experiment_id)
check_promotion_eligibility(experiment_id, target_stage)
get_experiment_leaderboard(filters)
analyze_skip_reasons(filters)
analyze_paper_vs_backtest_drift(strategy_id)
analyze_live_vs_paper_drift(strategy_id)
```

### 8.7 Paper tools

```python
start_paper_strategy(strategy_id)
stop_paper_strategy(strategy_id)
get_paper_state(strategy_id)
get_paper_results(strategy_id)
```

### 8.8 Live-adjacent tools

Hermes may request live actions, but cannot force them.

```python
create_trade_intent(strategy_id, order_request)
request_strategy_cancel_all(strategy_id, reason)
request_global_halt(reason)
request_live_promotion(strategy_id, target_limits, rationale)
```

The risk gateway and governance service decide.

### 8.9 Explicitly forbidden tools for Hermes

Hermes must never receive tools equivalent to:

```python
submit_order(...)
sign_order(...)
withdraw(...)
transfer(...)
bridge_funds(...)
edit_risk_policy(...)
edit_kubernetes_secret(...)
read_env(...)
read_private_key(...)
run_shell_on_execution_host(...)
install_plugin_on_execution_host(...)
```

## 9. Strategy families

Cashbox is strategy-agnostic but initially supports these production strategy families.

### 9.1 Binary pair constraint arb

Goal: detect and optionally exploit binary YES/NO pricing violations.

Canonical conditions:

```text
YES ask + NO ask + fees + slippage + buffer < 1.00
YES bid + NO bid - fees - slippage - buffer > 1.00
```

Production requirements:

- top-of-book and depth-aware simulation
- precision-aware execution
- stale-book rejection
- two-leg partial-fill handling
- no linked-order assumptions
- explicit unwind behavior

### 9.2 Multi-outcome / neg-risk basket arb

Goal: detect exhaustive basket mispricing.

Canonical condition:

```text
Σ outcome_asks + fees + slippage + buffer < 1.00
```

Production requirements:

- verified exhaustive outcome set
- neg-risk routing awareness
- multi-leg execution plan
- clear failure/unwind plan
- resolution ambiguity filter

### 9.3 Related-market constraint graph

Goal: exploit logical inconsistencies between related markets.

Examples:

- duplicate markets
- subset/superset markets
- mutually exclusive markets
- conditional markets
- event aliases

Production requirements:

- LLM may propose relationships
- deterministic relation verifier required
- high-confidence/manual approval required for live use
- relation versioning and expiry

### 9.4 Crypto fair-value maker

Goal: quote Polymarket crypto direction markets using external liquid market reference data.

Inputs:

- BTC/ETH spot/perp prices
- volatility
- time to resolution
- reference strike
- funding/market microstructure features
- Polymarket order book state

Production requirements:

- external data source redundancy
- fair-value model versioning
- post-only only by default
- aggressive stale-model cancellation
- adverse-selection tracking
- inventory skew control

### 9.5 Wallet-flow overlay

Goal: use profitable wallet activity as a feature, not a blind copy signal.

Requirements:

- wallet-quality model
- wash-like behavior filter
- delayed-copy backtests
- slippage-after-wallet-entry measurement
- cluster correlation detection
- multi-wallet confirmation option
- no strategy promotion based on leaderboard rank alone

### 9.6 News/event reaction research

Goal: evaluate whether external event feeds produce profitable signals.

Production restriction: research and paper only until data provenance, timestamp alignment, and latency are robust.

## 10. Data architecture

### 10.1 Data products

Cashbox maintains four classes of data.

1. **Raw events**
   - exact API/WebSocket payloads
   - receive timestamps
   - source endpoint/channel
   - schema hash

2. **Normalized events**
   - canonical order book snapshots
   - trades
   - market metadata updates
   - wallet activity
   - fills
   - cancellations
   - risk decisions

3. **Features**
   - versioned point-in-time derived data
   - strategy-specific feature sets
   - fair-value model outputs
   - wallet scores

4. **Research artifacts**
   - experiment configs
   - backtest runs
   - reports
   - charts
   - promotion decisions

### 10.2 Event topics

Recommended event stream topics:

```text
pm.raw.gamma.markets
pm.raw.clob.books
pm.raw.clob.trades
pm.raw.ws.market
pm.raw.ws.user
pm.normalized.markets
pm.normalized.books.l1
pm.normalized.books.l2
pm.normalized.trades
pm.normalized.wallet_activity
cashbox.features.market_microstructure
cashbox.features.wallet_quality
cashbox.features.related_markets
cashbox.experiments.created
cashbox.backtests.completed
cashbox.paper.orders
cashbox.paper.fills
cashbox.trade_intents.created
cashbox.risk.decisions
cashbox.live.orders
cashbox.live.fills
cashbox.reconciliation.snapshots
cashbox.alerts
```

### 10.3 Storage roles

| Store          | Role                                                  |
| -------------- | ----------------------------------------------------- |
| Redpanda/Kafka | real-time durable event stream                        |
| Object storage | immutable raw and Parquet history                     |
| ClickHouse     | time-series/order-book analytics                      |
| Postgres       | transactional metadata, experiments, approvals, audit |
| Redis/Valkey   | hot caches, locks, ephemeral state                    |
| Vault          | secrets and signing credentials                       |

### 10.4 Data quality invariants

A dataset is invalid for backtesting if:

- timestamps are missing
- event source is unknown
- market metadata is not versioned
- tick size history is missing
- fee model is missing
- resolution metadata leaks into pre-resolution features
- order book gaps exceed configured tolerances
- external data is not aligned point-in-time

## 11. Backtesting and validation requirements

### 11.1 Backtest levels

Cashbox supports three simulation levels.

**Level 1: trade/price replay**

Use only for rough directional research.

**Level 2: top-of-book replay**

Use for conservative taker strategies and opportunity detection.

**Level 3: full order book replay**

Required for production market making, partial-fill simulation, queue/adverse-selection analysis, and serious execution research.

### 11.2 Mandatory simulation assumptions

Every backtest must specify:

- dataset version
- strategy code version
- strategy config version
- fee model version
- tick/precision model
- latency model
- slippage model
- fill model
- stale-book threshold
- training/validation/test split
- comparison baseline

### 11.3 Prohibited backtest practices

- random train/test split for time-series trading data
- optimizing repeatedly against final holdout
- using latest market metadata across historical timestamps
- ignoring fees
- assuming instant two-leg execution
- ignoring partial fills
- ignoring order rejection/precision errors
- assuming paper fills equal live fills
- using LLM-generated metrics without deterministic recomputation

### 11.4 Promotion gates

#### Promote to paper

```yaml
required:
  config_schema_valid: true
  min_out_of_sample_trades: 250
  min_distinct_markets: 25
  positive_oos_ev: true
  conservative_fees_included: true
  conservative_slippage_included: true
  latency_model_included: true
  max_drawdown_within_policy: true
  beats_baseline: true
  no_lookahead_leakage: true
  evaluator_approved: true
```

#### Promote to tiny live

```yaml
required:
  min_paper_days: 14
  min_paper_trades: 100
  paper_pnl_positive_after_fees: true
  fill_model_error_below_threshold: true
  adverse_selection_acceptable: true
  unhandled_exceptions: 0
  reconciliation_errors: 0
  risk_gateway_tests_pass: true
  human_approval: true
```

#### Promote to scaled live

```yaml
required:
  min_tiny_live_days: 30
  min_live_trades: 100
  realized_pnl_positive_after_fees: true
  live_vs_paper_drift_acceptable: true
  daily_loss_breaches: 0
  drawdown_breaches: 0
  reconciliation_errors: 0
  security_review_passed: true
  human_approval: true
```

## 12. Risk gateway requirements

### 12.1 Universal risk checks

Every live trade intent must pass:

- strategy enabled
- strategy stage permits live trading
- strategy has approved capital limits
- market is allowed
- category is allowed
- event relation constraints are valid
- time to resolution is within policy
- order book is fresh
- external model is fresh if required
- price is tick-aligned
- quantity is precision-aligned
- order type is allowed
- time-in-force is allowed
- estimated fee is within assumptions
- estimated slippage is within assumptions
- market exposure cap not exceeded
- event exposure cap not exceeded
- portfolio exposure cap not exceeded
- daily loss cap not exceeded
- drawdown cap not exceeded
- open-order cap not exceeded
- signer service healthy
- exchange/API health acceptable
- no global halt
- no strategy halt

### 12.2 Order classes

Allowed order classes:

```text
TAKER_FOK
TAKER_IOC
POST_ONLY_LIMIT_GTC
POST_ONLY_LIMIT_GTD
CANCEL_ONLY
```

Forbidden by default:

```text
unbounded market buy
multi-leg orders without state machine
GTC directional copy orders without expiry logic
orders lacking strategy ID
orders lacking intent ID
orders that bypass risk gateway
```

### 12.3 Halt modes

**Soft halt**

- reject new trade intents
- keep current orders unless strategy policy says cancel

**Hard halt**

- reject new trade intents
- cancel all open orders
- snapshot positions
- alert operator

**Emergency halt**

- reject new trade intents
- cancel all reachable open orders
- disable all strategies
- freeze signer
- require manual reset

## 13. Security requirements

### 13.1 Agent isolation

Hermes must run in a sandboxed environment with:

- no production secrets
- no live executor shell
- no signer access
- no mounted production config directories
- no unrestricted network access
- no privilege escalation
- no third-party skill auto-installation
- read/write access only to research workspace

### 13.2 Secrets

- Secrets live in Vault.
- Agent has no Vault token capable of reading trading secrets.
- Signer receives only minimum required secret material.
- Secret access is audited.
- Key rotation is documented and tested.
- Hot wallet capital is limited.
- Cold funds are never accessible to Cashbox services.

### 13.3 Supply chain

- Lockfiles required.
- Signed container images preferred.
- CI dependency scanning required.
- SAST required for production code.
- Production deploys require reviewed PRs.
- Third-party agent skills/plugins banned from execution zone.
- Tool schemas versioned and reviewed.

### 13.4 Access control

Roles:

```text
viewer
researcher
operator
governor
security_admin
break_glass_admin
```

Only `governor` can approve strategy promotion or capital increases. Only `security_admin` can rotate signer credentials. Break-glass access requires separate audit and postmortem.

## 14. Observability requirements

Cashbox must emit:

- tool call traces
- workflow traces
- backtest traces
- market data ingestion metrics
- WebSocket freshness metrics
- API error rates
- order latency metrics
- risk rejection metrics
- strategy PnL metrics
- paper/live drift metrics
- signer request metrics
- reconciliation metrics
- exception logs
- audit logs

Critical alerts:

```text
stale_market_data
websocket_disconnected
risk_gateway_down
signer_down
unexpected_live_order
order_without_intent
position_reconciliation_mismatch
daily_loss_threshold_warning
daily_loss_breach
open_order_cancel_failed
vault_access_anomaly
agent_forbidden_tool_attempt
policy_version_changed
```

## 15. Governance requirements

### 15.1 Human approval required for

- strategy live promotion
- capital limit increases
- new live strategy family
- policy changes
- signer changes
- deployment to execution zone
- external feed changes for live models
- disabling critical risk checks

### 15.2 Audit log must include

- who/what initiated action
- exact input payload or payload hash
- policy version
- strategy version
- dataset version
- decision
- resulting action
- timestamp
- service identity

## 16. Production deployment architecture

### 16.1 Kubernetes namespaces

```text
cashbox-research
cashbox-data
cashbox-research-compute
cashbox-execution
cashbox-signer
cashbox-observability
cashbox-governance
```

### 16.2 Network policy

- Research can call Agent Gateway only.
- Agent Gateway can call approved Cashbox APIs.
- Research cannot call signer.
- Research cannot call live executor.
- Backtest runner cannot call live executor.
- Risk gateway can call signer and live executor.
- Signer accepts only risk gateway traffic.
- Live executor outbound is restricted to required Polymarket endpoints.
- Observability receives telemetry but cannot initiate trading actions.

### 16.3 Deployment environments

```text
dev
staging
shadow-production
production
```

`shadow-production` receives live data and runs paper/validation workloads with production-like latency and policy but no capital.

## 17. Acceptance criteria

Cashbox production v1 is acceptable only when:

1. Hermes can run research workflows but cannot access keys, raw shell on execution hosts, or live order APIs.
2. All market data used in backtests is versioned and point-in-time reproducible.
3. Every experiment is immutable after execution.
4. Backtests include fees, slippage, latency, precision, stale-book, and partial-fill assumptions.
5. The evaluator, not the agent, controls stage promotion.
6. Paper trading measures fill-quality drift against backtest assumptions.
7. Every live order originates from a recorded trade intent.
8. Every trade intent receives an auditable risk decision.
9. No order can be signed without a risk-gateway approval token.
10. Reconciliation can detect and alert on mismatched positions/orders.
11. Cancel-all works and is tested.
12. Global halt works and is tested.
13. Policy changes are versioned and audited.
14. Capital increases require human approval.
15. Observability can answer: “why did this trade happen?” and “why was this trade rejected?” within minutes.

## 18. Production roadmap by capability, not MVP

This is not an MVP sequence. These are capability tracks that must all exist for production readiness.

### Track A: Agent platform

- Hermes sandboxing
- Cashbox MCP/HTTP Tool API
- tool-level authorization
- tool-call audit
- agent memory/reporting integration

### Track B: Data platform

- raw event ingestion
- event stream
- immutable data lake
- ClickHouse analytics
- market catalog
- feature builder
- data quality monitoring

### Track C: Research platform

- experiment service
- strategy config schemas
- Nautilus backtest runner
- walk-forward runner
- sensitivity analysis
- evaluator

### Track D: Execution platform

- paper executor
- trade intent service
- risk gateway
- OPA policies
- signer service
- live executor
- reconciliation
- cancel-all

### Track E: Security/governance

- Vault
- RBAC
- network policies
- CI/CD protections
- supply-chain scanning
- approval workflows
- audit trail

### Track F: Observability

- OpenTelemetry instrumentation
- metrics dashboards
- alerting
- strategy health
- PnL/risk dashboards
- incident runbooks

## 19. Final product statement

Cashbox is a production-grade autonomous research system with a controlled execution boundary. Its primary innovation is not that an LLM can trade; it is that an LLM can generate and manage a high-throughput stream of trading experiments while deterministic infrastructure protects capital, preserves data integrity, validates performance, and enforces risk policy.

The system should be optimized for one question:

```text
Can we repeatedly discover and validate edge without ever allowing the research agent to become the risk manager, signer, or final judge?
```

The answer must be yes before Cashbox touches meaningful capital.
