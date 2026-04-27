# Cashbox Roadmap

This roadmap translates the production PRD into near-term implementation gates for the current repository. The repo is still a local, filesystem-backed vertical slice; production infrastructure such as Temporal, Redpanda, ClickHouse, Postgres, Vault, Kubernetes, NautilusTrader, and OPA remains future work.

## Current Baseline

Cashbox now has a working local control plane:

- Polymarket Gamma market ingest with raw payload preservation, normalized snapshots, manifests, and append-only per-market history.
- Research read APIs and a scoped agent gateway with tool authorization, rate limits, input sanitization, and audit logs.
- Experiment registry with immutable definitions, lifecycle history, cloning, notes, and strategy templates.
- Deterministic backtests over point-in-time history with explicit fees, latency, slippage, staleness, precision, partial-fill assumptions, artifacts, and failure explanations.
- Evaluator gates for paper, tiny-live, and scaled-live readiness.
- Paper trading runs with persisted state and backtest-vs-paper drift reporting.
- Trade intents, risk decisions, human review, approval tokens, live submission stubs, cancel-all, global halt, fills, and reconciliation.
- Governance RBAC, approval requests, policy versioning, emergency halt, shared audit timelines, and an operator evidence reader for health and incident views.

The main gap is no longer the shape of the trust boundary. The gap is production evidence: richer data, health visibility, repeatable workflows, and adapters that can graduate from local files to durable services without widening the agent's authority.

## MVP Gate

The next MVP gate is a capital-safe local operator demo:

1. A researcher can ingest a reproducible dataset, create an experiment, run a backtest, pass or fail deterministic gates, and start paper trading.
2. An operator can explain why a paper or live-adjacent action was allowed, rejected, halted, or reconciled as mismatched.
3. A governor can approve strategy promotion or policy changes through an audited path.
4. No agent-facing command can submit an order, sign a payload, change policy, or bypass risk and governance.
5. Health and audit commands can answer "what is stale, degraded, blocked, or risky?" without reading raw files by hand.

## Phase 1: Observability And Operator Evidence

Purpose: make the existing local control plane inspectable enough to operate.

Deliverables:

- Add a system health service that aggregates ingest freshness, gateway audit health, experiment/backtest/paper status, risk policy health, execution policy health, halt state, open orders, reconciliation mismatches, and governance pending requests.
- Add CLI commands for health summaries and degraded-component drill-down.
- Add durable event identifiers that connect experiment, backtest, paper, trade intent, risk decision, execution record, reconciliation snapshot, and governance request timelines.
- Extend audit filtering around actor, experiment, market, intent, decision, execution, and request identifiers.
- Add docs for the local operator demo path, including expected command outputs and failure examples.

Acceptance:

- One command answers whether Cashbox is safe to continue paper/live-adjacent operation.
- One command explains a rejected trade intent from experiment through risk checks.
- One command lists pending human decisions and emergency controls.
- Tests cover degraded signer/executor policy, stale ingest, pending governance, and reconciliation mismatch health states.

## Phase 2: Market Data Depth

Purpose: replace metadata-only research with enough market microstructure to support real strategy families.

Deliverables:

- Add canonical token, outcome, event, and relation catalog records.
- Add CLOB order-book and trade ingest fixtures with raw and normalized append-only storage.
- Add point-in-time top-of-book, spread, depth, and trade-history read APIs.
- Add data-quality checks for stale books, gaps, malformed payloads, schema hash changes, and token/market mapping drift.
- Extend backtest replay to consume book/trade history instead of relying only on market snapshots.

Acceptance:

- Backtests can reject stale books and simulate top-of-book fills from historical book snapshots.
- Research tools can retrieve book history and trade history by market/token and time window.
- Data health identifies stale stream, missing token mapping, and malformed payload examples.

## Phase 3: Strategy Robustness

Purpose: make promotion harder to game and less dependent on a single run.

Deliverables:

- Add walk-forward runner support with rolling windows and locked holdout.
- Add sensitivity analysis across strategy parameters and execution assumptions.
- Add experiment comparison and leaderboard tools with skip/rejection reasons.
- Add overfitting, sample-size, parameter-instability, and baseline-comparison checks to evaluator gates.
- Add richer paper-vs-backtest drift dimensions for missed fills, adverse selection, and opportunity decay.

Acceptance:

- Tiny-live eligibility requires walk-forward and paper evidence, not only one backtest.
- Evaluator explains every rejection with deterministic metrics and thresholds.
- Experiment comparison can rank candidates without mutating experiment definitions.

## Phase 4: Production Adapter Boundaries

Purpose: prepare the local services to move behind durable infrastructure without changing the agent trust model.

Deliverables:

- Define repository interfaces for experiments, backtests, paper, risk, execution, governance, and audit storage.
- Add conformance tests that run against the filesystem implementation and future database-backed implementations.
- Define event schemas for Redpanda/Kafka topics and analytics projections.
- Define signer-service request/response contract and refusal cases.
- Define OPA policy input/output contract while keeping application-level invariant checks in code.

Acceptance:

- Filesystem stores are one implementation behind explicit ports.
- Production adapters can be added without giving the agent broader permissions.
- Policy decisions remain auditable across application checks and future OPA checks.

## Phase 5: Shadow Production

Purpose: run production-like workflows with live data and no capital.

Deliverables:

- Durable workflow scheduling for ingest, backtests, walk-forward jobs, and paper runs.
- Database and object-store persistence for transactional metadata, audit records, raw payloads, and analytics.
- Live Polymarket adapter conformance tests for books, trades, order submission, cancellation, fills, and reconciliation.
- OpenTelemetry instrumentation, dashboards, alerts, and incident runbooks.
- Deployment boundaries for research, data, execution, signer, governance, and observability zones.

Acceptance:

- Shadow production receives live data and runs paper workloads under production-like latency and policy.
- Signer and live executor remain isolated from the research zone.
- Operators can answer "why did this trade happen?" and "why was this trade rejected?" within minutes.

## Active Next Threads

1. Implement `SystemHealthService` and `cashbox get-system-health`.
2. Deepen audit timeline and operator evidence queries across experiments, trade intents, risk decisions, executions, governance requests, and health evidence.
3. Add CLOB order-book/trade fixtures and point-in-time read APIs.
4. Add walk-forward runner contracts and evaluator gate placeholders.
5. Extract storage ports before introducing database-backed implementations.
