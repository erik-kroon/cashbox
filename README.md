# Cashbox

Cashbox is a governed prediction-market research and execution platform.

The goal is not to let an LLM trade directly. The goal is to let an LLM generate, inspect, and manage research workflows while deterministic infrastructure preserves data integrity, validates strategy quality, enforces risk policy, and keeps signing and live execution outside the research trust boundary.

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

## What Cashbox Is Becoming

Cashbox is intended to be a production-grade operating system for autonomous prediction-market research with a controlled live-trading boundary.

Target capabilities:

- continuous ingest of market metadata, books, trades, wallet activity, and resolution data
- immutable raw and normalized datasets with point-in-time reproducibility
- strategy research workflows driven by an LLM through a constrained tool API
- deterministic backtesting with fees, slippage, latency, stale-book rejection, and partial fills
- paper trading and drift analysis before any live capital is touched
- risk-gated trade intents, isolated signing, and auditable execution
- full observability for data health, research decisions, promotions, and live actions

Cashbox is explicitly not:

- an unconstrained trading bot
- a prompt connected directly to exchange credentials
- a system where backtests or model reasoning are treated as proof
- a path for an agent to bypass policy, risk, or human capital governance

## Architecture

Cashbox is designed around separate trust zones:

- `research`: LLM-driven hypothesis generation, report writing, and experiment orchestration
- `data`: append-first ingestion, normalized market data, features, and quality monitoring
- `research compute`: deterministic backtests, walk-forward runs, and simulation workloads
- `execution`: risk gateway, paper execution, live order state, and reconciliation
- `signer`: isolated signing service with no direct agent access

At the product boundary, Hermes or another research agent interacts with Cashbox through a capability-gated tool API. The agent can read sanctioned datasets, create experiments, run approved research jobs, and request live-adjacent actions such as trade intents. It cannot submit orders directly, read secrets, edit risk policy, or access execution hosts.

## System Shape

Planned production components:

- `market-data-ingestor`: preserves raw payloads and emits normalized market events
- `market-catalog`: maintains canonical market metadata and relation mappings
- `feature-builder`: computes point-in-time feature datasets
- `experiment-service`: stores immutable hypotheses, configs, and run lineage
- `backtest-runner`: executes reproducible simulations
- `walk-forward-runner`: validates robustness across regimes
- `evaluator`: promotes or rejects strategies deterministically
- `paper-executor`: measures live behavior without capital risk
- `risk-gateway`: enforces live-trading policy and invariants
- `signer-service`: signs only approved payloads
- `live-executor`: interfaces with the exchange adapter
- `governance-service`: handles approvals, RBAC, and audit review

## Current Status

This repository is early and currently implements the first three local vertical slices:

- append-first ingest of Polymarket Gamma market payloads
- immutable dataset manifests and normalized market snapshots
- append-only per-market history for point-in-time reads
- research read APIs for active markets, metadata, timeseries, and ingest health
- a local agent gateway for approved read-only market tools with audit logging
- an experiment registry with immutable definitions, append-only lifecycle history, and research notes
- a deterministic backtest runner with immutable assumptions, persisted artifacts, and failure explanations

That slice exists to support the first two derived user outcomes:

- a researcher can discover active markets and inspect sanitized, reproducible market data
- an operator can answer basic data-health and read-path questions from stored artifacts

Everything beyond that remains planned work.

The repository now also includes:

- a scoped agent gateway that exposes only approved read-only market tools
- credential issuance with per-tool authorization and fixed-window rate limits
- input sanitization and append-only audit logs for each gateway call
- a filesystem-backed experiment service for templates, validation, creation, cloning, and lifecycle tracking
- a filesystem-backed backtest service that replays point-in-time market history and models fees, latency, slippage, staleness, and partial fills deterministically
- a filesystem-backed paper-trading service that replays post-backtest history, measures fill drift, and persists paper run state
- a filesystem-backed trade-intent and risk-gateway service with deterministic live checks, auditable decisions, and explicit human approval/rejection records

## Local Usage

Create a virtualenv and install the package:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

Ingest a local file of Polymarket-style market payloads:

```bash
cashbox ingest-file examples/gamma-markets.json
```

Fetch directly from Polymarket Gamma and persist a dataset:

```bash
cashbox ingest-polymarket --limit 100 --active true
```

Read the research-facing market data:

```bash
cashbox list-active-markets --category politics
cashbox get-market-metadata election-2028
cashbox get-market-timeseries election-2028 --field question --field volume
cashbox get-ingest-health --stale-after-seconds 1800
```

Issue a local read-only gateway credential and call a tool through the gateway:

```bash
cashbox issue-agent-credential --subject hermes
cashbox gateway-call list_active_markets \
  --token <issued-token> \
  --user-id hermes \
  --session-id session-001 \
  --args-json '{"query":"btc","limit":1}'
```

By default, local data is stored under `.cashbox/market-data/`.

Create and inspect immutable experiments:

```bash
cashbox list-strategy-families
cashbox get-strategy-template midpoint_reversion
cashbox create-experiment \
  --hypothesis "Mean reversion after thin overnight liquidity dislocations" \
  --strategy-family midpoint_reversion \
  --config-json '{"market_id":"btc-150k","lookback_minutes":30,"entry_zscore":2.1,"exit_zscore":0.7,"max_position_usd":250}' \
  --dataset-id 20260424T100000Z-demo \
  --code-version local-dev \
  --generated-by hermes
cashbox transition-experiment-status <experiment-id> --status VALIDATED_CONFIG --changed-by evaluator
cashbox attach-research-note <experiment-id> --author hermes --markdown "Spread widened after CPI headlines."
cashbox list-experiments --status VALIDATED_CONFIG
cashbox get-experiment <experiment-id>
```

Run a deterministic backtest and inspect its artifacts:

```bash
cashbox run-backtest <experiment-id> \
  --assumptions-json '{"simulation_level":"top_of_book","fee_model_version":"fees-v1","latency_model_version":"latency-v1","slippage_model_version":"slippage-v1","fill_model_version":"fills-v1","tick_size":"0.01","price_precision_dp":4,"quantity_precision_dp":4,"stale_book_threshold_seconds":600,"fee_bps":10,"slippage_bps":5,"latency_seconds":0,"partial_fill_ratio":"0.75","split_method":"chronological","train_ratio":"0.6","validation_ratio":"0.2","test_ratio":"0.2","baseline":"hold"}'
cashbox get-backtest-artifacts <run-id>
cashbox explain-backtest-failure <run-id>
```

Score an experiment and evaluate the paper-promotion gate:

```bash
cashbox score-experiment <experiment-id>
cashbox check-promotion-eligibility <experiment-id> --target-stage paper
cashbox check-promotion-eligibility <experiment-id> --target-stage paper --promote-if-eligible
```

Start a paper run, inspect drift, and finalize the paper stage:

```bash
cashbox start-paper-strategy <experiment-id> --run-id <run-id>
cashbox get-paper-state <experiment-id>
cashbox get-paper-results <paper-run-id>
cashbox analyze-paper-vs-backtest-drift <experiment-id>
cashbox stop-paper-strategy <experiment-id>
```

Create a live-adjacent trade intent and evaluate the HITL risk path:

```bash
cashbox create-trade-intent <experiment-id> \
  --submitted-by hermes \
  --order-json '{"market_id":"btc-150k","outcome":"Yes","side":"BUY","order_class":"TAKER_IOC","time_in_force":"IOC","price":"0.52","quantity":"20","estimated_fee_bps":"10","estimated_slippage_bps":"8"}'
cashbox evaluate-trade-intent <intent-id>
cashbox review-trade-intent <intent-id> --reviewer ops-oncall --decision approve --reason "approved for tiny-live"
cashbox evaluate-trade-intent <intent-id>
cashbox submit-approved-order <intent-id> --approval-token <approval-token>
cashbox get-execution-state <intent-id>
cashbox get-execution-record <execution-id>
cashbox get-risk-decision <decision-id>
```

## Repository Layout

- `docs/prd.md`: target product and architecture definition
- `src/cashbox/backtests.py`: deterministic backtest execution, artifacts, and failure explanations
- `src/cashbox/evaluator.py`: experiment scoring and deterministic paper-promotion gates
- `src/cashbox/paper.py`: paper-trading runs, state transitions, and backtest drift analysis
- `src/cashbox/risk.py`: trade intents, risk evaluation, human review, and approval tokens
- `src/cashbox/execution.py`: signer-service release, live-executor submission, and execution audit trail
- `src/cashbox/ingest.py`: raw and normalized market ingest
- `src/cashbox/research.py`: deterministic research read path
- `src/cashbox/experiments.py`: experiment registry, immutable configs, and lifecycle tracking
- `src/cashbox/models.py`: normalized market and dataset models
- `src/cashbox/cli.py`: local ingest and read CLI
- `tests/test_backtests.py`: deterministic backtest coverage
- `tests/test_paper.py`: paper-trading and drift-report coverage
- `tests/test_market_data.py`: first-slice coverage
- `tests/test_experiments.py`: experiment registry coverage

## Near-Term Roadmap

The next slices after this one are:

1. cancel-all flows, broader live-state reconciliation, and fill/position tracking
2. governance approvals, RBAC, and policy lifecycle controls
3. richer observability for signer, executor, and platform health regressions

The repository should keep moving in that order so the live boundary stays narrow, auditable, and easy to reason about.
