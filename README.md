# Cashbox

Cashbox is a governed research and execution control plane for prediction-market strategies.

It is built for a workflow where an AI research agent can inspect market data, create experiments, run simulations, and request live-adjacent actions, while deterministic software enforces the parts that should not be left to an agent: data provenance, backtest reproducibility, promotion gates, risk policy, audit trails, execution controls, and human approval.

Cashbox is not a trading bot with credentials attached to a prompt. The core design is:

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

## Status

This repository is an early, local, filesystem-backed implementation of the Cashbox control plane. It is useful for development, architecture validation, and local operator demos. It is not production trading infrastructure.

Implemented today:

- Polymarket-style market ingest with raw payload preservation, normalized snapshots, manifests, and append-only market history.
- Market history reads for metadata, token mapping, CLOB books, trades, top-of-book, book health, and ingest health.
- A constrained agent gateway with scoped credentials, tool authorization, input sanitization, rate limits, and audit logging.
- Experiment registry with immutable strategy configs, lifecycle history, cloning, and research notes.
- Deterministic backtests over point-in-time data with fees, latency, slippage, stale-book rejection, precision constraints, partial fills, artifacts, and failure explanations.
- Evaluator gates for paper, tiny-live, and scaled-live readiness.
- Paper-trading replay with persisted state and paper-vs-backtest drift reports.
- Trade intents, risk decisions, human review, approval tokens, live-submission stubs, halt controls, fill tracking, and reconciliation snapshots.
- Governance workflows for RBAC, approval requests, policy versioning, emergency halt, shared audit timelines, and operator evidence.

Planned production infrastructure such as Postgres, ClickHouse, Redpanda/Kafka, Temporal, Vault, Kubernetes, OPA, and a real isolated signer remains future work.

## Why Cashbox Exists

AI agents are useful for research exploration, but prediction-market execution needs stronger guarantees than natural-language reasoning can provide. Cashbox separates research authority from execution authority.

An agent can:

- Read sanctioned market data.
- Create and clone experiments.
- Attach research notes.
- Run approved backtests and paper workflows.
- Submit structured trade intents for review.
- Request halts through governed commands.

An agent cannot:

- Access private keys, exchange credentials, wallets, or production secrets.
- Submit live orders directly.
- Change risk limits or governance policy by itself.
- Bypass deterministic evaluator, risk, execution, or human approval gates.
- Treat a backtest or model explanation as sufficient proof for capital allocation.

## Architecture

The code is organized around trust zones and deep local modules.

```text
Research agent
     |
     v
Agent gateway  ->  sanctioned read tools + audit
     |
     v
Market history ->  raw payloads, normalized records, books, trades
     |
     v
Experiments -> backtests -> evaluator -> paper replay
     |
     v
Risk gateway -> governance -> execution controls -> reconciliation
     |
     v
Operator evidence + health + audit timeline
```

The current runtime is grouped into four composition modules:

- `market_research`: market storage, research read path, and agent gateway.
- `experiment_replay`: experiments, backtests, evaluator, and paper replay.
- `execution_governance`: risk gateway, execution state, and governance policy.
- `operator_evidence`: audit trail, operator evidence, and system health.

These groups keep dependency wiring local and make it easier to move individual implementations behind durable production adapters later.

## Installation

Cashbox requires Python 3.11 or newer.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

Run the test suite:

```bash
PYTHONPATH=src python3 -m unittest discover -s tests
```

By default, local data is written under `.cashbox/`.

## Quick Start

Ingest example market data:

```bash
cashbox ingest-file examples/gamma-markets.json
```

Or fetch directly from Polymarket Gamma:

```bash
cashbox ingest-polymarket --limit 100 --active true
```

Read market data through the research-facing interface:

```bash
cashbox list-active-markets --category politics
cashbox get-market-metadata election-2028
cashbox get-market-timeseries election-2028 --field question --field volume
cashbox get-ingest-health --stale-after-seconds 1800
```

Issue a local read-only gateway credential and call an agent tool:

```bash
cashbox issue-agent-credential --subject hermes
cashbox gateway-call list_active_markets \
  --token <issued-token> \
  --user-id hermes \
  --session-id session-001 \
  --args-json '{"query":"btc","limit":1}'
```

## Experiment Workflow

Create and inspect an immutable experiment:

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
cashbox get-experiment <experiment-id>
```

Run a deterministic backtest:

```bash
cashbox run-backtest <experiment-id> \
  --assumptions-json '{"simulation_level":"top_of_book","fee_model_version":"fees-v1","latency_model_version":"latency-v1","slippage_model_version":"slippage-v1","fill_model_version":"fills-v1","tick_size":"0.01","price_precision_dp":4,"quantity_precision_dp":4,"stale_book_threshold_seconds":600,"fee_bps":10,"slippage_bps":5,"latency_seconds":0,"partial_fill_ratio":"0.75","split_method":"chronological","train_ratio":"0.6","validation_ratio":"0.2","test_ratio":"0.2","baseline":"hold"}'
cashbox get-backtest-artifacts <run-id>
cashbox explain-backtest-failure <run-id>
```

Score the experiment and check whether it can move to paper:

```bash
cashbox score-experiment <experiment-id>
cashbox check-promotion-eligibility <experiment-id> --target-stage paper
cashbox check-promotion-eligibility <experiment-id> --target-stage paper --promote-if-eligible
```

Start paper replay and inspect drift:

```bash
cashbox start-paper-strategy <experiment-id> --run-id <run-id>
cashbox get-paper-state <experiment-id>
cashbox get-paper-results <paper-run-id>
cashbox analyze-paper-vs-backtest-drift <experiment-id>
cashbox stop-paper-strategy <experiment-id>
```

## Live-Adjacent Controls

Cashbox models live execution as a governed path. A strategy submits a trade intent, risk evaluates it, a human can approve or reject it, and only an approved intent can be submitted to the execution module.

```bash
cashbox create-trade-intent <experiment-id> \
  --submitted-by hermes \
  --order-json '{"market_id":"btc-150k","outcome":"Yes","side":"BUY","order_class":"TAKER_IOC","time_in_force":"IOC","price":"0.52","quantity":"20","estimated_fee_bps":"10","estimated_slippage_bps":"8"}'
cashbox evaluate-trade-intent <intent-id>
cashbox review-trade-intent <intent-id> --reviewer ops-oncall --decision approve --reason "approved for tiny-live"
cashbox submit-approved-order <intent-id> --approval-token <approval-token>
cashbox get-execution-state <intent-id>
cashbox record-live-fill <execution-id> --filled-quantity 5 --fill-price 0.52
cashbox request-strategy-cancel-all <experiment-id> --reason "operator requested stop"
cashbox request-global-halt --reason "hard halt after venue anomaly"
cashbox get-live-controls
```

Reconcile local execution state against venue state:

```bash
cashbox reconcile-live-state \
  --venue-orders-json '[{"order_id":"ord-123","status":"SUBMITTED"}]' \
  --venue-positions-json '[{"market_id":"btc-150k","outcome":"Yes","net_quantity":"5"}]'
```

## Operator And Governance Commands

Operators can inspect system health, audit timelines, governance requests, policy versions, and live controls without reading raw files by hand.

Common commands:

```bash
cashbox get-system-health
cashbox list-audit-events
cashbox get-audit-timeline
cashbox request-policy-change --help
cashbox request-emergency-halt --help
cashbox get-risk-decision <decision-id>
```

## Repository Layout

- `src/cashbox/market_history.py`: raw and normalized market history, token mapping, CLOB books, trades, and data-health reads.
- `src/cashbox/research.py`: sanctioned research read interface over market history.
- `src/cashbox/gateway.py`: local agent gateway runtime.
- `src/cashbox/gateway_contract.py`: gateway tool definitions, argument rules, dispatch metadata, and audit naming.
- `src/cashbox/experiments.py`: experiment registry, strategy templates, immutable configs, and lifecycle tracking.
- `src/cashbox/strategy_replay.py`: shared replay engine for backtest and paper simulations.
- `src/cashbox/backtests.py`: backtest orchestration, artifacts, and failure explanations.
- `src/cashbox/evaluator.py`: deterministic promotion gates.
- `src/cashbox/paper.py`: paper-trading runs, state transitions, and drift analysis.
- `src/cashbox/risk.py`: trade intents, risk evaluation, human review, and approval tokens.
- `src/cashbox/execution.py`: live-submission stubs, halt controls, fills, and reconciliation.
- `src/cashbox/governance.py`: RBAC, approval requests, policy lifecycle, and emergency halt workflows.
- `src/cashbox/audit.py`: audit listing and timeline reconstruction.
- `src/cashbox/operator_evidence.py`: operator-facing evidence aggregation.
- `src/cashbox/health.py`: system health summaries.
- `src/cashbox/runtime.py`: grouped runtime composition.
- `src/cashbox/commands/`: CLI command handlers.
- `docs/prd.md`: production product and architecture definition.
- `docs/roadmap.md`: implementation roadmap.
- `docs/NEXT-THREADS.md`: active follow-up work queue.

## Roadmap

The near-term roadmap is to keep the live boundary narrow while making the local system more realistic and observable.

1. Improve health and audit evidence for signer, executor, ingest, governance, and platform regressions.
2. Expand market-data coverage for CLOB books, trades, token mapping, and point-in-time research reads.
3. Add walk-forward and sensitivity analysis so promotion depends on robustness, not one backtest.
4. Extract storage ports and conformance tests before introducing database-backed implementations.
5. Add production adapters only after the trust model remains unchanged under local tests.

See `docs/roadmap.md` for the detailed plan.

## Safety Notice

Cashbox is experimental software. It is not financial advice, not a profitability claim, and not a complete production trading system. Do not connect it to real capital, private keys, or exchange credentials without adding the missing production controls described in the roadmap and PRD.
