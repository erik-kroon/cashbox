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

This repository is early and currently implements only the first vertical slice:

- append-first ingest of Polymarket Gamma market payloads
- immutable dataset manifests and normalized market snapshots
- append-only per-market history for point-in-time reads
- research read APIs for active markets, metadata, timeseries, and ingest health

That slice exists to support the first two derived user outcomes:

- a researcher can discover active markets and inspect sanitized, reproducible market data
- an operator can answer basic data-health and read-path questions from stored artifacts

Everything beyond that remains planned work.

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

By default, local data is stored under `.cashbox/market-data/`.

## Repository Layout

- `docs/prd.md`: target product and architecture definition
- `src/cashbox/ingest.py`: raw and normalized market ingest
- `src/cashbox/research.py`: deterministic research read path
- `src/cashbox/models.py`: normalized market and dataset models
- `src/cashbox/cli.py`: local ingest and read CLI
- `tests/test_market_data.py`: first-slice coverage

## Near-Term Roadmap

The next slices after this one are:

1. agent gateway for read-only market tools
2. experiment registry with immutable configs
3. deterministic backtest execution
4. evaluator and paper-promotion gates
5. paper trading, drift reporting, and execution controls

The repository should keep moving in that order so the research path grows before any live order path exists.
