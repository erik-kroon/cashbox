# Next Threads

This queue is active-only. Each thread should be small enough for one implementation pass and preserve the current trust boundary: the agent can propose and inspect, but risk, execution, signing, and governance stay deterministic and audited.

## 1. System Health Service

Implement a local `SystemHealthService` that aggregates existing health signals into one operator-facing report.

Scope:

- Add `src/cashbox/health.py`.
- Add a workspace dependency in `src/cashbox/runtime.py`.
- Add `cashbox get-system-health` in the CLI command structure.
- Include ingest freshness, gateway audit presence, experiment/backtest/paper counts by status, risk/execution policy health, global halt state, open execution count, reconciliation mismatch count, and pending governance requests.
- Add tests in `tests/test_health.py`.

Acceptance:

- The service returns `OK`, `DEGRADED`, or `HALTED`.
- Stale ingest, unhealthy signer/executor policy, active global halt, pending governance requests, and reconciliation mismatches are visible as distinct checks.
- The CLI emits JSON consistent with the rest of the project.

## 2. Audit Timeline Query

Add a timeline query that explains how a market, experiment, trade intent, risk decision, execution record, or governance request moved through the system.

Scope:

- Extend existing audit aggregation in `src/cashbox/governance.py` or extract an audit read service if the implementation grows.
- Add filters for `experiment_id`, `market_id`, `intent_id`, `decision_id`, `execution_id`, and `request_id`.
- Add `cashbox get-audit-timeline`.
- Cover gateway, governance, risk, execution, and reconciliation events where identifiers are available.

Acceptance:

- A rejected trade intent can be traced from experiment status through risk checks and human review.
- Missing identifiers are handled gracefully without hiding unrelated audit entries.

## 3. CLOB Book And Trade Fixtures

Start the market-data-depth phase with local fixtures and read APIs before adding network adapters.

Scope:

- Add raw and normalized storage for order-book snapshots and trades.
- Add token-to-market lookup through the existing market ingest model.
- Add research read methods for top-of-book, book history, and trade history.
- Add gateway allowlisted read-only tools for the new methods.
- Add tests covering point-in-time reads and stale-book health.

Acceptance:

- Backtest and research code can read historical book state by token and time window.
- Data health can report stale or missing book coverage separately from stale metadata ingest.
