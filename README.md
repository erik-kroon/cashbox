# Cashbox

Cashbox is a small, deterministic scanner for binary prediction-market full-set arbitrage.

The first slice is intentionally narrow:

- no execution engine
- no forecasting model
- no market making

It only answers one question:

> Given a snapshot of YES/NO top-of-book quotes, do the hard constraints still leave positive expected value after fees and operational buffers?

## What It Scans

For each binary market:

- `buy_full_set`: buy YES and NO if `yes_ask + no_ask < 1.00` after fees and buffers
- `sell_full_set`: sell YES and NO if `yes_bid + no_bid > 1.00` after fees and buffers

The scanner models:

- category-specific taker fee rates
- fee formula `shares * fee_rate * price * (1 - price)`
- per-trade slippage buffer
- precision buffer
- safety margin
- available size from top-of-book liquidity

## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install .
```

## Input Format

Pass the CLI a JSON file containing a list of market snapshots:

```json
[
  {
    "market_id": "btc-above-100k-today",
    "category": "crypto",
    "yes": {
      "bid": "0.53",
      "ask": "0.56",
      "bid_size": "125",
      "ask_size": "75"
    },
    "no": {
      "bid": "0.45",
      "ask": "0.48",
      "bid_size": "130",
      "ask_size": "80"
    }
  }
]
```

## Usage

```bash
cashbox-scan examples/markets.json \
  --slippage 0.002 \
  --precision-buffer 0.001 \
  --safety-margin 0.003 \
  --min-edge 0.0
```

Example output:

```text
btc-above-100k-today buy_full_set qty=75 gross=0.070000 net=0.028986 pnl=2.173980
```

Live scan against Polymarket public APIs:

```bash
cashbox-scan \
  --polymarket-live \
  --limit 25 \
  --slippage 0.002 \
  --precision-buffer 0.001 \
  --safety-margin 0.003
```

This uses:

- `https://gamma-api.polymarket.com/markets` for live market discovery
- `https://clob.polymarket.com/book` for public order book snapshots

The loader handles a real API quirk: the CLOB `bids` and `asks` arrays are not guaranteed to arrive best-first, so Cashbox derives top-of-book by price rather than list position.

## Repo Layout

- `src/cashbox/models.py`: domain models and fee schedules
- `src/cashbox/scanner.py`: fee-aware full-set arb logic
- `src/cashbox/cli.py`: JSON-driven command-line entrypoint
- `src/cashbox/polymarket.py`: public Polymarket market and order book ingestion
- `tests/test_scanner.py`: stdlib `unittest` coverage for fee math and edge detection

## Next Steps

The natural next iteration is a read-only streaming path, either via Polymarket WebSockets or the Nautilus Polymarket adapter, so the scanner can evaluate near-real-time books instead of polling REST snapshots.
