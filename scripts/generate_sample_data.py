"""Generate the bundled synthetic dataset used for offline demos and CI.

Usage: ``python scripts/generate_sample_data.py [count]``
"""

from __future__ import annotations

import json
import random
import sys
from pathlib import Path

OUTPUT = Path("data/sample_trades.jsonl")
SEED = 42
START_TS_MS = 1_700_000_000_000

SEED_PRICES = {"BTC-USD": 60_000.0, "ETH-USD": 3_000.0, "SOL-USD": 150.0}
VOLATILITY = {"BTC-USD": 0.0006, "ETH-USD": 0.0009, "SOL-USD": 0.0015}


def generate(count: int) -> list[dict]:
    rng = random.Random(SEED)
    prices = dict(SEED_PRICES)
    records: list[dict] = []
    ts = START_TS_MS

    for index in range(count):
        symbol = rng.choice(list(SEED_PRICES))
        prices[symbol] = max(prices[symbol] * (1 + rng.gauss(0, VOLATILITY[symbol])), 0.01)
        ts += rng.randint(5, 400)
        records.append(
            {
                "trade_id": f"sample-{index:07d}",
                "exchange": "synthetic",
                "symbol": symbol,
                "price": round(prices[symbol], 2),
                "quantity": round(abs(rng.gauss(0.05, 0.08)) + 0.001, 6),
                "side": rng.choice(["BUY", "SELL"]),
                "trade_ts": ts,
                "ingest_ts": ts,
            }
        )

    records.sort(key=lambda record: record["trade_ts"])
    return records


def main() -> None:
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 2000
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT.open("w", encoding="utf-8") as handle:
        for record in generate(count):
            handle.write(json.dumps(record) + "\n")
    print(f"wrote {count} records to {OUTPUT}")


if __name__ == "__main__":
    main()
