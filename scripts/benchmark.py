"""Benchmark harness for tickstream.

Measures real numbers against a running stack:

  produce    producer -> Kafka throughput and per-message publish latency
  delta      rows processed by Spark, from a Delta table's transaction log
  sse        database-write -> browser delivery latency over the SSE stream
  resources  container CPU / memory footprint

Examples (from the repo root, with PYTHONPATH=src):
  python scripts/benchmark.py delta --path /opt/tickstream/delta/raw_trades
  python scripts/benchmark.py produce --count 200000 --mode throughput
  python scripts/benchmark.py produce --count 2000 --mode latency
  python scripts/benchmark.py sse --seconds 60
  python scripts/benchmark.py resources
"""

from __future__ import annotations

import argparse
import asyncio
import http.client
import json
import random
import subprocess
import time
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from aiokafka import AIOKafkaProducer

DEFAULT_BOOTSTRAP = "localhost:29092"
DEFAULT_TOPIC = "trades"
DEFAULT_SYMBOL = "BENCH-USD"
DEFAULT_DELTA_PATH = "/opt/tickstream/delta/raw_trades"
DEFAULT_SPARK_CONTAINER = "tickstream-spark"
SSE_URL = "http://localhost:8000/api/stream"

SCRIPT_DIR = Path(__file__).resolve().parent
DELTA_STATS_SCRIPT = SCRIPT_DIR / "delta_stats.py"


def _percentiles(values: list[float]) -> dict[str, float]:
    if not values:
        return {}
    ordered = sorted(values)

    def pick(pct: float) -> float:
        index = min(len(ordered) - 1, int(round((pct / 100) * (len(ordered) - 1))))
        return round(ordered[index], 3)

    return {"p50": pick(50), "p95": pick(95), "p99": pick(99), "max": round(ordered[-1], 3)}


def _make_trade(symbol: str, price: float) -> dict:
    now = int(time.time() * 1000)
    return {
        "trade_id": f"bench:{uuid4().hex}",
        "exchange": "benchmark",
        "symbol": symbol,
        "price": round(price, 2),
        "quantity": round(random.uniform(0.001, 0.5), 6),
        "side": random.choice(["BUY", "SELL"]),
        "trade_ts": now,
        "ingest_ts": now,
    }


async def _produce(args: argparse.Namespace) -> dict:
    producer = AIOKafkaProducer(
        bootstrap_servers=args.bootstrap,
        acks="all",
        enable_idempotence=True,
        linger_ms=5,
        compression_type="gzip",
    )
    try:
        await asyncio.wait_for(producer.start(), timeout=args.connect_timeout)
    except asyncio.TimeoutError as exc:
        raise SystemExit(
            f"could not connect to Kafka at {args.bootstrap} within {args.connect_timeout}s"
        ) from exc
    price = 60_000.0
    latencies: list[float] = []
    sent = 0
    started = time.perf_counter()
    try:
        for _ in range(args.count):
            price = max(price * (1 + random.gauss(0, 0.0005)), 0.01)
            payload = json.dumps(_make_trade(args.symbol, price)).encode()
            if args.mode == "latency":
                t0 = time.perf_counter()
                await producer.send_and_wait(args.topic, key=args.symbol.encode(), value=payload)
                latencies.append((time.perf_counter() - t0) * 1000)
            else:
                await producer.send(args.topic, key=args.symbol.encode(), value=payload)
            sent += 1
        if args.mode != "latency":
            await producer.flush()
    finally:
        await producer.stop()
    elapsed = time.perf_counter() - started
    return {
        "mode": args.mode,
        "messages": sent,
        "seconds": round(elapsed, 3),
        "messages_per_sec": round(sent / elapsed, 1),
        "publish_latency_ms": _percentiles(latencies) if latencies else "n/a (throughput mode)",
    }


def cmd_produce(args: argparse.Namespace) -> None:
    print(json.dumps(asyncio.run(_produce(args)), indent=2))


def cmd_delta(args: argparse.Namespace) -> None:
    script = DELTA_STATS_SCRIPT.read_bytes()
    completed = subprocess.run(
        ["docker", "exec", "-i", args.container, "python3", "-", args.path],
        input=script,
        capture_output=True,
        check=True,
    )
    print(completed.stdout.decode())


def cmd_resources(args: argparse.Namespace) -> None:
    completed = subprocess.run(
        ["docker", "stats", "--no-stream", "--format", "{{json .}}"],
        capture_output=True,
        text=True,
        check=True,
    )
    rows = []
    for line in completed.stdout.splitlines():
        entry = json.loads(line)
        if "tickstream" in entry["Name"]:
            rows.append(
                {
                    "name": entry["Name"],
                    "cpu": entry["CPUPerc"],
                    "mem": entry["MemUsage"],
                    "mem_percent": entry["MemPerc"],
                }
            )
    print(json.dumps(sorted(rows, key=lambda r: r["name"]), indent=2))


def _frame_latency_ms(obj: dict) -> float | None:
    stamp = obj.get("updated_at") or obj.get("detected_at")
    if not stamp:
        return None
    written = datetime.fromisoformat(stamp)
    if written.tzinfo is None:
        written = written.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - written).total_seconds() * 1000


def cmd_sse(args: argparse.Namespace) -> None:
    parsed = urllib.parse.urlparse(args.url)
    connection = http.client.HTTPConnection(
        parsed.hostname, parsed.port or 80, timeout=args.seconds + 10
    )
    connection.request("GET", parsed.path or "/api/stream")
    response = connection.getresponse()

    event: str | None = None
    data_lines: list[str] = []
    latencies: dict[str, list[float]] = {"candle": [], "anomaly": []}
    counts = {"candle": 0, "anomaly": 0}
    deadline = time.time() + args.seconds

    try:
        while time.time() < deadline:
            try:
                raw = response.readline()
            except Exception:
                break
            if not raw:
                break
            text = raw.decode("utf-8", "replace").rstrip("\n")
            if text == "":
                if event in counts:
                    counts[event] += 1
                if event in latencies and data_lines:
                    try:
                        latency = _frame_latency_ms(json.loads("\n".join(data_lines)))
                        if latency is not None:
                            latencies[event].append(latency)
                    except Exception:
                        pass
                event, data_lines = None, []
                continue
            if text.startswith(":"):
                continue
            if text.startswith("event:"):
                event = text.split(":", 1)[1].strip()
            elif text.startswith("data:"):
                data_lines.append(text.split(":", 1)[1].strip())
    finally:
        connection.close()

    print(
        json.dumps(
            {
                "window_seconds": args.seconds,
                "frames": counts,
                "delivery_latency_ms": {
                    name: _percentiles(values) for name, values in latencies.items() if values
                },
            },
            indent=2,
        )
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="tickstream benchmark harness")
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--connect-timeout", type=float, default=15.0)
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("produce", help="producer -> Kafka throughput / latency")
    p.add_argument("--count", type=int, default=200_000)
    p.add_argument("--symbol", default=DEFAULT_SYMBOL)
    p.add_argument("--mode", choices=["latency", "throughput"], default="throughput")
    p.set_defaults(func=cmd_produce)

    p = sub.add_parser("delta", help="rows processed by Spark, from the Delta log")
    p.add_argument("--path", default=DEFAULT_DELTA_PATH)
    p.add_argument("--container", default=DEFAULT_SPARK_CONTAINER)
    p.set_defaults(func=cmd_delta)

    p = sub.add_parser("sse", help="database-write -> browser delivery latency")
    p.add_argument("--url", default=SSE_URL)
    p.add_argument("--seconds", type=int, default=60)
    p.set_defaults(func=cmd_sse)

    p = sub.add_parser("resources", help="container CPU / memory footprint")
    p.set_defaults(func=cmd_resources)

    return parser


def main() -> None:
    args = build_parser().parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
