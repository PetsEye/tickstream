# Benchmarks

Real numbers measured against a running stack, plus the exact commands to
reproduce them. These are **single-node local** figures on one machine, not a
distributed cluster benchmark.

## Environment

| | |
|---|---|
| Host | Apple Silicon (arm64), Docker Desktop, 7.65 GiB allocated |
| Spark | 4.0.4, `local[2]`, driver `1g`, `spark.sql.shuffle.partitions=4` |
| Kafka | 4.3.1, single-node KRaft |
| TimescaleDB | PostgreSQL 17 |
| Producer | Python 3.12, `aiokafka`, `acks=all`, idempotent, gzip |

Reproduce everything with:

```bash
docker compose --profile web up -d --build
./scripts/measure.sh                      # defaults: 50k throughput, 3k latency, 75s SSE
COUNT=50000 LATENCY_COUNT=3000 SSE_SECONDS=75 ./scripts/measure.sh
```

## Results

### Producer → Kafka throughput

Asynchronous batched produce (gzip, `acks=all`, idempotent). Across runs:

| Metric | Value |
|---|---|
| Throughput | **~40,000 msg/s** (observed range 25k–58k) |
| 50,000 messages | 0.9–2.0 s |
| 100,000 messages | 2.8 s |

### Producer publish latency

`send_and_wait` per message (waits for the broker ack), so this is a true durable
write latency, not fire-and-forget:

| p50 | p95 | p99 | max |
|---|---|---|---|
| ~7 ms | ~8–10 ms | ~10–15 ms | 21–47 ms |

### End-to-end processed throughput (Kafka → Spark → Delta)

Measured by summing `numRecords` in the `raw_trades` Delta transaction log
(`scripts/delta_stats.py`) before and after a load, over the time to fully drain
the backlog. Throughput is deliberately bounded by
`maxOffsetsPerTrigger / triggerInterval`; tuning shows the headroom:

| Configuration | Records | Elapsed | Throughput |
|---|---|---|---|
| Default: 5,000 / 5 s | 50,000 | 50 s | **1,000 rec/s** |
| Tuned: 20,000 / 2 s | 50,003 | 8 s | **6,250 rec/s** |
| Aggressive: 100,000 / 1 s | 100,000 | 8 s | **12,500 rec/s** |

The default is a config choice for smooth, low-latency micro-batches, not a Spark
ceiling. Override without editing code:

```bash
MAX_OFFSETS_PER_TRIGGER=20000 SPARK_TRIGGER_INTERVAL="2 seconds" docker compose up -d spark
```

### SSE delivery latency (database write → browser)

Time from a row's `updated_at` (the Postgres write) to the client receiving the
frame over Server-Sent Events, measured under live Coinbase traffic:

| Event | p50 | p95 | p99 |
|---|---|---|---|
| candle | ~11–13 ms | ~14–20 ms | ~14–20 ms |
| anomaly | ~12–14 ms | ~18–30 ms | ~18–30 ms |

Path: Spark upsert → `pg_notify` → asyncpg `LISTEN` → SSE → localhost client.

### Container footprint

| Service | Memory | Notes |
|---|---|---|
| spark | ~1.7–1.9 GiB | spikes to ~350% CPU while draining a backlog |
| kafka | ~400–440 MiB | ~50% CPU during sustained ingestion |
| timescaledb | ~125–130 MiB | |
| grafana | ~100 MiB | |
| api | ~42 MiB | |
| web (nginx) | ~9 MiB | |
| **total** | **~2.5 GiB** | of 7.65 GiB allocated |

### Cross-check: GitHub-hosted runner

The `benchmark` workflow (`.github/workflows/benchmark.yml`) runs the same suite
on a `ubuntu-latest` runner (4 vCPU, 15.6 GiB) and publishes the output to the
job summary. One run:

| Metric | Local (Apple Silicon) | GitHub runner |
|---|---|---|
| Producer → Kafka | ~40k msg/s | 24.6k msg/s |
| Publish latency p50 / p99 | ~7 ms / ~15 ms | 6.5 ms / 13.3 ms |
| End-to-end (default config) | 1,000 rec/s | 943 rec/s |
| SSE candle p50 | ~11 ms | 15.5 ms |
| SSE anomaly p50 | ~12 ms | 17.0 ms |

Same order of magnitude on different hardware: the config-bounded end-to-end
rate and the millisecond-scale delivery latency both hold.

## Methodology & caveats

- **Local single-node topology.** No cluster, no replication, no multi-broker
  Kafka. These numbers characterise the code and the tuning knobs, not a
  production deployment.
- **Hardware-dependent.** Absolute values reflect one Apple Silicon laptop under
  Docker Desktop; relative comparisons and the config scaling are the useful part.
- **Durable writes.** Producer throughput/latency use gzip + idempotence +
  `acks=all`; a fire-and-forget producer would report higher throughput.
- **Throughput is capped by design.** `maxOffsetsPerTrigger` bounds how much a
  micro-batch ingests; the "aggressive" row shows the practical processing limit
  on this hardware (~12.5k rec/s) once the cap is removed.
- **SSE latency uses `updated_at`.** Both `candles` and `anomalies` stamp
  `updated_at = now()` on every upsert, so re-notified updates are measured
  against their own write rather than the row's first insert.
- **Benchmark traffic is isolated.** `measure.sh` pauses the live producer for
  the throughput run and restores it afterwards, and writes to a `BENCH-USD`
  symbol so live series are untouched.
