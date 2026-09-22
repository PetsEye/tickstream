"""Synthetic replay adapter.

Replays a bundled JSONL dataset with compressed inter-arrival timing. This
guarantees the demo and CI run deterministically with no external network
dependency, while keeping Spark event-time windows current.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from pathlib import Path
from uuid import uuid4

from common.logging import get_logger
from common.schemas import Side, Trade
from producer.adapters.base import ExchangeAdapter, now_ms

logger = get_logger(__name__)

# Never sleep longer than this between replayed records, even at low speed.
_MAX_SLEEP_SECONDS = 1.0


class SyntheticAdapter(ExchangeAdapter):
    name = "synthetic"

    def __init__(
        self,
        symbols: list[str],
        file: str,
        speed: float = 50.0,
        loop: bool = True,
        reconnect_backoff_seconds: float = 5.0,
    ) -> None:
        super().__init__(symbols, reconnect_backoff_seconds)
        self.file = Path(file)
        self.speed = max(speed, 0.001)
        self.loop = loop

    def _load(self) -> list[dict]:
        if not self.file.exists():
            logger.warning("synthetic dataset not found: %s", self.file)
            return []
        records: list[dict] = []
        with self.file.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        records.sort(key=lambda record: record["trade_ts"])
        return records

    async def stream(self) -> AsyncIterator[Trade]:
        while True:
            previous_ts: int | None = None
            for record in self._load():
                if record["symbol"] not in self.symbols:
                    continue
                if previous_ts is not None:
                    gap_seconds = (record["trade_ts"] - previous_ts) / 1000.0
                    delay = min(max(gap_seconds, 0.0) / self.speed, _MAX_SLEEP_SECONDS)
                    if delay > 0:
                        await asyncio.sleep(delay)
                previous_ts = record["trade_ts"]
                emitted_at = now_ms()
                yield Trade(
                    trade_id=f"synthetic:{record['trade_id']}:{uuid4().hex[:8]}",
                    exchange=self.name,
                    symbol=record["symbol"],
                    price=float(record["price"]),
                    quantity=float(record["quantity"]),
                    side=Side(record["side"]),
                    trade_ts=emitted_at,
                    ingest_ts=emitted_at,
                )
            if not self.loop:
                logger.info("synthetic replay complete (%s)", self.file)
                return
