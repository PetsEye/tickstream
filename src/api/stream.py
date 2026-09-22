"""Server-Sent Events backed by Postgres LISTEN/NOTIFY.

Spark writes trigger ``pg_notify`` on the candles/anomalies tables; this module
relays those notifications to connected browsers over a single SSE stream. No
polling, no second Kafka consumer.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import asyncpg
from starlette.requests import Request

from common.logging import get_logger

logger = get_logger(__name__)

CANDLE_CHANNEL = "tickstream_candles"
ANOMALY_CHANNEL = "tickstream_anomalies"
KEEPALIVE_SECONDS = 15
MAX_QUEUE = 1000


async def event_stream(pool: asyncpg.Pool, request: Request) -> AsyncIterator[str]:
    queue: asyncio.Queue[tuple[str, str]] = asyncio.Queue(maxsize=MAX_QUEUE)

    def make_callback(event: str):
        def _callback(connection, pid, channel, payload) -> None:  # noqa: ANN001
            try:
                queue.put_nowait((event, payload))
            except asyncio.QueueFull:
                logger.warning("SSE queue full, dropping %s notification", event)

        return _callback

    candle_callback = make_callback("candle")
    anomaly_callback = make_callback("anomaly")

    connection = await pool.acquire()
    await connection.add_listener(CANDLE_CHANNEL, candle_callback)
    await connection.add_listener(ANOMALY_CHANNEL, anomaly_callback)
    try:
        yield "retry: 3000\n\n"
        while True:
            if await request.is_disconnected():
                break
            try:
                event, payload = await asyncio.wait_for(queue.get(), timeout=KEEPALIVE_SECONDS)
            except asyncio.TimeoutError:
                yield ": keepalive\n\n"
                continue
            yield f"event: {event}\ndata: {payload}\n\n"
    finally:
        await connection.remove_listener(CANDLE_CHANNEL, candle_callback)
        await connection.remove_listener(ANOMALY_CHANNEL, anomaly_callback)
        await pool.release(connection)
