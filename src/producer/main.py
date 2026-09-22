"""Producer entrypoint: exchange websocket -> normalize -> Kafka.

Run with ``python -m producer.main`` (requires ``PYTHONPATH=src`` or an
editable install).
"""

from __future__ import annotations

import asyncio
import contextlib
import signal

from common.config import load_config
from common.logging import get_logger, setup_logging
from producer.adapters import create_adapter
from producer.kafka_sink import KafkaTradeSink

LOG_EVERY = 100


def _install_signal_handlers(stop: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # pragma: no cover - non-POSIX platforms
            signal.signal(sig, lambda *_: stop.set())


async def run() -> None:
    settings = load_config()
    setup_logging(settings.app.log_level)
    logger = get_logger("producer.main")

    adapter = create_adapter(settings.producer)
    sink = KafkaTradeSink(
        bootstrap_servers=settings.kafka.bootstrap_servers,
        topic=settings.kafka.topic,
        client_id=settings.kafka.client_id,
    )

    stop = asyncio.Event()
    _install_signal_handlers(stop)

    await sink.start()
    logger.info(
        "source=%s symbols=%s -> kafka=%s topic=%s",
        adapter.name,
        adapter.symbols,
        settings.kafka.bootstrap_servers,
        settings.kafka.topic,
    )

    published = 0
    try:
        async for trade in adapter.stream():
            if stop.is_set():
                break
            await sink.send(trade)
            published += 1
            if published % LOG_EVERY == 0:
                logger.info(
                    "published %d trades (latest %s %s %.4f x %.6f)",
                    published,
                    trade.symbol,
                    trade.side.value,
                    trade.price,
                    trade.quantity,
                )
    finally:
        await sink.stop()
    logger.info("producer stopped after %d trades", published)


def main() -> None:
    with contextlib.suppress(KeyboardInterrupt):  # pragma: no cover
        asyncio.run(run())


if __name__ == "__main__":
    main()
