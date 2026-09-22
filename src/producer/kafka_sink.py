"""Async Kafka sink for normalized trades."""

from __future__ import annotations

from aiokafka import AIOKafkaProducer

from common.logging import get_logger
from common.schemas import Trade

logger = get_logger(__name__)


class KafkaTradeSink:
    """Thin wrapper around :class:`AIOKafkaProducer` with idempotent delivery.

    Trades are keyed by symbol so that every trade for a given symbol lands on
    the same partition, preserving per-symbol ordering for downstream windows.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        client_id: str = "tickstream-producer",
        acks: str = "all",
        linger_ms: int = 20,
    ) -> None:
        self._topic = topic
        self._producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            client_id=client_id,
            acks=acks,
            enable_idempotence=True,
            linger_ms=linger_ms,
            compression_type="gzip",
        )

    async def start(self) -> None:
        await self._producer.start()
        logger.info("connected to Kafka, publishing to topic %r", self._topic)

    async def stop(self) -> None:
        await self._producer.stop()

    async def send(self, trade: Trade) -> None:
        await self._producer.send_and_wait(
            self._topic,
            key=trade.symbol.encode("utf-8"),
            value=trade.to_kafka_value(),
        )
