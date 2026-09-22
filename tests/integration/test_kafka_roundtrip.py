"""Integration test: requires a running Kafka broker.

Enable with ``TICKSTREAM_INTEGRATION=1`` and ``KAFKA_BOOTSTRAP_SERVERS``.
Skipped by default so CI stays hermetic.
"""

from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("TICKSTREAM_INTEGRATION") != "1",
    reason="set TICKSTREAM_INTEGRATION=1 and KAFKA_BOOTSTRAP_SERVERS to run",
)


@pytest.mark.asyncio
async def test_producer_publishes_to_kafka():
    from aiokafka import AIOKafkaConsumer

    from common.schemas import Side, Trade
    from producer.kafka_sink import KafkaTradeSink

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.environ.get("TICKSTREAM_KAFKA__TOPIC", "trades")

    sink = KafkaTradeSink(bootstrap_servers=bootstrap, topic=topic)
    await sink.start()
    await sink.send(
        Trade(
            trade_id="integration:1",
            exchange="test",
            symbol="BTC-USD",
            price=1.0,
            quantity=1.0,
            side=Side.BUY,
            trade_ts=1,
            ingest_ts=1,
        )
    )
    await sink.stop()

    consumer = AIOKafkaConsumer(
        topic, bootstrap_servers=bootstrap, auto_offset_reset="earliest", group_id="tickstream-it"
    )
    await consumer.start()
    try:
        message = await consumer.getone()
        assert b"integration:1" in message.value
    finally:
        await consumer.stop()
