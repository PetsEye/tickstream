"""Spark transformation tests (skipped automatically without pyspark)."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

pytest.importorskip("pyspark")

from streaming.transforms import (  # noqa: E402
    build_anomalies,
    build_candles,
    malformed_trades,
    parse_trades,
    valid_trades,
)

KAFKA_SCHEMA = (
    "key string, value string, topic string, partition int, offset long, timestamp timestamp"
)


def as_utc(value: datetime) -> datetime:
    """Spark's collect() may return naive datetimes; normalise to aware UTC."""
    return (
        value.replace(tzinfo=timezone.utc)
        if value.tzinfo is None
        else value.astimezone(timezone.utc)
    )


def _trade_json(symbol: str, price: float, quantity: float, side: str, trade_ts: int) -> str:
    return json.dumps(
        {
            "trade_id": f"{symbol}:{trade_ts}",
            "exchange": "test",
            "symbol": symbol,
            "price": price,
            "quantity": quantity,
            "side": side,
            "trade_ts": trade_ts,
            "ingest_ts": trade_ts,
        }
    )


def test_parse_and_validate_roundtrip(spark):
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    rows = [
        ("k", _trade_json("BTC-USD", 100.0, 1.0, "BUY", 1_700_000_000_000), "trades", 0, 1, now),
        ("k", "not-json", "trades", 0, 2, now),
    ]
    kafka_df = spark.createDataFrame(rows, KAFKA_SCHEMA)

    parsed = parse_trades(kafka_df)
    valid = valid_trades(parsed).collect()
    bad = malformed_trades(parsed).collect()

    assert len(valid) == 1
    assert valid[0]["symbol"] == "BTC-USD"
    assert as_utc(valid[0]["event_time"]) == datetime(2023, 11, 14, 22, 13, 20, tzinfo=timezone.utc)
    assert len(bad) == 1
    assert bad[0]["value"] == "not-json"


def test_build_candles_computes_ohlcv_and_vwap(spark):
    base = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    rows = [
        ("BTC-USD", 100.0, 1.0, "BUY", datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)),
        ("BTC-USD", 110.0, 2.0, "SELL", datetime(2024, 1, 1, 0, 0, 10, tzinfo=timezone.utc)),
        ("BTC-USD", 90.0, 1.0, "BUY", datetime(2024, 1, 1, 0, 0, 20, tzinfo=timezone.utc)),
    ]
    df = spark.createDataFrame(
        rows, "symbol string, price double, quantity double, side string, event_time timestamp"
    )

    candles = build_candles(df, "1 minute", "1 minute").collect()
    assert len(candles) == 1
    candle = candles[0]
    assert as_utc(candle["window_start"]) == base
    assert candle["open"] == 100.0
    assert candle["high"] == 110.0
    assert candle["low"] == 90.0
    assert candle["close"] == 90.0
    assert candle["volume"] == 4.0
    assert candle["buy_volume"] == 2.0
    assert candle["sell_volume"] == 2.0
    assert candle["trade_count"] == 3
    assert candle["vwap"] == 102.5


def test_build_anomalies_flags_outlier_notional(spark):
    rows = [
        ("BTC-USD", 100.0, 1.0, datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)),
        ("BTC-USD", 101.0, 1.0, datetime(2024, 1, 1, 0, 0, 5, tzinfo=timezone.utc)),
        ("BTC-USD", 99.0, 1.0, datetime(2024, 1, 1, 0, 0, 10, tzinfo=timezone.utc)),
        ("BTC-USD", 100.0, 500.0, datetime(2024, 1, 1, 0, 0, 15, tzinfo=timezone.utc)),
    ]
    df = spark.createDataFrame(
        rows, "symbol string, price double, quantity double, event_time timestamp"
    )

    anomalies = build_anomalies(df, "5 minutes", "5 minutes", 1.0, 3).collect()
    assert len(anomalies) == 1
    assert anomalies[0]["metric"] == "max_trade_notional"
    assert anomalies[0]["zscore"] > 1.0
