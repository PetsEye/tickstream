"""Pure, testable DataFrame transformations for the streaming job."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from streaming.schema import TRADE_SCHEMA


def parse_trades(kafka_df: DataFrame) -> DataFrame:
    """Decode Kafka values against the explicit trade schema.

    Keeps the raw JSON plus Kafka metadata so invalid records can be routed to a
    dead-letter topic and valid ones carry lineage (partition/offset).
    """
    return kafka_df.select(
        F.col("value").cast("string").alias("json"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    ).withColumn("trade", F.from_json(F.col("json"), TRADE_SCHEMA))


def valid_trades(parsed_df: DataFrame) -> DataFrame:
    """Flatten structurally valid trades and attach event time."""
    return (
        parsed_df.filter(F.col("trade").isNotNull())
        .select(
            F.col("trade.trade_id").alias("trade_id"),
            F.col("trade.exchange").alias("exchange"),
            F.col("trade.symbol").alias("symbol"),
            F.col("trade.price").alias("price"),
            F.col("trade.quantity").alias("quantity"),
            F.col("trade.side").alias("side"),
            F.col("trade.trade_ts").alias("trade_ts"),
            F.col("trade.ingest_ts").alias("ingest_ts"),
            F.col("kafka_partition").alias("kafka_partition"),
            F.col("kafka_offset").alias("kafka_offset"),
            F.timestamp_millis(F.col("trade.trade_ts")).alias("event_time"),
        )
        .filter((F.col("price") > 0) & (F.col("quantity") >= 0))
    )


def malformed_trades(parsed_df: DataFrame) -> DataFrame:
    """Raw payloads that could not be decoded (schema violations)."""
    return parsed_df.filter(F.col("trade").isNull()).select(F.col("json").alias("value"))


def deduplicate(trades_df: DataFrame, watermark_delay: str) -> DataFrame:
    """Event-time watermark plus dropDuplicates on trade_id.

    Watermarking bounds the dedup state; ``dropDuplicates`` is exact for
    duplicates that arrive within the watermark window.
    """
    return trades_df.withWatermark("event_time", watermark_delay).dropDuplicates(["trade_id"])


def build_candles(trades_df: DataFrame, window_duration: str, window_slide: str) -> DataFrame:
    """OHLCV + VWAP per symbol over event-time windows.

    ``min_by``/``max_by`` on event time make open/close deterministic, which
    ``first``/``last`` are not.
    """
    windowed = trades_df.groupBy(
        F.window(F.col("event_time"), window_duration, window_slide).alias("window"),
        F.col("symbol"),
    ).agg(
        F.min_by("price", "event_time").alias("open"),
        F.max("price").alias("high"),
        F.min("price").alias("low"),
        F.max_by("price", "event_time").alias("close"),
        F.sum("quantity").alias("volume"),
        F.sum(F.when(F.col("side") == "BUY", F.col("quantity")).otherwise(F.lit(0.0))).alias(
            "buy_volume"
        ),
        F.sum(F.when(F.col("side") == "SELL", F.col("quantity")).otherwise(F.lit(0.0))).alias(
            "sell_volume"
        ),
        F.sum(F.col("price") * F.col("quantity")).alias("notional"),
        F.count(F.lit(1)).alias("trade_count"),
    )
    return windowed.select(
        F.col("symbol"),
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.col("open"),
        F.col("high"),
        F.col("low"),
        F.col("close"),
        F.col("volume"),
        F.col("buy_volume"),
        F.col("sell_volume"),
        F.when(F.col("volume") > 0, F.col("notional") / F.col("volume"))
        .otherwise(F.col("close"))
        .alias("vwap"),
        F.col("trade_count"),
    )


def build_anomalies(
    trades_df: DataFrame,
    window: str,
    slide: str,
    zscore_threshold: float,
    min_trades: int,
) -> DataFrame:
    """Flag windows whose largest trade notional is a statistical outlier.

    A self-contained streaming-friendly detector: for each window we compute the
    z-score of the largest trade notional against the window's own mean/stddev.
    Windows below ``min_trades`` or with zero variance are ignored.
    """
    scored = (
        trades_df.withColumn("notional", F.col("price") * F.col("quantity"))
        .groupBy(
            F.window(F.col("event_time"), window, slide).alias("window"),
            F.col("symbol"),
        )
        .agg(
            F.avg("notional").alias("mean"),
            F.stddev("notional").alias("stddev"),
            F.max("notional").alias("value"),
            F.count(F.lit(1)).alias("trade_count"),
        )
        .filter((F.col("trade_count") >= min_trades) & (F.col("stddev") > 0))
        .withColumn("zscore", (F.col("value") - F.col("mean")) / F.col("stddev"))
        .filter(F.col("zscore") >= zscore_threshold)
    )
    return scored.select(
        F.col("symbol"),
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.lit("max_trade_notional").alias("metric"),
        F.col("value"),
        F.col("mean"),
        F.col("stddev"),
        F.col("zscore"),
    )
