"""Spark Structured Streaming entrypoint.

Three independent streaming queries share the ``trades`` topic:

1. ``raw_trades``  -> Delta bronze zone (+ Kafka dead-letter for bad payloads)
2. ``candles``     -> TimescaleDB (upsert) + Delta (merge)
3. ``anomalies``   -> TimescaleDB (upsert) + Delta (merge)

Each query has its own checkpoint so it can restart independently.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from common.config import Settings, load_config
from common.logging import get_logger, setup_logging
from streaming.sinks import (
    make_anomalies_batch_sink,
    make_candles_batch_sink,
    make_raw_batch_sink,
)
from streaming.transforms import (
    build_anomalies,
    build_candles,
    deduplicate,
    parse_trades,
    valid_trades,
)


def build_spark(settings: Settings) -> SparkSession:
    builder = (
        SparkSession.builder.appName(settings.spark.app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    )
    if settings.delta.enabled:
        builder = builder.config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        ).config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    return builder.getOrCreate()


def read_trades(spark: SparkSession, settings: Settings) -> DataFrame:
    """A fresh Kafka source per query (independent consumer groups)."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.spark.kafka_bootstrap_servers)
        .option("subscribe", settings.spark.topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", settings.spark.max_offsets_per_trigger)
        .load()
    )


def _deduped_trades(spark: SparkSession, settings: Settings) -> DataFrame:
    parsed = parse_trades(read_trades(spark, settings))
    return deduplicate(valid_trades(parsed), settings.spark.watermark_delay)


def main() -> None:
    settings = load_config()
    setup_logging(settings.app.log_level)
    logger = get_logger("streaming.main")

    spark = build_spark(settings)
    spark.sparkContext.setLogLevel("WARN")

    checkpoint_root = settings.spark.checkpoint_dir
    trigger = settings.spark.trigger_interval

    raw_query = (
        parse_trades(read_trades(spark, settings))
        .writeStream.foreachBatch(make_raw_batch_sink(spark, settings))
        .outputMode("append")
        .option("checkpointLocation", f"{checkpoint_root}/raw_trades")
        .trigger(processingTime=trigger)
        .queryName("raw_trades")
        .start()
    )

    candles = build_candles(
        _deduped_trades(spark, settings),
        settings.spark.window_duration,
        settings.spark.window_slide,
    )
    candles_query = (
        candles.writeStream.foreachBatch(
            make_candles_batch_sink(spark, settings.timescale, settings.delta)
        )
        .outputMode("update")
        .option("checkpointLocation", f"{checkpoint_root}/candles")
        .trigger(processingTime=trigger)
        .queryName("candles")
        .start()
    )

    queries = [raw_query, candles_query]

    if settings.anomaly.enabled:
        anomalies = build_anomalies(
            _deduped_trades(spark, settings),
            settings.anomaly.window,
            settings.anomaly.slide,
            settings.anomaly.zscore_threshold,
            settings.anomaly.min_trades,
        )
        anomalies_query = (
            anomalies.writeStream.foreachBatch(
                make_anomalies_batch_sink(spark, settings.timescale, settings.delta)
            )
            .outputMode("update")
            .option("checkpointLocation", f"{checkpoint_root}/anomalies")
            .trigger(processingTime=trigger)
            .queryName("anomalies")
            .start()
        )
        queries.append(anomalies_query)

    logger.info(
        "started %d streaming queries: %s",
        len(queries),
        ", ".join(f"{q.name}={q.id}" for q in queries),
    )
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
