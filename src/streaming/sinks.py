"""Sinks: idempotent TimescaleDB upserts and Delta Lake writes.

Timescale writes use a staging table + ``INSERT ... ON CONFLICT`` so replayed
micro-batches converge to the same state (exactly-once semantics on top of
Spark's at-least-once ``foreachBatch``). Delta writes use ``MERGE`` for the same
reason.
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession

from common.config import DeltaSettings, Settings, TimescaleSettings
from common.logging import get_logger
from streaming.transforms import malformed_trades, valid_trades

logger = get_logger(__name__)

UPSERT_CANDLES_SQL = """
INSERT INTO candles (
    symbol, window_start, window_end, open, high, low, close,
    volume, buy_volume, sell_volume, vwap, trade_count
)
SELECT
    symbol, window_start, window_end, open, high, low, close,
    volume, buy_volume, sell_volume, vwap, trade_count
FROM candles_staging
ON CONFLICT (symbol, window_start) DO UPDATE SET
    window_end = EXCLUDED.window_end,
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    volume = EXCLUDED.volume,
    buy_volume = EXCLUDED.buy_volume,
    sell_volume = EXCLUDED.sell_volume,
    vwap = EXCLUDED.vwap,
    trade_count = EXCLUDED.trade_count,
    updated_at = now();
"""

UPSERT_ANOMALIES_SQL = """
INSERT INTO anomalies (
    symbol, window_start, window_end, metric, value, mean, stddev, zscore
)
SELECT symbol, window_start, window_end, metric, value, mean, stddev, zscore
FROM anomalies_staging
ON CONFLICT (symbol, window_start, metric) DO UPDATE SET
    window_end = EXCLUDED.window_end,
    value = EXCLUDED.value,
    mean = EXCLUDED.mean,
    stddev = EXCLUDED.stddev,
    zscore = EXCLUDED.zscore,
    updated_at = now();
"""


def _pg_kwargs(cfg: TimescaleSettings) -> dict[str, Any]:
    return {
        "host": cfg.host,
        "port": cfg.port,
        "dbname": cfg.database,
        "user": cfg.user,
        "password": cfg.password,
    }


def _jdbc_properties(cfg: TimescaleSettings) -> dict[str, str]:
    return {"user": cfg.user, "password": cfg.password, "driver": "org.postgresql.Driver"}


def upsert_timescale(
    batch_df: DataFrame,
    cfg: TimescaleSettings,
    staging_table: str,
    upsert_sql: str,
) -> None:
    """Land the batch in a staging table, then merge it into the target table."""
    import psycopg

    with psycopg.connect(**_pg_kwargs(cfg), autocommit=True) as conn:
        conn.execute(f"TRUNCATE {staging_table}")

    batch_df.write.jdbc(
        url=cfg.jdbc_url,
        table=staging_table,
        mode="append",
        properties=_jdbc_properties(cfg),
    )

    with psycopg.connect(**_pg_kwargs(cfg), autocommit=True) as conn:
        conn.execute(upsert_sql)


def merge_delta(
    spark: SparkSession, batch_df: DataFrame, path: str, key_columns: list[str]
) -> None:
    """Upsert a batch into a Delta table, creating it on first write."""
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, path):
        batch_df.write.format("delta").mode("overwrite").save(path)
        return

    condition = " AND ".join(f"t.{column} = s.{column}" for column in key_columns)
    (
        DeltaTable.forPath(spark, path)
        .alias("t")
        .merge(batch_df.alias("s"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def append_delta(batch_df: DataFrame, path: str) -> None:
    batch_df.write.format("delta").mode("append").save(path)


def make_candles_batch_sink(
    spark: SparkSession, timescale: TimescaleSettings, delta: DeltaSettings
):
    def _sink(batch_df: DataFrame, epoch_id: int) -> None:
        logger.info("candles batch %s: %d rows", epoch_id, batch_df.count())
        upsert_timescale(batch_df, timescale, "candles_staging", UPSERT_CANDLES_SQL)
        if delta.enabled:
            merge_delta(spark, batch_df, f"{delta.base_path}/candles", ["symbol", "window_start"])

    return _sink


def make_anomalies_batch_sink(
    spark: SparkSession, timescale: TimescaleSettings, delta: DeltaSettings
):
    def _sink(batch_df: DataFrame, epoch_id: int) -> None:
        logger.info("anomalies batch %s: %d rows", epoch_id, batch_df.count())
        upsert_timescale(batch_df, timescale, "anomalies_staging", UPSERT_ANOMALIES_SQL)
        if delta.enabled:
            merge_delta(
                spark,
                batch_df,
                f"{delta.base_path}/anomalies",
                ["symbol", "window_start", "metric"],
            )

    return _sink


def make_raw_batch_sink(spark: SparkSession, settings: Settings):
    """Persist validated raw trades to the Delta bronze zone; route bad payloads
    to the Kafka dead-letter topic."""

    delta = settings.delta
    kafka = settings.kafka
    spark_cfg = settings.spark

    def _sink(batch_df: DataFrame, epoch_id: int) -> None:
        if delta.enabled:
            append_delta(valid_trades(batch_df), f"{delta.base_path}/raw_trades")

        dead = malformed_trades(batch_df)
        if not dead.isEmpty():
            count = dead.count()
            logger.warning("routing %d malformed records to %s", count, kafka.dead_letter_topic)
            (
                dead.write.format("kafka")
                .option("kafka.bootstrap.servers", spark_cfg.kafka_bootstrap_servers)
                .option("topic", kafka.dead_letter_topic)
                .save()
            )

    return _sink
