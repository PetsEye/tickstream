"""Spark schemas for the wire format."""

from __future__ import annotations

from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

# Mirrors common.schemas.Trade. Declared explicitly (never inferred) so that
# malformed records fail fast instead of silently corrupting downstream state.
TRADE_SCHEMA = StructType(
    [
        StructField("trade_id", StringType(), nullable=False),
        StructField("exchange", StringType(), nullable=False),
        StructField("symbol", StringType(), nullable=False),
        StructField("price", DoubleType(), nullable=False),
        StructField("quantity", DoubleType(), nullable=False),
        StructField("side", StringType(), nullable=False),
        StructField("trade_ts", LongType(), nullable=False),
        StructField("ingest_ts", LongType(), nullable=False),
    ]
)
