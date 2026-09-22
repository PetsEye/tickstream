"""Canonical wire format shared by the producer and the Spark job.

Every exchange is normalized into this single schema before it reaches Kafka so
downstream Spark logic never has to know which venue a trade came from.
"""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field, field_validator


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class Trade(BaseModel):
    """A single normalized trade, serialized to Kafka as JSON."""

    trade_id: str = Field(..., description="Globally unique trade id (prefixed by exchange)")
    exchange: str = Field(..., description="Source venue, e.g. coinbase")
    symbol: str = Field(..., description="Normalized symbol, e.g. BTC-USD")
    price: float = Field(..., gt=0)
    quantity: float = Field(..., ge=0)
    side: Side = Field(..., description="Taker side of the trade")
    trade_ts: int = Field(..., description="Exchange event time, epoch milliseconds")
    ingest_ts: int = Field(..., description="Producer ingest time, epoch milliseconds")

    @field_validator("symbol")
    @classmethod
    def _upper_symbol(cls, value: str) -> str:
        return value.upper()

    def to_kafka_value(self) -> bytes:
        return self.model_dump_json().encode("utf-8")
