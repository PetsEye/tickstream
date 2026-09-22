"""API response models (also drive the generated OpenAPI schema)."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class Candle(BaseModel):
    symbol: str
    window_start: datetime
    window_end: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    buy_volume: float
    sell_volume: float
    vwap: float
    trade_count: int


class Anomaly(BaseModel):
    symbol: str
    window_start: datetime
    window_end: datetime
    metric: str
    value: float
    mean: float
    stddev: float
    zscore: float
    detected_at: datetime | None = None


class SymbolSummary(BaseModel):
    symbol: str
    window_start: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap: float
    trade_count: int
