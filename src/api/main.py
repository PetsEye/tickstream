"""tickstream query + streaming API over TimescaleDB."""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

from api.db import close_pool, get_pool, init_pool
from api.models import Anomaly, Candle, SymbolSummary
from api.stream import event_stream
from common.config import load_config
from common.logging import get_logger, setup_logging

settings = load_config()
setup_logging(settings.app.log_level)
logger = get_logger("api.main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_pool(settings.timescale)
    yield
    await close_pool()


app = FastAPI(
    title="tickstream API",
    version="0.1.0",
    summary="Query and live-stream crypto candles and anomalies.",
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health() -> dict[str, str]:
    async with get_pool().acquire() as connection:
        await connection.fetchval("SELECT 1")
    return {"status": "ok"}


@app.get("/api/symbols", response_model=list[str])
async def symbols() -> list[str]:
    rows = await get_pool().fetch("SELECT DISTINCT symbol FROM candles ORDER BY symbol")
    return [row["symbol"] for row in rows]


@app.get("/api/summary", response_model=list[SymbolSummary])
async def summary() -> list[SymbolSummary]:
    rows = await get_pool().fetch(
        """
        SELECT DISTINCT ON (symbol)
            symbol, window_start, open, high, low, close, volume, vwap, trade_count
        FROM candles
        ORDER BY symbol, window_start DESC
        """
    )
    return [SymbolSummary(**dict(row)) for row in rows]


@app.get("/api/candles", response_model=list[Candle])
async def candles(
    symbol: str = Query(..., description="Normalized symbol, e.g. BTC-USD"),
    limit: int = Query(300, ge=1, le=2000),
) -> list[Candle]:
    rows = await get_pool().fetch(
        """
        SELECT symbol, window_start, window_end, open, high, low, close,
               volume, buy_volume, sell_volume, vwap, trade_count
        FROM candles
        WHERE symbol = $1
        ORDER BY window_start DESC
        LIMIT $2
        """,
        symbol.upper(),
        limit,
    )
    return [Candle(**dict(row)) for row in reversed(rows)]


@app.get("/api/anomalies", response_model=list[Anomaly])
async def anomalies(limit: int = Query(100, ge=1, le=1000)) -> list[Anomaly]:
    rows = await get_pool().fetch(
        """
        SELECT symbol, window_start, window_end, metric, value, mean, stddev, zscore, detected_at
        FROM anomalies
        ORDER BY window_start DESC
        LIMIT $1
        """,
        limit,
    )
    return [Anomaly(**dict(row)) for row in rows]


@app.get("/api/stream")
async def stream(request: Request) -> StreamingResponse:
    return StreamingResponse(
        event_stream(get_pool(), request),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
