"""asyncpg connection pool lifecycle."""

from __future__ import annotations

import asyncpg

from common.config import TimescaleSettings
from common.logging import get_logger

logger = get_logger(__name__)

_pool: asyncpg.Pool | None = None


async def init_pool(cfg: TimescaleSettings) -> asyncpg.Pool:
    global _pool
    _pool = await asyncpg.create_pool(
        host=cfg.host,
        port=cfg.port,
        database=cfg.database,
        user=cfg.user,
        password=cfg.password,
        min_size=1,
        max_size=10,
    )
    logger.info("connected to TimescaleDB at %s:%s/%s", cfg.host, cfg.port, cfg.database)
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("database pool is not initialised")
    return _pool
