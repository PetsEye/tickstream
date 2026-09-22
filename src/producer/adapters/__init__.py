"""Adapter factory: map configuration to a concrete :class:`ExchangeAdapter`."""

from __future__ import annotations

from common.config import ProducerSettings
from producer.adapters.base import ExchangeAdapter
from producer.adapters.binance import BinanceAdapter
from producer.adapters.coinbase import CoinbaseAdapter
from producer.adapters.kraken import KrakenAdapter
from producer.adapters.synthetic import SyntheticAdapter

__all__ = [
    "BinanceAdapter",
    "CoinbaseAdapter",
    "ExchangeAdapter",
    "KrakenAdapter",
    "SyntheticAdapter",
    "create_adapter",
]


def create_adapter(settings: ProducerSettings) -> ExchangeAdapter:
    source = settings.source
    if source == "coinbase":
        return CoinbaseAdapter(settings.symbols, settings.reconnect_backoff_seconds)
    if source == "binance":
        return BinanceAdapter(settings.symbols, settings.reconnect_backoff_seconds)
    if source == "kraken":
        return KrakenAdapter(settings.symbols, settings.reconnect_backoff_seconds)
    if source == "synthetic":
        synthetic = settings.synthetic
        return SyntheticAdapter(
            symbols=settings.symbols,
            file=synthetic.file,
            speed=synthetic.speed,
            loop=synthetic.loop,
            reconnect_backoff_seconds=settings.reconnect_backoff_seconds,
        )
    raise ValueError(f"Unknown producer source: {source!r}")
