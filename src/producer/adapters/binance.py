"""Binance public aggTrade adapter (no API key required).

Docs: https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md

Note: Binance is geo-restricted in some regions (notably the US). Use the
``coinbase`` or ``synthetic`` source if the stream is unreachable.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator

import websockets

from common.schemas import Side, Trade
from producer.adapters.base import ExchangeAdapter, normalize_symbol, now_ms

WS_BASE = "wss://stream.binance.com:9443"


class BinanceAdapter(ExchangeAdapter):
    name = "binance"

    def _stream_name(self, symbol: str) -> str:
        return f"{symbol.replace('-', '').lower()}@aggTrade"

    async def _stream_once(self) -> AsyncIterator[Trade]:
        streams = "/".join(self._stream_name(symbol) for symbol in self.symbols)
        url = f"{WS_BASE}/stream?streams={streams}"
        async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
            async for raw in ws:
                payload = json.loads(raw)
                data = payload.get("data", payload)
                if data.get("e") == "aggTrade":
                    yield self._parse(data)

    def _parse(self, data: dict) -> Trade:
        # "m" is true when the buyer is the market maker, i.e. the taker sold.
        taker_side = Side.SELL if data.get("m") else Side.BUY
        return Trade(
            trade_id=f"{self.name}:{data['a']}",
            exchange=self.name,
            symbol=normalize_symbol(data["s"]),
            price=float(data["p"]),
            quantity=float(data["q"]),
            side=taker_side,
            trade_ts=int(data["T"]),
            ingest_ts=now_ms(),
        )
