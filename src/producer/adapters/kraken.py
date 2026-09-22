"""Kraken WebSocket v2 public trade adapter (no API key required).

Docs: https://docs.kraken.com/api/docs/websocket-v2/trade
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator

import websockets

from common.schemas import Side, Trade
from producer.adapters.base import ExchangeAdapter, normalize_symbol, now_ms, parse_rfc3339_ms

WS_URL = "wss://ws.kraken.com/v2"


class KrakenAdapter(ExchangeAdapter):
    name = "kraken"

    def _subscription_symbols(self) -> list[str]:
        return [symbol.replace("-", "/") for symbol in self.symbols]

    async def _stream_once(self) -> AsyncIterator[Trade]:
        async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
            await ws.send(
                json.dumps(
                    {
                        "method": "subscribe",
                        "params": {"channel": "trade", "symbol": self._subscription_symbols()},
                    }
                )
            )
            async for raw in ws:
                message = json.loads(raw)
                if message.get("channel") != "trade":
                    continue
                for trade in message.get("data", []):
                    yield self._parse(trade)

    def _parse(self, trade: dict) -> Trade:
        side = Side.BUY if str(trade.get("side", "")).lower() == "buy" else Side.SELL
        return Trade(
            trade_id=f"{self.name}:{trade['trade_id']}",
            exchange=self.name,
            symbol=normalize_symbol(trade["symbol"]),
            price=float(trade["price"]),
            quantity=float(trade["qty"]),
            side=side,
            trade_ts=parse_rfc3339_ms(trade["timestamp"]),
            ingest_ts=now_ms(),
        )
