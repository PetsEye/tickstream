"""Coinbase Advanced Trade public market_trades adapter (no API key required).

Docs: https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/websocket/market-trades

Note: Coinbase reports the *maker* side of each trade. Because market analytics
care about the aggressor, this adapter inverts it to expose the taker side.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator

import websockets

from common.schemas import Side, Trade
from producer.adapters.base import ExchangeAdapter, normalize_symbol, now_ms, parse_rfc3339_ms

WS_URL = "wss://advanced-trade-ws.coinbase.com"
TRADE_CHANNEL = "market_trades"
HEARTBEAT_CHANNEL = "heartbeats"


class CoinbaseAdapter(ExchangeAdapter):
    name = "coinbase"

    async def _stream_once(self) -> AsyncIterator[Trade]:
        async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
            await ws.send(json.dumps({"type": "subscribe", "channel": HEARTBEAT_CHANNEL}))
            await ws.send(
                json.dumps(
                    {
                        "type": "subscribe",
                        "channel": TRADE_CHANNEL,
                        "product_ids": self.symbols,
                    }
                )
            )
            async for raw in ws:
                message = json.loads(raw)
                if message.get("channel") != TRADE_CHANNEL:
                    continue
                for event in message.get("events", []):
                    for trade in event.get("trades", []):
                        yield self._parse(trade)

    def _parse(self, trade: dict) -> Trade:
        maker_side = str(trade.get("side", "BUY")).upper()
        taker_side = Side.SELL if maker_side == "BUY" else Side.BUY
        return Trade(
            trade_id=f"{self.name}:{trade['trade_id']}",
            exchange=self.name,
            symbol=normalize_symbol(trade["product_id"]),
            price=float(trade["price"]),
            quantity=float(trade["size"]),
            side=taker_side,
            trade_ts=parse_rfc3339_ms(trade["time"]),
            ingest_ts=now_ms(),
        )
