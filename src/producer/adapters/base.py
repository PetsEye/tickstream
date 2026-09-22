"""Exchange adapter abstractions and symbol normalization."""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from datetime import datetime

from common.logging import get_logger
from common.schemas import Trade

logger = get_logger(__name__)

# Quote assets to probe when normalizing concatenated symbols (Binance style).
_QUOTE_ASSETS = ("USDT", "USDC", "USD", "EUR", "GBP", "BTC", "ETH")


def now_ms() -> int:
    return int(time.time() * 1000)


def parse_rfc3339_ms(value: str) -> int:
    """Parse an RFC 3339 / ISO-8601 timestamp into epoch milliseconds."""
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return int(parsed.timestamp() * 1000)


def normalize_symbol(symbol: str) -> str:
    """Normalize any venue symbol to the ``BASE-QUOTE`` uppercase form.

    ``BTCUSDT`` -> ``BTC-USDT``, ``btc/usd`` -> ``BTC-USD``, ``BTC-USD`` unchanged.
    """
    normalized = symbol.upper().replace("/", "-")
    if "-" in normalized:
        return normalized
    for quote in _QUOTE_ASSETS:
        if normalized.endswith(quote) and len(normalized) > len(quote):
            return f"{normalized[: -len(quote)]}-{quote}"
    return normalized


class ExchangeAdapter:
    """Base class for live/synthetic trade sources.

    Subclasses implement :meth:`_stream_once`; :meth:`stream` wraps it with an
    infinite reconnect loop so transient websocket drops self-heal.
    """

    name: str = "base"

    def __init__(self, symbols: list[str], reconnect_backoff_seconds: float = 5.0) -> None:
        self.symbols = [normalize_symbol(s) for s in symbols]
        self.reconnect_backoff_seconds = reconnect_backoff_seconds

    def _stream_once(self) -> AsyncIterator[Trade]:
        """Yield trades for a single connection lifetime (async generator).

        Live adapters implement this; the synthetic adapter overrides
        :meth:`stream` directly because it must not auto-reconnect.
        """
        raise NotImplementedError

    async def stream(self) -> AsyncIterator[Trade]:
        while True:
            try:
                async for trade in self._stream_once():
                    yield trade
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - reconnect on any transport error
                logger.warning(
                    "%s adapter disconnected (%s); reconnecting in %.1fs",
                    self.name,
                    exc,
                    self.reconnect_backoff_seconds,
                )
                await asyncio.sleep(self.reconnect_backoff_seconds)
