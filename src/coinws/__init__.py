"""coinws 顶层导出。

对外提供两套入口：
1. 高层易用入口：`coinws(exchange=...).ws.trades(...)`
2. 低层流式入口：`CoinWS.stream(...)`
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterable

from .client import CoinWS
from .easy import CoinWSEntry, coinws
from .types import (
    BaseEvent,
    ChannelName,
    ExchangeName,
    FundingRateEvent,
    IndexPriceEvent,
    MarkPriceEvent,
    MarketType,
    OpenInterestEvent,
    QuoteEvent,
    TradeEvent,
    UnifiedEvent,
)

__version__ = "0.1.0"


async def stream(
    *,
    exchange: ExchangeName,
    channel: ChannelName,
    symbols: Iterable[str],
    market_type: MarketType | None = None,
    include_raw: bool = False,
    proxy: str | None = None,
) -> AsyncIterator[UnifiedEvent]:
    """顶层便捷流式入口。

    这是 `CoinWS.stream(...)` 的轻量包装，适合快速脚本使用。
    """
    client = CoinWS(proxy=proxy)
    async for event in client.stream(
        exchange=exchange,
        channel=channel,
        symbols=symbols,
        market_type=market_type,
        include_raw=include_raw,
    ):
        yield event


__all__ = [
    "CoinWS",
    "CoinWSEntry",
    "coinws",
    "stream",
    "BaseEvent",
    "UnifiedEvent",
    "TradeEvent",
    "QuoteEvent",
    "FundingRateEvent",
    "OpenInterestEvent",
    "MarkPriceEvent",
    "IndexPriceEvent",
    "ExchangeName",
    "MarketType",
    "ChannelName",
]
