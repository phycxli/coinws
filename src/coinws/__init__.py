"""coinws 对外公共入口。

对客户暴露封装后的高层调用方式：

    exchange = coinws(exchange="binance")
    await exchange.ws.trades(...)
"""

from __future__ import annotations

from .gateway import ExchangeGateway, MarketWebSocket, coinws
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

__all__ = [
    "coinws",
    "ExchangeGateway",
    "MarketWebSocket",
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
