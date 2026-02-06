from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, TypeAlias

ExchangeName: TypeAlias = Literal["binance", "okx", "gate"]
MarketType: TypeAlias = Literal["spot", "swap", "futures"]
ChannelName: TypeAlias = Literal[
    "trades",
    "quotes",
    "funding_rate",
    "open_interest",
    "mark_price",
    "index_price",
]

TradeSide: TypeAlias = Literal["buy", "sell"]


@dataclass(slots=True, frozen=True)
class BaseEvent:
    exchange: ExchangeName
    market_type: MarketType
    channel: ChannelName
    symbol: str
    timestamp: int
    local_timestamp: int
    raw: Any | None = None


@dataclass(slots=True, frozen=True)
class TradeEvent(BaseEvent):
    trade_id: str | None = None
    side: TradeSide | None = None
    price: str | None = None
    amount: str | None = None


@dataclass(slots=True, frozen=True)
class QuoteEvent(BaseEvent):
    ask_price: str | None = None
    ask_amount: str | None = None
    bid_price: str | None = None
    bid_amount: str | None = None


@dataclass(slots=True, frozen=True)
class FundingRateEvent(BaseEvent):
    funding_rate: str | None = None
    funding_time: int | None = None
    next_funding_time: int | None = None


@dataclass(slots=True, frozen=True)
class OpenInterestEvent(BaseEvent):
    open_interest: str | None = None


@dataclass(slots=True, frozen=True)
class MarkPriceEvent(BaseEvent):
    mark_price: str | None = None


@dataclass(slots=True, frozen=True)
class IndexPriceEvent(BaseEvent):
    index_price: str | None = None


UnifiedEvent: TypeAlias = (
    TradeEvent
    | QuoteEvent
    | FundingRateEvent
    | OpenInterestEvent
    | MarkPriceEvent
    | IndexPriceEvent
)

SUPPORTED_CHANNELS: tuple[ChannelName, ...] = (
    "trades",
    "quotes",
    "funding_rate",
    "open_interest",
    "mark_price",
    "index_price",
)
