"""统一事件类型定义。

本文件只做一件事：
- 定义库对外暴露的标准事件结构（dataclass）。

设计原则：
1. 所有频道共享一组公共字段（BaseEvent）。
2. 每个频道在公共字段上扩展自身字段。
3. 事件字段类型尽量稳定，便于下游直接做序列化/落盘。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, TypeAlias

# 支持的交易所枚举。
ExchangeName: TypeAlias = Literal["binance", "okx", "gate"]

# 支持的市场类型枚举。
MarketType: TypeAlias = Literal["spot", "swap", "futures"]

# 支持的频道枚举。
ChannelName: TypeAlias = Literal[
    "trades",
    "quotes",
    "funding_rate",
    "open_interest",
    "mark_price",
    "index_price",
]

# 成交方向枚举。
TradeSide: TypeAlias = Literal["buy", "sell"]


@dataclass(slots=True, frozen=True)
class BaseEvent:
    """所有事件共享的基础字段。"""

    exchange: ExchangeName
    market_type: MarketType
    channel: ChannelName
    symbol: str
    # 交易所原始时间戳，统一为微秒。
    timestamp: int
    # 本地接收/处理时间戳，统一为微秒。
    local_timestamp: int
    # 可选保留原始 WS payload。
    raw: Any | None = None


@dataclass(slots=True, frozen=True)
class TradeEvent(BaseEvent):
    """逐笔成交事件。"""

    trade_id: str | None = None
    side: TradeSide | None = None
    price: str | None = None
    amount: str | None = None


@dataclass(slots=True, frozen=True)
class QuoteEvent(BaseEvent):
    """最优档行情事件（best bid/ask）。"""

    ask_price: str | None = None
    ask_amount: str | None = None
    bid_price: str | None = None
    bid_amount: str | None = None


@dataclass(slots=True, frozen=True)
class FundingRateEvent(BaseEvent):
    """资金费率事件。"""

    funding_rate: str | None = None
    funding_time: int | None = None
    next_funding_time: int | None = None


@dataclass(slots=True, frozen=True)
class OpenInterestEvent(BaseEvent):
    """持仓量事件。"""

    open_interest: str | None = None


@dataclass(slots=True, frozen=True)
class MarkPriceEvent(BaseEvent):
    """标记价格事件。"""

    mark_price: str | None = None


@dataclass(slots=True, frozen=True)
class IndexPriceEvent(BaseEvent):
    """指数价格事件。"""

    index_price: str | None = None


# 统一事件联合类型，便于调用方做类型标注。
UnifiedEvent: TypeAlias = (
    TradeEvent
    | QuoteEvent
    | FundingRateEvent
    | OpenInterestEvent
    | MarkPriceEvent
    | IndexPriceEvent
)

# 统一维护频道白名单，用于适配器入口校验。
SUPPORTED_CHANNELS: tuple[ChannelName, ...] = (
    "trades",
    "quotes",
    "funding_rate",
    "open_interest",
    "mark_price",
    "index_price",
)
