"""对外统一客户端（底层流式 API）。"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Iterable

from .exchanges import BinanceAdapter, GateAdapter, OkxAdapter
from .types import (
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


class CoinWS:
    """统一的 asyncio 行情订阅客户端。

    说明：
    - 仅支持公共行情频道。
    - 默认自动重连，并在重连后自动重新订阅。
    - 输出为统一字段事件对象（dataclass）。
    """

    def __init__(
        self,
        *,
        proxy: str | None = None,
        reconnect_delay: float = 1.0,
        max_reconnect_delay: float = 30.0,
        logger: logging.Logger | None = None,
    ) -> None:
        self._proxy = proxy
        self._reconnect_delay = reconnect_delay
        self._max_reconnect_delay = max_reconnect_delay
        self._logger = logger or logging.getLogger("coinws")

    def set_proxy(self, proxy: str | None) -> None:
        """设置客户端代理地址。"""
        self._proxy = proxy

    def _build_adapter(self, exchange: ExchangeName):
        """根据交易所名选择适配器实现。"""
        common_kwargs = {
            "proxy": self._proxy,
            "reconnect_delay": self._reconnect_delay,
            "max_reconnect_delay": self._max_reconnect_delay,
            "logger": self._logger,
        }
        if exchange == "binance":
            return BinanceAdapter(**common_kwargs)
        if exchange == "okx":
            return OkxAdapter(**common_kwargs)
        if exchange == "gate":
            return GateAdapter(**common_kwargs)
        raise ValueError(f"不支持的交易所: {exchange}")

    async def stream(
        self,
        *,
        exchange: ExchangeName,
        channel: ChannelName,
        symbols: Iterable[str],
        market_type: MarketType | None = None,
        include_raw: bool = False,
    ) -> AsyncIterator[UnifiedEvent]:
        """统一流式入口。"""
        adapter = self._build_adapter(exchange)
        async for event in adapter.stream(
            channel=channel,
            symbols=symbols,
            market_type=market_type,
            include_raw=include_raw,
        ):
            yield event

    async def watch_trades(
        self,
        *,
        exchange: ExchangeName,
        symbols: Iterable[str],
        market_type: MarketType | None = None,
        include_raw: bool = False,
    ) -> AsyncIterator[TradeEvent]:
        """快捷订阅：逐笔成交。"""
        async for event in self.stream(
            exchange=exchange,
            channel="trades",
            symbols=symbols,
            market_type=market_type,
            include_raw=include_raw,
        ):
            yield event

    async def watch_quotes(
        self,
        *,
        exchange: ExchangeName,
        symbols: Iterable[str],
        market_type: MarketType | None = None,
        include_raw: bool = False,
    ) -> AsyncIterator[QuoteEvent]:
        """快捷订阅：最优档行情。"""
        async for event in self.stream(
            exchange=exchange,
            channel="quotes",
            symbols=symbols,
            market_type=market_type,
            include_raw=include_raw,
        ):
            yield event

    async def watch_funding_rate(
        self,
        *,
        exchange: ExchangeName,
        symbols: Iterable[str],
        market_type: MarketType | None = "swap",
        include_raw: bool = False,
    ) -> AsyncIterator[FundingRateEvent]:
        """快捷订阅：资金费率。"""
        async for event in self.stream(
            exchange=exchange,
            channel="funding_rate",
            symbols=symbols,
            market_type=market_type,
            include_raw=include_raw,
        ):
            yield event

    async def watch_open_interest(
        self,
        *,
        exchange: ExchangeName,
        symbols: Iterable[str],
        market_type: MarketType | None = "swap",
        include_raw: bool = False,
    ) -> AsyncIterator[OpenInterestEvent]:
        """快捷订阅：持仓量。"""
        async for event in self.stream(
            exchange=exchange,
            channel="open_interest",
            symbols=symbols,
            market_type=market_type,
            include_raw=include_raw,
        ):
            yield event

    async def watch_mark_price(
        self,
        *,
        exchange: ExchangeName,
        symbols: Iterable[str],
        market_type: MarketType | None = "swap",
        include_raw: bool = False,
    ) -> AsyncIterator[MarkPriceEvent]:
        """快捷订阅：标记价格。"""
        async for event in self.stream(
            exchange=exchange,
            channel="mark_price",
            symbols=symbols,
            market_type=market_type,
            include_raw=include_raw,
        ):
            yield event

    async def watch_index_price(
        self,
        *,
        exchange: ExchangeName,
        symbols: Iterable[str],
        market_type: MarketType | None = "swap",
        include_raw: bool = False,
    ) -> AsyncIterator[IndexPriceEvent]:
        """快捷订阅：指数价格。"""
        async for event in self.stream(
            exchange=exchange,
            channel="index_price",
            symbols=symbols,
            market_type=market_type,
            include_raw=include_raw,
        ):
            yield event
