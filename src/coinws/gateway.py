"""面向业务方的统一网关（对外主入口）。

推荐用法：

    exchange = coinws(exchange="binance")
    await exchange.ws.trades(...)

定位：
- 这是给客户直接使用的高层 API。
- 屏蔽底层适配器与流式细节。
- 支持实时订阅并可选 CSV 持续追加写盘。
"""

from __future__ import annotations

import csv
from collections.abc import AsyncIterator, Iterable, Sequence
from pathlib import Path

from .client import CoinWS
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


def _as_text(value) -> str:
    """把值转为可写入 CSV 的字符串。"""
    return "" if value is None else str(value)


def _normalize_symbols(
    *,
    symbol: str | None,
    symbols: str | Sequence[str] | Iterable[str] | None,
) -> list[str]:
    """统一处理 symbol/symbols 输入。"""
    if symbol and symbols is not None:
        raise ValueError("symbol 和 symbols 不能同时传")

    if symbol is not None:
        items = [symbol]
    elif symbols is None:
        raise ValueError("必须传 symbol 或 symbols")
    elif isinstance(symbols, str):
        items = [symbols]
    else:
        items = [item for item in symbols if item]

    if not items:
        raise ValueError("symbol/symbols 不能为空")
    return items


class CsvEventSink:
    """统一事件 CSV 持久化器（追加模式）。"""

    def __init__(self, base_dir: str | Path) -> None:
        self._base_dir = Path(base_dir)
        self._base_dir.mkdir(parents=True, exist_ok=True)
        self._handles: dict[Path, tuple] = {}

    def _path_for(self, event: UnifiedEvent) -> Path:
        return self._base_dir / f"{event.channel}_{event.symbol}.csv"

    def _header_row(self, event: UnifiedEvent) -> tuple[list[str], list[str]]:
        common_header = [
            "exchange",
            "market_type",
            "channel",
            "symbol",
            "timestamp",
            "local_timestamp",
        ]
        common_row = [
            _as_text(event.exchange),
            _as_text(event.market_type),
            _as_text(event.channel),
            _as_text(event.symbol),
            _as_text(event.timestamp),
            _as_text(event.local_timestamp),
        ]

        if isinstance(event, TradeEvent):
            return (
                common_header + ["trade_id", "side", "price", "amount"],
                common_row
                + [
                    _as_text(event.trade_id),
                    _as_text(event.side),
                    _as_text(event.price),
                    _as_text(event.amount),
                ],
            )

        if isinstance(event, QuoteEvent):
            return (
                common_header + ["ask_price", "ask_amount", "bid_price", "bid_amount"],
                common_row
                + [
                    _as_text(event.ask_price),
                    _as_text(event.ask_amount),
                    _as_text(event.bid_price),
                    _as_text(event.bid_amount),
                ],
            )

        if isinstance(event, FundingRateEvent):
            return (
                common_header + ["funding_rate", "funding_time", "next_funding_time"],
                common_row
                + [
                    _as_text(event.funding_rate),
                    _as_text(event.funding_time),
                    _as_text(event.next_funding_time),
                ],
            )

        if isinstance(event, OpenInterestEvent):
            return (
                common_header + ["open_interest"],
                common_row + [_as_text(event.open_interest)],
            )

        if isinstance(event, MarkPriceEvent):
            return (
                common_header + ["mark_price"],
                common_row + [_as_text(event.mark_price)],
            )

        if isinstance(event, IndexPriceEvent):
            return (
                common_header + ["index_price"],
                common_row + [_as_text(event.index_price)],
            )

        return common_header, common_row

    def write(self, event: UnifiedEvent) -> None:
        path = self._path_for(event)
        if path not in self._handles:
            path.parent.mkdir(parents=True, exist_ok=True)
            file_handle = open(path, "a", encoding="utf-8", newline="")
            writer = csv.writer(file_handle)
            if path.stat().st_size == 0:
                header, _ = self._header_row(event)
                writer.writerow(header)
            self._handles[path] = (file_handle, writer)

        file_handle, writer = self._handles[path]
        _, row = self._header_row(event)
        writer.writerow(row)
        file_handle.flush()

    def close(self) -> None:
        for file_handle, _ in self._handles.values():
            file_handle.flush()
            file_handle.close()
        self._handles.clear()


class MarketWebSocket:
    """单交易所 WS 业务接口（客户调用的核心对象）。"""

    def __init__(
        self,
        *,
        client: CoinWS,
        exchange: ExchangeName,
        default_proxy: str | None = None,
    ) -> None:
        self._client = client
        self._exchange = exchange
        # 当前 ws 对象的代理变量。
        # 默认值来自 coinws(exchange=..., proxy=...) 传入的初始代理。
        # 客户可在运行时通过 self.proxy 或 set_proxy() 动态修改。
        self.proxy: str | None = default_proxy

    def set_proxy(self, proxy: str | None) -> None:
        """设置当前 ws 对象的代理变量。"""
        self.proxy = proxy

    async def _stream(
        self,
        *,
        channel: ChannelName,
        exchange_type: MarketType,
        symbols: list[str],
        include_raw: bool,
    ) -> AsyncIterator[UnifiedEvent]:
        """内部统一流式入口。"""
        # 每次发起订阅前，把 ws 变量中的代理同步到底层客户端。
        self._client.set_proxy(self.proxy)
        async for event in self._client.stream(
            exchange=self._exchange,
            channel=channel,
            symbols=symbols,
            market_type=exchange_type,
            include_raw=include_raw,
        ):
            yield event

    async def _consume(
        self,
        *,
        channel: ChannelName,
        exchange_type: MarketType,
        symbols: list[str],
        save_path: str | Path | None,
        include_raw: bool,
        limit: int | None,
    ) -> int:
        """消费流式数据并按需写入 CSV，返回处理事件条数。"""
        sink = CsvEventSink(save_path) if save_path else None
        count = 0
        try:
            async for event in self._stream(
                channel=channel,
                exchange_type=exchange_type,
                symbols=symbols,
                include_raw=include_raw,
            ):
                if sink:
                    sink.write(event)
                count += 1
                if limit is not None and count >= limit:
                    return count
        finally:
            if sink:
                sink.close()

    async def trades(
        self,
        *,
        exchange_type: MarketType = "spot",
        symbol: str | None = None,
        symbols: str | Sequence[str] | Iterable[str] | None = None,
        save_path: str | Path | None = None,
        include_raw: bool = False,
        limit: int | None = None,
    ) -> int:
        """订阅逐笔成交。"""
        target_symbols = _normalize_symbols(symbol=symbol, symbols=symbols)
        return await self._consume(
            channel="trades",
            exchange_type=exchange_type,
            symbols=target_symbols,
            save_path=save_path,
            include_raw=include_raw,
            limit=limit,
        )

    async def quotes(
        self,
        *,
        exchange_type: MarketType = "spot",
        symbol: str | None = None,
        symbols: str | Sequence[str] | Iterable[str] | None = None,
        save_path: str | Path | None = None,
        include_raw: bool = False,
        limit: int | None = None,
    ) -> int:
        """订阅最优档行情。"""
        target_symbols = _normalize_symbols(symbol=symbol, symbols=symbols)
        return await self._consume(
            channel="quotes",
            exchange_type=exchange_type,
            symbols=target_symbols,
            save_path=save_path,
            include_raw=include_raw,
            limit=limit,
        )

    async def funding_rate(
        self,
        *,
        exchange_type: MarketType = "swap",
        symbol: str | None = None,
        symbols: str | Sequence[str] | Iterable[str] | None = None,
        save_path: str | Path | None = None,
        include_raw: bool = False,
        limit: int | None = None,
    ) -> int:
        """订阅资金费率。"""
        target_symbols = _normalize_symbols(symbol=symbol, symbols=symbols)
        return await self._consume(
            channel="funding_rate",
            exchange_type=exchange_type,
            symbols=target_symbols,
            save_path=save_path,
            include_raw=include_raw,
            limit=limit,
        )

    async def open_interest(
        self,
        *,
        exchange_type: MarketType = "swap",
        symbol: str | None = None,
        symbols: str | Sequence[str] | Iterable[str] | None = None,
        save_path: str | Path | None = None,
        include_raw: bool = False,
        limit: int | None = None,
    ) -> int:
        """订阅持仓量。"""
        target_symbols = _normalize_symbols(symbol=symbol, symbols=symbols)
        return await self._consume(
            channel="open_interest",
            exchange_type=exchange_type,
            symbols=target_symbols,
            save_path=save_path,
            include_raw=include_raw,
            limit=limit,
        )

    async def mark_price(
        self,
        *,
        exchange_type: MarketType = "swap",
        symbol: str | None = None,
        symbols: str | Sequence[str] | Iterable[str] | None = None,
        save_path: str | Path | None = None,
        include_raw: bool = False,
        limit: int | None = None,
    ) -> int:
        """订阅标记价格。"""
        target_symbols = _normalize_symbols(symbol=symbol, symbols=symbols)
        return await self._consume(
            channel="mark_price",
            exchange_type=exchange_type,
            symbols=target_symbols,
            save_path=save_path,
            include_raw=include_raw,
            limit=limit,
        )

    async def index_price(
        self,
        *,
        exchange_type: MarketType = "swap",
        symbol: str | None = None,
        symbols: str | Sequence[str] | Iterable[str] | None = None,
        save_path: str | Path | None = None,
        include_raw: bool = False,
        limit: int | None = None,
    ) -> int:
        """订阅指数价格。"""
        target_symbols = _normalize_symbols(symbol=symbol, symbols=symbols)
        return await self._consume(
            channel="index_price",
            exchange_type=exchange_type,
            symbols=target_symbols,
            save_path=save_path,
            include_raw=include_raw,
            limit=limit,
        )


class ExchangeGateway:
    """单交易所对外网关对象。"""

    def __init__(
        self,
        *,
        exchange: ExchangeName,
        proxy: str | None = None,
        reconnect_delay: float = 1.0,
        max_reconnect_delay: float = 30.0,
    ) -> None:
        self.exchange = exchange
        self._client = CoinWS(
            proxy=proxy,
            reconnect_delay=reconnect_delay,
            max_reconnect_delay=max_reconnect_delay,
        )
        self.ws = MarketWebSocket(
            client=self._client,
            exchange=exchange,
            default_proxy=proxy,
        )


def coinws(
    *,
    exchange: ExchangeName,
    proxy: str | None = None,
    reconnect_delay: float = 1.0,
    max_reconnect_delay: float = 30.0,
) -> ExchangeGateway:
    """创建交易所网关（对外工厂函数）。"""
    return ExchangeGateway(
        exchange=exchange,
        proxy=proxy,
        reconnect_delay=reconnect_delay,
        max_reconnect_delay=max_reconnect_delay,
    )
