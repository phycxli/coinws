"""面向业务侧的“开箱即用”封装层。

这个模块的目标是让使用方式尽量贴近：

    exchange = coinws(exchange="binance")
    await exchange.ws.trades(...)

与底层 `CoinWS.stream(...)` 相比，这里额外做了两件事：
1. 提供 `ws.trades / ws.quotes / ...` 六个直接方法。
2. 可选将实时事件持续追加写入 CSV（`save_path`）。
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
    """把任意值转换为 CSV 可写的字符串。

    这里统一把 `None` 写成空字符串，避免 CSV 中出现字面量 `None`。
    """
    return "" if value is None else str(value)


def _normalize_symbols(
    *,
    symbol: str | None,
    symbols: str | Sequence[str] | Iterable[str] | None,
) -> list[str]:
    """统一处理 `symbol`/`symbols` 参数。

    设计约束：
    - 只能二选一传入（单 symbol 或多 symbols）
    - 最终都转换成 list[str]
    - 空输入会直接抛错，避免建立无意义订阅
    """
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
    """把统一事件持续追加写入 CSV。

    写盘策略：
    - 每个 `(channel, symbol)` 独立一个文件。
    - 首次写入文件时自动写表头。
    - 每次写一行后立即 flush，保证实时可见性。

    文件命名规则：
    - `trades_BTCUSDT.csv`
    - `quotes_ETHUSDT.csv`
    - 其余频道同理。
    """

    def __init__(self, base_dir: str | Path) -> None:
        self._base_dir = Path(base_dir)
        self._base_dir.mkdir(parents=True, exist_ok=True)
        # key: 目标 csv 文件路径
        # value: (文件句柄, csv writer)
        self._handles: dict[Path, tuple] = {}

    def _path_for(self, event: UnifiedEvent) -> Path:
        """根据事件计算落盘路径。"""
        return self._base_dir / f"{event.channel}_{event.symbol}.csv"

    def _header_row(self, event: UnifiedEvent) -> tuple[list[str], list[str]]:
        """根据事件类型生成“表头 + 行数据”。

        所有频道共享公共字段，不同频道追加各自专属字段。
        """
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
        """写入单条事件（追加模式）。"""
        path = self._path_for(event)

        # 首次命中文件时创建句柄并按需写入表头。
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
        # 实时流式场景优先可见性，这里每条都 flush。
        file_handle.flush()

    def close(self) -> None:
        """关闭所有已打开文件句柄。"""
        for file_handle, _ in self._handles.values():
            file_handle.flush()
            file_handle.close()
        self._handles.clear()


class WSFacade:
    """围绕单交易所实例提供六个频道快捷方法。"""

    def __init__(self, *, client: CoinWS, exchange: ExchangeName) -> None:
        self._client = client
        self._exchange = exchange

    async def _stream(
        self,
        *,
        channel: ChannelName,
        exchange_type: MarketType,
        symbols: list[str],
        include_raw: bool,
    ) -> AsyncIterator[UnifiedEvent]:
        """委托底层 `CoinWS.stream` 拉取统一事件流。"""
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
        """消费事件流，并可选写盘。

        返回值：
        - 实际处理的事件条数。
        - 若传入 `limit`，达到上限后返回；否则持续运行。
        """
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
            # 无论正常结束还是异常退出，都确保句柄关闭。
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
        """订阅逐笔成交。

        示例：
            await exchange.ws.trades(
                exchange_type="spot",
                symbol="BTCUSDT",
                save_path="data/binance/spot",
            )
        """
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
        """订阅最优档行情（best bid/ask）。"""
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
        """订阅资金费率（默认 `swap`）。"""
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
        """订阅持仓量（默认 `swap`）。"""
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
        """订阅标记价格（默认 `swap`）。"""
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
        """订阅指数价格（默认 `swap`）。"""
        target_symbols = _normalize_symbols(symbol=symbol, symbols=symbols)
        return await self._consume(
            channel="index_price",
            exchange_type=exchange_type,
            symbols=target_symbols,
            save_path=save_path,
            include_raw=include_raw,
            limit=limit,
        )


class CoinWSEntry:
    """`coinws(...)` 返回的入口对象。"""

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
        # 暴露给业务方的核心对象：exchange.ws.trades(...)
        self.ws = WSFacade(client=self._client, exchange=exchange)


def coinws(
    *,
    exchange: ExchangeName,
    proxy: str | None = None,
    reconnect_delay: float = 1.0,
    max_reconnect_delay: float = 30.0,
) -> CoinWSEntry:
    """创建并返回 `CoinWSEntry`。

    这是建议给业务代码直接使用的工厂函数。
    """
    return CoinWSEntry(
        exchange=exchange,
        proxy=proxy,
        reconnect_delay=reconnect_delay,
        max_reconnect_delay=max_reconnect_delay,
    )
