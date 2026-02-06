"""交易所适配器抽象层。

职责：
1. 统一外部调用入口 `stream(...)`。
2. 统一做参数校验、symbol 标准化。
3. 统一做自动重连与退避。
4. 具体交易所只需实现：
   - `resolve_market_type(...)`
   - `_stream_once(...)`
"""

from __future__ import annotations

import abc
import asyncio
import json
import logging
from collections.abc import AsyncIterator, Iterable

import websockets

from ..types import ChannelName, ExchangeName, MarketType, SUPPORTED_CHANNELS, UnifiedEvent
from ..utils import ensure_symbols, jittered_sleep_seconds, normalize_symbol


class ExchangeAdapter(abc.ABC):
    """所有交易所适配器的基类。"""

    exchange: ExchangeName

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
        self._logger = logger or logging.getLogger(f"coinws.{self.exchange}")

    async def stream(
        self,
        *,
        channel: ChannelName,
        symbols: Iterable[str],
        market_type: MarketType | None = None,
        include_raw: bool = False,
    ) -> AsyncIterator[UnifiedEvent]:
        """统一流式入口。

        关键逻辑：
        - 频道校验
        - symbol 标准化
        - 连接异常时自动重连（指数退避 + 抖动）
        """
        if channel not in SUPPORTED_CHANNELS:
            raise ValueError(f"不支持的频道: {channel}")

        resolved_market = self.resolve_market_type(channel=channel, market_type=market_type)
        normalized_symbols = ensure_symbols(
            normalize_symbol(self.exchange, resolved_market, symbol) for symbol in symbols
        )

        backoff = self._reconnect_delay
        while True:
            try:
                got_event = False
                async for event in self._stream_once(
                    channel=channel,
                    symbols=normalized_symbols,
                    market_type=resolved_market,
                    include_raw=include_raw,
                ):
                    got_event = True
                    # 一旦收到正常事件，就重置退避时间。
                    backoff = self._reconnect_delay
                    yield event

                # 连接正常结束但没有事件，也按可恢复场景处理。
                if not got_event:
                    self._logger.warning(
                        "%s %s 流无数据返回，%.1fs 后重连",
                        self.exchange,
                        channel,
                        backoff,
                    )
            except asyncio.CancelledError:
                # 任务被上层取消时直接抛出，避免吞掉取消信号。
                raise
            except Exception as exc:
                self._logger.warning(
                    "%s %s 连接异常: %s，%.1fs 后重连",
                    self.exchange,
                    channel,
                    exc,
                    backoff,
                )

            sleep_for = jittered_sleep_seconds(backoff)
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)
            backoff = min(max(backoff, 0.1) * 2, self._max_reconnect_delay)

    @abc.abstractmethod
    def resolve_market_type(
        self,
        *,
        channel: ChannelName,
        market_type: MarketType | None,
    ) -> MarketType:
        """将调用方 market_type 解析为当前交易所可接受的值。"""
        raise NotImplementedError

    @abc.abstractmethod
    async def _stream_once(
        self,
        *,
        channel: ChannelName,
        symbols: list[str],
        market_type: MarketType,
        include_raw: bool,
    ) -> AsyncIterator[UnifiedEvent]:
        """建立一次 WS 连接并输出事件。

        约定：
        - 方法内部只处理“单次连接生命周期”。
        - 断线重连由基类 `stream(...)` 统一处理。
        """
        raise NotImplementedError

    def _connect(
        self,
        ws_url: str,
        *,
        ping_interval: float | None = 20,
        ping_timeout: float | None = 20,
        max_size: int = 2**24,
        additional_headers: dict[str, str] | None = None,
    ):
        """统一 WS 连接参数封装。"""
        return websockets.connect(
            ws_url,
            ping_interval=ping_interval,
            ping_timeout=ping_timeout,
            max_size=max_size,
            proxy=self._proxy,
            additional_headers=additional_headers,
        )

    @staticmethod
    def _json_loads(payload: str) -> dict:
        """JSON 解析封装，便于子类复用与测试替换。"""
        return json.loads(payload)
