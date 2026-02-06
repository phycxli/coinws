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
                    backoff = self._reconnect_delay
                    yield event

                if not got_event:
                    self._logger.warning(
                        "%s %s 流无数据返回，%.1fs 后重连",
                        self.exchange,
                        channel,
                        backoff,
                    )
            except asyncio.CancelledError:
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
        return json.loads(payload)
