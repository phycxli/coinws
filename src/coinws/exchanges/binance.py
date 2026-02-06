"""Binance 公共行情适配器。

支持频道：
- trades
- quotes (bookTicker)
- funding_rate
- open_interest
- mark_price
- index_price

说明：
- `funding_rate/open_interest/mark_price/index_price` 仅支持 `swap`。
- 解析后统一输出 coinws 的标准事件对象。
"""

from __future__ import annotations

import json
import time
from collections.abc import AsyncIterator

from .base import ExchangeAdapter
from ..types import (
    ChannelName,
    FundingRateEvent,
    IndexPriceEvent,
    MarkPriceEvent,
    MarketType,
    OpenInterestEvent,
    QuoteEvent,
    TradeEvent,
    UnifiedEvent,
)
from ..utils import as_str, now_us

# 不同市场类型对应不同 WS 域名。
BINANCE_WS_URLS: dict[MarketType, str] = {
    "spot": "wss://stream.binance.com/ws",
    "swap": "wss://fstream.binance.com/ws",
    "futures": "wss://dstream.binance.com/ws",
}

# Binance 单次 SUBSCRIBE 请求建议不要过大。
BINANCE_BATCH_SIZE = 100


class BinanceAdapter(ExchangeAdapter):
    """Binance 适配器实现。"""

    exchange = "binance"

    def resolve_market_type(
        self,
        *,
        channel: ChannelName,
        market_type: MarketType | None,
    ) -> MarketType:
        """按频道约束解析 market_type。"""
        # 衍生品指标类频道只在永续市场可用。
        if channel in {"funding_rate", "open_interest", "mark_price", "index_price"}:
            resolved = market_type or "swap"
            if resolved != "swap":
                raise ValueError(f"Binance 频道 {channel} 仅支持 market_type='swap'")
            return resolved

        resolved = market_type or "spot"
        if resolved not in {"spot", "swap", "futures"}:
            raise ValueError(f"Binance 不支持的 market_type: {resolved}")
        return resolved

    async def _stream_once(
        self,
        *,
        channel: ChannelName,
        symbols: list[str],
        market_type: MarketType,
        include_raw: bool,
    ) -> AsyncIterator[UnifiedEvent]:
        """建立一次 WS 连接并持续输出事件。"""
        ws_url = BINANCE_WS_URLS[market_type]

        async with self._connect(
            ws_url,
            ping_interval=20,
            ping_timeout=20,
            max_size=2**24,
        ) as ws:
            await self._subscribe(ws=ws, channel=channel, symbols=symbols)
            async for message in ws:
                for event in self._parse_payload(
                    channel=channel,
                    market_type=market_type,
                    payload=message,
                    include_raw=include_raw,
                ):
                    yield event

    async def _subscribe(self, ws, *, channel: ChannelName, symbols: list[str]) -> None:
        """发送订阅请求（按批次）。"""
        stream_suffix = self._stream_suffix(channel)
        params = [f"{symbol.lower()}@{stream_suffix}" for symbol in symbols]

        # 使用同一 id 便于服务端侧追踪本次订阅批处理。
        sub_id = int(time.time() * 1000) % 1_000_000
        for index in range(0, len(params), BINANCE_BATCH_SIZE):
            batch = params[index : index + BINANCE_BATCH_SIZE]
            payload = {"method": "SUBSCRIBE", "params": batch, "id": sub_id}
            await ws.send(json.dumps(payload))
            # 等待服务端确认，减少一次发太快造成的确认堆积。
            await ws.recv()

    @staticmethod
    def _stream_suffix(channel: ChannelName) -> str:
        """把统一频道名映射为 Binance 流后缀。"""
        if channel == "trades":
            return "trade"
        if channel == "quotes":
            return "bookTicker"
        if channel == "open_interest":
            return "openInterest@1s"
        if channel in {"funding_rate", "mark_price", "index_price"}:
            return "markPrice"
        raise ValueError(f"不支持的频道: {channel}")

    def _parse_payload(
        self,
        *,
        channel: ChannelName,
        market_type: MarketType,
        payload: str,
        include_raw: bool,
    ) -> list[UnifiedEvent]:
        """将 Binance 原始消息解析成统一事件列表。"""
        data = self._json_loads(payload)
        if not isinstance(data, dict):
            return []

        # SUBSCRIBE/UNSUBSCRIBE 的确认包：{"result": null, "id": ...}
        if data.get("result") is None and "id" in data:
            return []

        raw = data if include_raw else None
        event_type = data.get("e")

        # ===== trades =====
        if channel == "trades":
            if event_type != "trade":
                return []
            ts_us = int(data["T"]) * 1000
            symbol = str(data["s"])
            return [
                TradeEvent(
                    exchange="binance",
                    market_type=market_type,
                    channel="trades",
                    symbol=symbol,
                    timestamp=ts_us,
                    local_timestamp=now_us(),
                    trade_id=as_str(data.get("t")),
                    # m=True 表示做市方是买方，主动方为卖，因此这里记作 sell。
                    side="sell" if data.get("m") else "buy",
                    price=as_str(data.get("p")),
                    amount=as_str(data.get("q")),
                    raw=raw,
                )
            ]

        # ===== quotes (bookTicker) =====
        if channel == "quotes":
            # 某些情况下 event 字段可能为空，兼容处理。
            if event_type is not None and event_type != "bookTicker":
                return []
            symbol = as_str(data.get("s"))
            if not symbol:
                return []
            ts_ms = data.get("E") or data.get("T")
            ts_us = int(ts_ms) * 1000 if ts_ms is not None else now_us()
            return [
                QuoteEvent(
                    exchange="binance",
                    market_type=market_type,
                    channel="quotes",
                    symbol=symbol,
                    timestamp=ts_us,
                    local_timestamp=now_us(),
                    ask_price=as_str(data.get("a")),
                    ask_amount=as_str(data.get("A")),
                    bid_price=as_str(data.get("b")),
                    bid_amount=as_str(data.get("B")),
                    raw=raw,
                )
            ]

        # ===== open interest =====
        if event_type == "openInterest" and channel == "open_interest":
            symbol = as_str(data.get("s"))
            ts_ms = data.get("E")
            if not symbol or ts_ms is None:
                return []
            return [
                OpenInterestEvent(
                    exchange="binance",
                    market_type=market_type,
                    channel="open_interest",
                    symbol=symbol,
                    timestamp=int(ts_ms) * 1000,
                    local_timestamp=now_us(),
                    open_interest=as_str(data.get("oi")),
                    raw=raw,
                )
            ]

        # 下方三类都来自 markPriceUpdate。
        if event_type != "markPriceUpdate":
            return []

        symbol = as_str(data.get("s"))
        ts_ms = data.get("E")
        if not symbol or ts_ms is None:
            return []

        ts_us = int(ts_ms) * 1000

        # ===== funding rate =====
        if channel == "funding_rate":
            funding_rate = data.get("r")
            if funding_rate is None:
                return []
            next_funding = data.get("T")
            next_funding_time = int(next_funding) if next_funding is not None else None
            return [
                FundingRateEvent(
                    exchange="binance",
                    market_type=market_type,
                    channel="funding_rate",
                    symbol=symbol,
                    timestamp=ts_us,
                    local_timestamp=now_us(),
                    funding_rate=as_str(funding_rate),
                    funding_time=None,
                    next_funding_time=next_funding_time,
                    raw=raw,
                )
            ]

        # ===== mark price =====
        if channel == "mark_price":
            mark_price = data.get("p")
            if mark_price is None:
                return []
            return [
                MarkPriceEvent(
                    exchange="binance",
                    market_type=market_type,
                    channel="mark_price",
                    symbol=symbol,
                    timestamp=ts_us,
                    local_timestamp=now_us(),
                    mark_price=as_str(mark_price),
                    raw=raw,
                )
            ]

        # ===== index price =====
        if channel == "index_price":
            index_price = data.get("i")
            if index_price is None:
                return []
            return [
                IndexPriceEvent(
                    exchange="binance",
                    market_type=market_type,
                    channel="index_price",
                    symbol=symbol,
                    timestamp=ts_us,
                    local_timestamp=now_us(),
                    index_price=as_str(index_price),
                    raw=raw,
                )
            ]

        return []
