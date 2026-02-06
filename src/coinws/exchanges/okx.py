"""OKX 公共行情适配器。

支持频道：
- trades
- quotes (tickers)
- funding_rate
- open_interest
- mark_price
- index_price

说明：
- 衍生品指标类频道（funding/open_interest/mark/index）仅支持 `swap`。
- `index_price` 频道订阅的是指数 ID（如 BTC-USDT），
  但输出会映射回原始请求 symbol（如 BTC-USDT-SWAP）。
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from decimal import Decimal, InvalidOperation

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
from ..utils import SlidingWindowRateLimiter, as_str, now_us, okx_index_inst_id

OKX_WS_URL = "wss://ws.okx.com:8443/ws/v5/public"

# 每次 subscribe/unsubscribe 批次大小。
OKX_SUBSCRIBE_BATCH = 20

# OKX 公共 WS 请求限流（每小时）。
OKX_MAX_REQ_PER_HOUR = 480
OKX_REQ_WINDOW_SEC = 3600

# 空闲读取超时；超时后主动 ping。
OKX_IDLE_PING_SECONDS = 25

# 统一频道名 -> OKX 原生频道名。
OKX_CHANNEL_MAP: dict[ChannelName, str] = {
    "trades": "trades",
    "quotes": "tickers",
    "funding_rate": "funding-rate",
    "open_interest": "open-interest",
    "mark_price": "mark-price",
    "index_price": "index-tickers",
}


def _normalize_size(value) -> str | None:
    """将 size 转成正数字符串。

    逐笔频道中，某些交易所字段可能带符号；统一这里转为绝对值字符串。
    """
    if value is None:
        return None
    try:
        return str(abs(Decimal(str(value))))
    except (InvalidOperation, ValueError):
        return None


class OkxAdapter(ExchangeAdapter):
    """OKX 适配器实现。"""

    exchange = "okx"

    def resolve_market_type(
        self,
        *,
        channel: ChannelName,
        market_type: MarketType | None,
    ) -> MarketType:
        """按频道约束 market_type。"""
        if channel in {"funding_rate", "open_interest", "mark_price", "index_price"}:
            resolved = market_type or "swap"
            if resolved != "swap":
                raise ValueError(f"OKX 频道 {channel} 仅支持 market_type='swap'")
            return resolved

        resolved = market_type or "spot"
        if resolved not in {"spot", "swap", "futures"}:
            raise ValueError(f"OKX 不支持的 market_type: {resolved}")
        return resolved

    async def _stream_once(
        self,
        *,
        channel: ChannelName,
        symbols: list[str],
        market_type: MarketType,
        include_raw: bool,
    ) -> AsyncIterator[UnifiedEvent]:
        """建立一次连接，完成订阅并持续解析消息。"""
        okx_channel = OKX_CHANNEL_MAP[channel]
        # index_price 需要维护“指数ID -> 合约ID列表”映射。
        index_map: dict[str, list[str]] = {}

        if channel == "index_price":
            for symbol in symbols:
                index_id = okx_index_inst_id(symbol)
                index_map.setdefault(index_id, []).append(symbol)
            # 实际订阅对象是指数 ID，而不是每个合约 ID。
            subscribe_ids = sorted(index_map.keys())
        else:
            subscribe_ids = symbols

        limiter = SlidingWindowRateLimiter(
            max_requests=OKX_MAX_REQ_PER_HOUR,
            window_seconds=OKX_REQ_WINDOW_SEC,
        )

        async with self._connect(
            OKX_WS_URL,
            ping_interval=None,
            ping_timeout=20,
            max_size=2**24,
        ) as ws:
            # 分批订阅并做限流。
            for index in range(0, len(subscribe_ids), OKX_SUBSCRIBE_BATCH):
                batch = subscribe_ids[index : index + OKX_SUBSCRIBE_BATCH]
                await limiter.wait()
                payload = {
                    "op": "subscribe",
                    "args": [{"channel": okx_channel, "instId": inst_id} for inst_id in batch],
                }
                await ws.send(json.dumps(payload))
                # 小间隔降低服务端判定风险。
                await asyncio.sleep(0.05)

            while True:
                message = await self._recv_with_ping(ws)
                if message == "pong":
                    continue
                for event in self._parse_payload(
                    channel=channel,
                    market_type=market_type,
                    payload=message,
                    index_map=index_map,
                    include_raw=include_raw,
                ):
                    yield event

    async def _recv_with_ping(self, ws) -> str:
        """带空闲保活的读取。

        读取超时后主动发 `ping`，再等待一次读取，降低空闲断链概率。
        """
        try:
            return await asyncio.wait_for(ws.recv(), timeout=OKX_IDLE_PING_SECONDS)
        except asyncio.TimeoutError:
            await ws.send("ping")
            return await asyncio.wait_for(ws.recv(), timeout=OKX_IDLE_PING_SECONDS)

    def _parse_payload(
        self,
        *,
        channel: ChannelName,
        market_type: MarketType,
        payload: str,
        index_map: dict[str, list[str]],
        include_raw: bool,
    ) -> list[UnifiedEvent]:
        """解析 OKX 原始消息为统一事件列表。"""
        data = self._json_loads(payload)
        if not isinstance(data, dict):
            return []

        event = data.get("event")

        # 订阅确认类消息不产出行情事件。
        if event in {"subscribe", "unsubscribe"}:
            return []

        # 服务端错误直接抛出，让上层重连策略接管。
        if event == "error":
            message = data.get("msg") or "unknown OKX error"
            raise RuntimeError(f"OKX 订阅错误: {message} code={data.get('code')}")

        arg = data.get("arg", {})
        if not isinstance(arg, dict):
            return []

        if arg.get("channel") != OKX_CHANNEL_MAP[channel]:
            return []

        raw = data if include_raw else None
        result: list[UnifiedEvent] = []

        for item in data.get("data", []):
            if not isinstance(item, dict):
                continue

            ts_raw = item.get("ts")
            if ts_raw is None:
                continue
            ts_us = int(ts_raw) * 1000
            local_ts = now_us()

            # ===== trades =====
            if channel == "trades":
                symbol = as_str(item.get("instId") or arg.get("instId"))
                if not symbol:
                    continue
                result.append(
                    TradeEvent(
                        exchange="okx",
                        market_type=market_type,
                        channel="trades",
                        symbol=symbol,
                        timestamp=ts_us,
                        local_timestamp=local_ts,
                        trade_id=as_str(item.get("tradeId")),
                        side=as_str(item.get("side")),
                        price=as_str(item.get("px")),
                        amount=_normalize_size(item.get("sz")),
                        raw=raw,
                    )
                )
                continue

            # ===== quotes =====
            if channel == "quotes":
                symbol = as_str(item.get("instId") or arg.get("instId"))
                if not symbol:
                    continue
                result.append(
                    QuoteEvent(
                        exchange="okx",
                        market_type=market_type,
                        channel="quotes",
                        symbol=symbol,
                        timestamp=ts_us,
                        local_timestamp=local_ts,
                        ask_price=as_str(item.get("askPx")),
                        ask_amount=as_str(item.get("askSz")),
                        bid_price=as_str(item.get("bidPx")),
                        bid_amount=as_str(item.get("bidSz")),
                        raw=raw,
                    )
                )
                continue

            # ===== funding_rate =====
            if channel == "funding_rate":
                symbol = as_str(item.get("instId") or arg.get("instId"))
                if not symbol:
                    continue
                funding_time = item.get("fundingTime")
                next_funding_time = item.get("nextFundingTime")
                result.append(
                    FundingRateEvent(
                        exchange="okx",
                        market_type=market_type,
                        channel="funding_rate",
                        symbol=symbol,
                        timestamp=ts_us,
                        local_timestamp=local_ts,
                        funding_rate=as_str(item.get("fundingRate")),
                        funding_time=int(funding_time) if funding_time is not None else None,
                        next_funding_time=int(next_funding_time) if next_funding_time is not None else None,
                        raw=raw,
                    )
                )
                continue

            # ===== open_interest =====
            if channel == "open_interest":
                symbol = as_str(item.get("instId") or arg.get("instId"))
                if not symbol:
                    continue
                result.append(
                    OpenInterestEvent(
                        exchange="okx",
                        market_type=market_type,
                        channel="open_interest",
                        symbol=symbol,
                        timestamp=ts_us,
                        local_timestamp=local_ts,
                        open_interest=as_str(item.get("oi") or item.get("openInterest")),
                        raw=raw,
                    )
                )
                continue

            # ===== mark_price =====
            if channel == "mark_price":
                symbol = as_str(item.get("instId") or arg.get("instId"))
                if not symbol:
                    continue
                result.append(
                    MarkPriceEvent(
                        exchange="okx",
                        market_type=market_type,
                        channel="mark_price",
                        symbol=symbol,
                        timestamp=ts_us,
                        local_timestamp=local_ts,
                        mark_price=as_str(item.get("markPx")),
                        raw=raw,
                    )
                )
                continue

            # ===== index_price =====
            if channel == "index_price":
                index_id = as_str(arg.get("instId"))
                if not index_id:
                    continue
                index_price = as_str(item.get("idxPx"))
                # 将指数价同步映射到该指数下的全部合约 symbol。
                for symbol in index_map.get(index_id, []):
                    result.append(
                        IndexPriceEvent(
                            exchange="okx",
                            market_type=market_type,
                            channel="index_price",
                            symbol=symbol,
                            timestamp=ts_us,
                            local_timestamp=local_ts,
                            index_price=index_price,
                            raw=raw,
                        )
                    )

        return result
