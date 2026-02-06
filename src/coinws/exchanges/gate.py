from __future__ import annotations

import asyncio
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
from ..utils import (
    as_str,
    gate_normalize_futures_side_and_amount,
    now_us,
    parse_epoch_to_us,
    pick_first,
)

GATE_SPOT_WS_URL = "wss://api.gateio.ws/ws/v4/"
GATE_SWAP_WS_URL = "wss://fx-ws.gateio.ws/v4/ws/usdt"
GATE_SUBSCRIBE_BATCH = 100
GATE_APP_PING_SECONDS = 10


class GateAdapter(ExchangeAdapter):
    exchange = "gate"

    def resolve_market_type(
        self,
        *,
        channel: ChannelName,
        market_type: MarketType | None,
    ) -> MarketType:
        if channel in {"funding_rate", "open_interest", "mark_price", "index_price"}:
            resolved = market_type or "swap"
            if resolved != "swap":
                raise ValueError(f"Gate 频道 {channel} 仅支持 market_type='swap'")
            return resolved

        resolved = market_type or "spot"
        if resolved == "futures":
            resolved = "swap"
        if resolved not in {"spot", "swap"}:
            raise ValueError(f"Gate 不支持的 market_type: {resolved}")
        return resolved

    async def _stream_once(
        self,
        *,
        channel: ChannelName,
        symbols: list[str],
        market_type: MarketType,
        include_raw: bool,
    ) -> AsyncIterator[UnifiedEvent]:
        ws_url = GATE_SPOT_WS_URL if market_type == "spot" else GATE_SWAP_WS_URL
        gate_channel = self._gate_channel(channel=channel, market_type=market_type)
        headers = {"X-Gate-Size-Decimal": "1"} if market_type == "swap" else None

        async with self._connect(
            ws_url,
            ping_interval=20,
            ping_timeout=20,
            max_size=2**24,
            additional_headers=headers,
        ) as ws:
            await self._subscribe(ws, gate_channel=gate_channel, symbols=symbols)
            ping_stop = asyncio.Event()
            ping_task = asyncio.create_task(
                self._ping_loop(
                    ws=ws,
                    ping_channel="spot.ping" if market_type == "spot" else "futures.ping",
                    stop_event=ping_stop,
                )
            )

            try:
                async for message in ws:
                    for event in self._parse_payload(
                        channel=channel,
                        market_type=market_type,
                        gate_channel=gate_channel,
                        payload=message,
                        include_raw=include_raw,
                    ):
                        yield event
            finally:
                ping_stop.set()
                ping_task.cancel()

    async def _subscribe(self, ws, *, gate_channel: str, symbols: list[str]) -> None:
        for index in range(0, len(symbols), GATE_SUBSCRIBE_BATCH):
            batch = symbols[index : index + GATE_SUBSCRIBE_BATCH]
            payload = {
                "time": int(time.time()),
                "channel": gate_channel,
                "event": "subscribe",
                "payload": batch,
            }
            await ws.send(json.dumps(payload))
            await asyncio.sleep(0.05)

    async def _ping_loop(self, *, ws, ping_channel: str, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            await asyncio.sleep(GATE_APP_PING_SECONDS)
            try:
                payload = {"time": int(time.time()), "channel": ping_channel}
                await ws.send(json.dumps(payload))
            except Exception:
                return

    @staticmethod
    def _gate_channel(*, channel: ChannelName, market_type: MarketType) -> str:
        if channel == "trades":
            return "spot.trades" if market_type == "spot" else "futures.trades"
        if channel == "quotes":
            return "spot.book_ticker" if market_type == "spot" else "futures.book_ticker"
        if channel in {"funding_rate", "open_interest", "mark_price", "index_price"}:
            return "futures.tickers"
        raise ValueError(f"不支持的频道: {channel}")

    def _parse_payload(
        self,
        *,
        channel: ChannelName,
        market_type: MarketType,
        gate_channel: str,
        payload: str,
        include_raw: bool,
    ) -> list[UnifiedEvent]:
        data = self._json_loads(payload)
        if not isinstance(data, dict):
            return []

        if data.get("channel") in {"spot.pong", "futures.pong"}:
            return []

        event = data.get("event")
        if event == "subscribe":
            result = data.get("result")
            if isinstance(result, dict) and result.get("status") not in {None, "success"}:
                raise RuntimeError(f"Gate 订阅失败: {data.get('error')}")
            return []

        if event != "update" or data.get("channel") != gate_channel:
            return []

        result = data.get("result")
        if result is None:
            return []

        rows = result if isinstance(result, list) else [result]
        raw = data if include_raw else None
        out: list[UnifiedEvent] = []

        for item in rows:
            if not isinstance(item, dict):
                continue

            if channel == "trades":
                out.extend(
                    self._parse_trade(
                        item=item,
                        market_type=market_type,
                        include_raw=include_raw,
                        raw=raw,
                    )
                )
                continue

            if channel == "quotes":
                out.extend(
                    self._parse_quote(
                        item=item,
                        market_type=market_type,
                        include_raw=include_raw,
                        raw=raw,
                    )
                )
                continue

            symbol = as_str(item.get("contract"))
            ts_us = parse_epoch_to_us(item.get("t") or item.get("time"))
            if not symbol or ts_us is None:
                continue

            local_ts = now_us()
            if channel == "funding_rate":
                funding_rate = as_str(item.get("funding_rate"))
                if funding_rate is None:
                    continue
                out.append(
                    FundingRateEvent(
                        exchange="gate",
                        market_type=market_type,
                        channel="funding_rate",
                        symbol=symbol,
                        timestamp=ts_us,
                        local_timestamp=local_ts,
                        funding_rate=funding_rate,
                        funding_time=None,
                        next_funding_time=None,
                        raw=raw,
                    )
                )
                continue

            if channel == "open_interest":
                open_interest = as_str(item.get("total_size"))
                if open_interest is None:
                    continue
                out.append(
                    OpenInterestEvent(
                        exchange="gate",
                        market_type=market_type,
                        channel="open_interest",
                        symbol=symbol,
                        timestamp=ts_us,
                        local_timestamp=local_ts,
                        open_interest=open_interest,
                        raw=raw,
                    )
                )
                continue

            if channel == "mark_price":
                mark_price = as_str(item.get("mark_price"))
                if mark_price is None:
                    continue
                out.append(
                    MarkPriceEvent(
                        exchange="gate",
                        market_type=market_type,
                        channel="mark_price",
                        symbol=symbol,
                        timestamp=ts_us,
                        local_timestamp=local_ts,
                        mark_price=mark_price,
                        raw=raw,
                    )
                )
                continue

            if channel == "index_price":
                index_price = as_str(item.get("index_price"))
                if index_price is None:
                    continue
                out.append(
                    IndexPriceEvent(
                        exchange="gate",
                        market_type=market_type,
                        channel="index_price",
                        symbol=symbol,
                        timestamp=ts_us,
                        local_timestamp=local_ts,
                        index_price=index_price,
                        raw=raw,
                    )
                )

        return out

    def _parse_trade(
        self,
        *,
        item: dict,
        market_type: MarketType,
        include_raw: bool,
        raw,
    ) -> list[TradeEvent]:
        if market_type == "spot":
            symbol = as_str(item.get("currency_pair") or item.get("s"))
            ts_us = parse_epoch_to_us(
                item.get("create_time_ms")
                or item.get("create_time")
                or item.get("t")
                or item.get("time")
            )
            side = as_str(item.get("side"))
            amount = as_str(item.get("amount"))
            price = as_str(item.get("price"))
            trade_id = as_str(item.get("id"))
        else:
            symbol = as_str(item.get("contract") or item.get("s"))
            ts_us = parse_epoch_to_us(
                item.get("create_time_ms")
                or item.get("create_time")
                or item.get("t")
                or item.get("time")
            )
            side, amount = gate_normalize_futures_side_and_amount(
                item.get("size"), item.get("side")
            )
            price = as_str(item.get("price"))
            trade_id = as_str(item.get("id"))

        if not symbol or ts_us is None:
            return []

        return [
            TradeEvent(
                exchange="gate",
                market_type=market_type,
                channel="trades",
                symbol=symbol,
                timestamp=ts_us,
                local_timestamp=now_us(),
                trade_id=trade_id,
                side=side,
                price=price,
                amount=amount,
                raw=raw if include_raw else None,
            )
        ]

    def _parse_quote(
        self,
        *,
        item: dict,
        market_type: MarketType,
        include_raw: bool,
        raw,
    ) -> list[QuoteEvent]:
        symbol = as_str(pick_first(item, ["s", "currency_pair", "contract"]))
        ts_us = parse_epoch_to_us(
            pick_first(item, ["t", "T", "time", "ts", "timestamp"])
        )
        if not symbol or ts_us is None:
            return []

        return [
            QuoteEvent(
                exchange="gate",
                market_type=market_type,
                channel="quotes",
                symbol=symbol,
                timestamp=ts_us,
                local_timestamp=now_us(),
                ask_price=as_str(pick_first(item, ["a", "ask", "askPrice"])),
                ask_amount=as_str(
                    pick_first(item, ["A", "ask_size", "ask_qty", "askQty", "askSize"])
                ),
                bid_price=as_str(pick_first(item, ["b", "bid", "bidPrice"])),
                bid_amount=as_str(
                    pick_first(item, ["B", "bid_size", "bid_qty", "bidQty", "bidSize"])
                ),
                raw=raw if include_raw else None,
            )
        ]
