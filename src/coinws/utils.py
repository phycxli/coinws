from __future__ import annotations

import random
import time
from collections import deque
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

from .types import ExchangeName, MarketType


def now_us() -> int:
    return int(time.time() * 1_000_000)


def as_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def pick_first(data: dict[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        value = data.get(key)
        if value is not None:
            return value
    return None


def parse_epoch_to_us(value: Any) -> int | None:
    if value is None:
        return None
    try:
        numeric = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if numeric >= Decimal("1000000000000"):
        return int(numeric * 1000)
    return int(numeric * 1_000_000)


def normalize_symbol(exchange: ExchangeName, market_type: MarketType, symbol: str) -> str:
    symbol = symbol.strip().upper()
    if not symbol:
        raise ValueError("symbol 不能为空")

    if exchange == "binance":
        return symbol.replace("/", "").replace("-", "").replace("_", "")

    if exchange == "okx":
        return symbol.replace("/", "-").replace("_", "-")

    if exchange == "gate":
        if market_type == "spot":
            return symbol.replace("/", "_").replace("-", "_")
        return symbol.replace("/", "_")

    raise ValueError(f"不支持的交易所: {exchange}")


def ensure_symbols(symbols: Iterable[str]) -> list[str]:
    items = [item for item in symbols if item]
    if not items:
        raise ValueError("symbols 不能为空")
    return items


def okx_index_inst_id(inst_id: str) -> str:
    parts = inst_id.split("-")
    if len(parts) >= 3:
        return "-".join(parts[:2])
    return inst_id


def gate_normalize_futures_side_and_amount(size: Any, side: Any) -> tuple[str | None, str]:
    side_value = str(side).lower() if side is not None else None
    if side_value in {"buy", "sell"}:
        normalized_side = side_value
    else:
        try:
            size_num = Decimal(str(size))
        except (InvalidOperation, ValueError):
            size_num = Decimal(0)
        normalized_side = "buy" if size_num > 0 else "sell"

    try:
        abs_amount = str(abs(Decimal(str(size))))
    except (InvalidOperation, ValueError):
        abs_amount = "0"

    return normalized_side, abs_amount


def jittered_sleep_seconds(base: float, ratio: float = 0.2) -> float:
    if base <= 0:
        return 0.0
    delta = base * ratio
    return max(0.0, base + random.uniform(-delta, delta))


class SlidingWindowRateLimiter:
    def __init__(self, max_requests: int, window_seconds: float) -> None:
        if max_requests <= 0:
            raise ValueError("max_requests 必须 > 0")
        if window_seconds <= 0:
            raise ValueError("window_seconds 必须 > 0")
        self._max_requests = max_requests
        self._window_seconds = window_seconds
        self._timestamps: deque[float] = deque()

    async def wait(self) -> None:
        import asyncio

        now = time.monotonic()
        while self._timestamps and now - self._timestamps[0] > self._window_seconds:
            self._timestamps.popleft()

        if len(self._timestamps) >= self._max_requests:
            sleep_for = self._window_seconds - (now - self._timestamps[0]) + 0.01
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)
            now = time.monotonic()
            while self._timestamps and now - self._timestamps[0] > self._window_seconds:
                self._timestamps.popleft()

        self._timestamps.append(time.monotonic())
