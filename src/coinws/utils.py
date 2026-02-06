"""公共工具函数。

这个模块聚合了适配器会复用的通用逻辑：
- 时间与类型转换
- symbol 标准化
- 字段兜底提取
- 简单限流器
- 重连抖动计算
"""

from __future__ import annotations

import random
import time
from collections import deque
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

from .types import ExchangeName, MarketType


def now_us() -> int:
    """返回当前 Unix 时间戳（微秒）。"""
    return int(time.time() * 1_000_000)


def as_str(value: Any) -> str | None:
    """把值转换为字符串；`None` 保持为 `None`。"""
    if value is None:
        return None
    return str(value)


def pick_first(data: dict[str, Any], keys: Iterable[str]) -> Any:
    """按顺序获取字典中第一个非空字段值。"""
    for key in keys:
        value = data.get(key)
        if value is not None:
            return value
    return None


def parse_epoch_to_us(value: Any) -> int | None:
    """将秒/毫秒 epoch 转换为微秒。

    规则：
    - 值 >= 1e12 视为毫秒，乘 1000。
    - 否则视为秒，乘 1_000_000。
    - 非法值返回 `None`。
    """
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
    """按交易所规则标准化 symbol。

    说明：
    - Binance 常用 `BTCUSDT`。
    - OKX 常用 `BTC-USDT` / `BTC-USDT-SWAP`。
    - Gate 现货常用 `BTC_USDT`。
    """
    symbol = symbol.strip().upper()
    if not symbol:
        raise ValueError("symbol 不能为空")

    if exchange == "binance":
        return symbol.replace("/", "").replace("-", "").replace("_", "")

    if exchange == "okx":
        return symbol.replace("/", "-").replace("_", "-")

    if exchange == "gate":
        # Gate 现货常用下划线分隔。
        if market_type == "spot":
            return symbol.replace("/", "_").replace("-", "_")
        return symbol.replace("/", "_")

    raise ValueError(f"不支持的交易所: {exchange}")


def ensure_symbols(symbols: Iterable[str]) -> list[str]:
    """确保 symbol 列表非空，并过滤空字符串。"""
    items = [item for item in symbols if item]
    if not items:
        raise ValueError("symbols 不能为空")
    return items


def okx_index_inst_id(inst_id: str) -> str:
    """将 OKX 合约 instId 映射到指数 instId。

    示例：
    - `BTC-USDT-SWAP` -> `BTC-USDT`
    - `ETH-USDT-SWAP` -> `ETH-USDT`
    """
    parts = inst_id.split("-")
    if len(parts) >= 3:
        return "-".join(parts[:2])
    return inst_id


def gate_normalize_futures_side_and_amount(size: Any, side: Any) -> tuple[str | None, str]:
    """归一化 Gate 合约成交方向与数量。

    Gate 合约有时仅给 `size`（正负表达方向），有时给 `side`。
    这里统一产出：
    - side: buy/sell
    - amount: 正数字符串
    """
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
    """给等待时长增加抖动，避免集群同时重连。"""
    if base <= 0:
        return 0.0
    delta = base * ratio
    return max(0.0, base + random.uniform(-delta, delta))


class SlidingWindowRateLimiter:
    """滑动窗口限流器。

    用于限制“单位时间内请求次数”，例如 OKX 每小时请求上限。
    """

    def __init__(self, max_requests: int, window_seconds: float) -> None:
        if max_requests <= 0:
            raise ValueError("max_requests 必须 > 0")
        if window_seconds <= 0:
            raise ValueError("window_seconds 必须 > 0")
        self._max_requests = max_requests
        self._window_seconds = window_seconds
        self._timestamps: deque[float] = deque()

    async def wait(self) -> None:
        """等待到可安全发送下一次请求。"""
        import asyncio

        now = time.monotonic()
        # 清理窗口外时间戳。
        while self._timestamps and now - self._timestamps[0] > self._window_seconds:
            self._timestamps.popleft()

        # 若已达到窗口上限，等待到最早请求滑出窗口。
        if len(self._timestamps) >= self._max_requests:
            sleep_for = self._window_seconds - (now - self._timestamps[0]) + 0.01
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)
            now = time.monotonic()
            while self._timestamps and now - self._timestamps[0] > self._window_seconds:
                self._timestamps.popleft()

        self._timestamps.append(time.monotonic())
