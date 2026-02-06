"""Binance 逐笔成交连通性测试脚本（真实联网）。

用法示例：
    python tests/test_binance_trades_live.py
    python tests/test_binance_trades_live.py --symbol ETHUSDT --limit 20
"""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from coinws import coinws


def parse_args() -> argparse.Namespace:
    """解析命令行参数。"""
    parser = argparse.ArgumentParser(description="测试 coinws 获取 Binance 现货逐笔 trades")
    parser.add_argument("--symbol", default="BTCUSDT", help="交易对，默认 BTCUSDT")
    parser.add_argument("--limit", type=int, default=10, help="获取条数上限，默认 10")
    parser.add_argument(
        "--save-path",
        default="data/binance/spot",
        help="CSV 保存目录，默认 data/binance/spot",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="超时时间（秒），默认 30",
    )
    return parser.parse_args()


async def run_test(*, symbol: str, limit: int, save_path: str, timeout: int) -> None:
    """执行 Binance trades 流式测试。"""
    exchange = coinws(exchange="binance")

    # 如果需要走 VPN/代理，请在这里给 ws.proxy 赋值。
    # 示例：exchange.ws.proxy = "http://127.0.0.1:7890"
    exchange.ws.proxy = None

    task = exchange.ws.trades(
        exchange_type="spot",
        symbol=symbol,
        save_path=save_path,
        limit=limit,
    )

    try:
        count = await asyncio.wait_for(task, timeout=timeout)
        csv_file = Path(save_path) / f"trades_{symbol}.csv"
        print(f"测试完成：收到 {count} 条 trades，已保存到 {csv_file}")
    except asyncio.TimeoutError:
        print(f"测试超时：{timeout}s 内未达到 limit={limit}，请检查网络或稍后重试")


def main() -> None:
    """脚本主入口。"""
    args = parse_args()
    asyncio.run(
        run_test(
            symbol=args.symbol,
            limit=args.limit,
            save_path=args.save_path,
            timeout=args.timeout,
        )
    )


if __name__ == "__main__":
    main()
