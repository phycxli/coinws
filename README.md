# coinws

`coinws` 是一个统一的 `asyncio` WebSocket 行情库，面向：

- Binance
- OKX
- Gate.io

首版仅支持**公共行情**，并输出**统一字段**事件对象。

## 功能范围

支持频道：

- `trades`
- `quotes`（最优档）
- `funding_rate`
- `open_interest`
- `mark_price`
- `index_price`

支持 Python：

- `>=3.12`

## 安装

```bash
pip install coinws
```

## 客户使用方式（封装接口）

你可以直接这样用：

```python
import asyncio
from coinws import coinws


async def main():
    exchange = coinws(exchange="binance")

    await exchange.ws.trades(
        exchange_type="spot",
        symbol="BTCUSDT",
        save_path="data/binance/spot",
    )


asyncio.run(main())
```

效果：

- 持续订阅币安现货 `BTCUSDT` 逐笔成交
- 不断流式接收
- 以 CSV 追加模式保存到 `save_path`
- 文件名形如：`data/binance/spot/trades_BTCUSDT.csv`

### `ws` 方法列表

- `exchange.ws.trades(...)`
- `exchange.ws.quotes(...)`
- `exchange.ws.funding_rate(...)`
- `exchange.ws.open_interest(...)`
- `exchange.ws.mark_price(...)`
- `exchange.ws.index_price(...)`

通用参数：

- `exchange_type`: `spot` / `swap` / `futures`
- `symbol` 或 `symbols`
- `save_path`: 保存目录，传入后自动追加 CSV
- `include_raw`: 是否保留原始 payload
- `limit`: 可选，处理 N 条后返回（便于测试）

## 统一事件结构

所有事件都有以下公共字段：

- `exchange`
- `market_type`
- `channel`
- `symbol`
- `timestamp`（交易所时间，微秒）
- `local_timestamp`（本地接收时间，微秒）
- `raw`（`include_raw=True` 时携带原始 payload）

不同频道额外字段：

- `TradeEvent`: `trade_id`, `side`, `price`, `amount`
- `QuoteEvent`: `ask_price`, `ask_amount`, `bid_price`, `bid_amount`
- `FundingRateEvent`: `funding_rate`, `funding_time`, `next_funding_time`
- `OpenInterestEvent`: `open_interest`
- `MarkPriceEvent`: `mark_price`
- `IndexPriceEvent`: `index_price`

## 符号格式建议

建议传交易所原生格式：

- Binance: `BTCUSDT`
- OKX: `BTC-USDT` / `BTC-USDT-SWAP`
- Gate: `BTC_USDT`

## 设计说明

- 仅 `asyncio`
- 自动断线重连并自动重订阅
- 支持 `proxy`
- 不含私有频道（账户/订单）

## 许可证

MIT
