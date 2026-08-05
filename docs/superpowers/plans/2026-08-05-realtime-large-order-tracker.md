# 多檔即時大單追蹤器 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 以單一 Fugle WebSocket 連線同時追蹤多檔台股，即時把每筆成交分為買/賣/集合競價並依價位聚合，另依可調金額門檻篩出大單，資料寫入 Parquet，並提供即時更新的 Web 看盤畫面。

**Architecture:** 一條 Fugle WebSocket 連線（API key 硬限制 1 條）在認證完成後訂閱 N 檔股票 × 2 個頻道（`trades`、`books`）。收到的事件送進記憶體中的 per-symbol 聚合器：逐筆成交依 `bid`/`ask` 判方向後累加到價位階梯，同時保留逐筆明細以便大單門檻變更時即時重算。聚合器狀態經一個執行緒安全的廣播器推送到瀏覽器 WebSocket，並由緩衝寫入器批次落地成 Parquet。Fugle SDK 是同步／執行緒式，FastAPI 是 asyncio，兩者以 `loop.call_soon_threadsafe` 銜接。

**Tech Stack:** Python 3.10、fugle-marketdata 2.4.1、FastAPI + uvicorn、pyarrow（Parquet）、pytest。前端為單一 HTML + 原生 JS，不引入前端框架。

## Global Constraints

- **Python 3.10**（`Pipfile` 既有設定），套件裝在 pipenv 虛擬環境 `tw_stock_realtime-JRdNzDPE`。
- **同一把 API key 只能開一條 WebSocket 連線**。實測超過會被伺服器以 close frame `Maximum number of connections reached` 斷線。所有股票與頻道必須共用單一 `WebSocketClient`；程式執行期間不可另外再跑 `collector.py`。
- **訂閱必須等 `authenticated` 事件之後才送出**。SDK 在 `connect` 事件內送出 auth frame 但不等回覆（`fugle_marketdata/websocket/client.py:41,112-115`），在 `connect` 內訂閱會被伺服器以 `{"message": "Forbidden resource"}` 拒絕。
- **`size` 單位是張（lot），不是股。** 股數 = 張 × 1000。成交金額 = 價格 × 張數 × 1000。
- **方向判定（已用當日 10,055 筆實測驗證，與交易所口徑零差異）**：
  - 無 `bid`/`ask` 欄位 → `auction`（集合競價）
  - `price >= ask` → `buy`（外盤）
  - `price <= bid` → `sell`（內盤）
  - 其餘 → `unknown`（連續交易時段實測 0 筆，僅作防禦性保留）
- **價格可能為小數**（例：2317 為 257.5），價位聚合鍵不可假設為整數。
- **預設排除試撮**（`isTrial=true`），除非明確帶 `--include-trials`。
- **不得關閉 SSL 驗證**；沿用 `certifi` 憑證包（`collector.py:123-124` 既有做法）。
- **不修改現有 `collector.py` 與 `test_collector.py`**。新功能全部放在 `app/` 套件，兩者並存。

## 已驗證的資料形狀（實測，非文件推測）

`trades` 事件（連續交易時段）：
```json
{"symbol":"2330","type":"EQUITY","exchange":"TWSE","market":"TSE","price":2400,
 "size":1,"bid":2400,"ask":2405,"volume":21910,"isContinuous":true,
 "time":1785902195644483,"serial":13665698}
```

`trades` 事件（開盤集合競價，**無 bid/ask**）：
```json
{"price":2385,"size":2024,"volume":2024,"time":1785891605048734,"serial":131115}
```

`books` 事件（五檔快照，**無 serial**）：
```json
{"symbol":"2330","type":"EQUITY","exchange":"TWSE","market":"TSE",
 "bids":[{"price":2390,"size":226},{"price":2385,"size":284},{"price":2380,"size":288},
         {"price":2375,"size":287},{"price":2370,"size":511}],
 "asks":[{"price":2395,"size":343},{"price":2400,"size":481},{"price":2405,"size":450},
         {"price":2410,"size":694},{"price":2415,"size":980}],
 "isContinuous":true,"time":1785907321886311}
```

## 大單門檻的語意（實作前請先讀）

規格定義為「成交單量 × 成交股價 >= 門檻」。本計畫將「成交單量」實作為**張數**，因此：

```
金額(TWD) = price × lots × 1000
```

以門檻 100 萬為例：
- 2330 @ 2400：1 張 = 240 萬 → **每一筆 ≥1 張的成交都是大單**（當日 10,055 筆全數符合）
- 2317 @ 258：1 張 = 25.8 萬 → 需 ≥4 張才符合

這是規格文字的直接翻譯。因為高價股會讓門檻幾乎失效，Web UI 提供即時調整門檻的功能（Task 6），使用者可邊看邊校準到合理值，不需重跑程式。

---

## File Structure

| 檔案 | 責任 |
|------|------|
| `app/__init__.py` | 空套件標記 |
| `app/classify.py` | 純函式：方向判定、金額換算、大單判定。無 IO、無狀態 |
| `app/aggregator.py` | 單一股票的記憶體狀態：價位階梯、逐筆明細、五檔快照、大單重算 |
| `app/storage.py` | 緩衝式 Parquet 寫入器 |
| `app/feed.py` | Fugle WebSocket 接線：認證後訂閱多檔多頻道，分派事件 |
| `app/web.py` | FastAPI：快照 API、瀏覽器 WebSocket 推送、門檻調整端點、執行緒安全廣播器 |
| `app/static/index.html` | 看盤畫面（原生 JS） |
| `app/__main__.py` | CLI 進入點：組裝 feed 執行緒 + uvicorn |
| `app/export.py` | Parquet → CSV 匯出 CLI |
| `tests/test_classify.py` | 方向判定與大單判定 |
| `tests/test_aggregator.py` | 聚合與門檻重算 |
| `tests/test_storage.py` | Parquet 寫入與讀回 |
| `tests/test_feed.py` | 訂閱時序、單一連線多訂閱、事件分派 |
| `tests/test_web.py` | 快照 API、門檻端點、廣播器 |
| `tests/test_export.py` | CSV 匯出 |

---

## Task 1: 方向判定與大單判定（純函式）

**Files:**
- Create: `app/__init__.py`
- Create: `app/classify.py`
- Test: `tests/test_classify.py`

**Interfaces:**
- Consumes: 無（第一個任務）
- Produces:
  - `SHARES_PER_LOT: int = 1000`
  - `BUY: str = "buy"`, `SELL: str = "sell"`, `AUCTION: str = "auction"`, `UNKNOWN: str = "unknown"`
  - `classify_side(trade: dict) -> str`
  - `trade_value_twd(price: float, lots: int) -> float`
  - `is_large_order(trade: dict, threshold_twd: float) -> bool`

- [ ] **Step 1: 建立套件標記檔**

```bash
mkdir -p app tests
touch app/__init__.py
```

- [ ] **Step 2: 寫失敗的測試**

建立 `tests/test_classify.py`：

```python
"""方向判定：以當日 2330 全 10,055 筆實測驗證過的規則。

買(外盤) 15,113 張 / 賣(內盤) 10,042 張，與交易所 total.tradeVolumeAtAsk /
tradeVolumeAtBid 完全相符；五檔中間成交 0 筆；唯一無 bid/ask 的是開盤集合競價。
"""

import pytest

from app import classify


# 真實事件：連續交易時段，成交在賣價 -> 外盤(買方主動)
TRADE_AT_ASK = {
    "symbol": "2330", "price": 2405, "size": 1, "bid": 2400, "ask": 2405,
    "time": 1785902209312276, "serial": 13674373,
}
# 真實事件：成交在買價 -> 內盤(賣方主動)
TRADE_AT_BID = {
    "symbol": "2330", "price": 2400, "size": 1, "bid": 2400, "ask": 2405,
    "time": 1785902195644483, "serial": 13665698,
}
# 真實事件：09:00:05 開盤集合競價，沒有 bid/ask 欄位
TRADE_AUCTION = {
    "price": 2385, "size": 2024, "volume": 2024,
    "time": 1785891605048734, "serial": 131115,
}


def test_trade_at_ask_is_buy():
    assert classify.classify_side(TRADE_AT_ASK) == classify.BUY


def test_trade_at_bid_is_sell():
    assert classify.classify_side(TRADE_AT_BID) == classify.SELL


def test_trade_above_ask_is_buy():
    assert classify.classify_side({**TRADE_AT_ASK, "price": 2410}) == classify.BUY


def test_trade_below_bid_is_sell():
    assert classify.classify_side({**TRADE_AT_BID, "price": 2395}) == classify.SELL


def test_trade_without_quote_is_auction():
    assert classify.classify_side(TRADE_AUCTION) == classify.AUCTION


def test_mid_spread_is_unknown_not_silently_merged():
    """連續交易時段實測 0 筆，但若出現不可混入買/賣或集合競價。"""
    mid = {"price": 2402, "size": 1, "bid": 2400, "ask": 2405}
    assert classify.classify_side(mid) == classify.UNKNOWN


def test_trade_value_uses_lots_times_thousand_shares():
    # 1 張 2330 @ 2400 = 2,400,000 元
    assert classify.trade_value_twd(2400, 1) == 2_400_000


def test_trade_value_handles_fractional_price():
    # 2317 @ 257.5，4 張 = 1,030,000 元
    assert classify.trade_value_twd(257.5, 4) == 1_030_000


@pytest.mark.parametrize("lots,expected", [(3, False), (4, True), (5, True)])
def test_is_large_order_threshold_boundary(lots, expected):
    # 2317 @ 258：1 張 = 258,000 元，門檻 100 萬 -> 需 4 張
    trade = {"price": 258, "size": lots, "bid": 257.5, "ask": 258}
    assert classify.is_large_order(trade, 1_000_000) is expected


def test_auction_trade_can_also_be_large_order():
    # 開盤那筆 2,024 張 × 2385 = 4.83 億，必定符合大單
    assert classify.is_large_order(TRADE_AUCTION, 1_000_000) is True
```

- [ ] **Step 3: 執行測試確認失敗**

```bash
cd /Users/chia-chingcho/Documents/vscode_project/tw_stock_realtime
/Users/chia-chingcho/.local/share/virtualenvs/tw_stock_realtime-JRdNzDPE/bin/python -m pytest tests/test_classify.py -q
```
Expected: FAIL，`ModuleNotFoundError: No module named 'app.classify'`

- [ ] **Step 4: 實作**

建立 `app/classify.py`：

```python
"""成交方向判定與大單判定 —— 純函式，無狀態、無 IO。

方向規則以當日 2330 全 10,055 筆逐筆成交驗證過：買(外盤) 15,113 張、
賣(內盤) 10,042 張，與交易所 total.tradeVolumeAtAsk / tradeVolumeAtBid
零差異。連續交易時段沒有任何一筆落在五檔中間。
"""

from __future__ import annotations

SHARES_PER_LOT = 1000

BUY = "buy"          # 外盤，買方主動
SELL = "sell"        # 內盤，賣方主動
AUCTION = "auction"  # 集合競價（開盤/收盤/瞬間價格穩定措施），無主動方
UNKNOWN = "unknown"  # 五檔中間；實測未發生，保留以免靜默歸錯類


def classify_side(trade: dict) -> str:
    """判定一筆成交的方向。

    集合競價的成交事件不帶 bid/ask —— 那筆撮合本來就沒有最佳一檔可比，
    買賣雙方同時成交，因此獨立成一類，不併入買或賣。
    """
    if "bid" not in trade or "ask" not in trade:
        return AUCTION
    price = trade["price"]
    if price >= trade["ask"]:
        return BUY
    if price <= trade["bid"]:
        return SELL
    return UNKNOWN


def trade_value_twd(price: float, lots: int) -> float:
    """成交金額（台幣）。Fugle 的 size 單位是張，一張 1,000 股。"""
    return price * lots * SHARES_PER_LOT


def is_large_order(trade: dict, threshold_twd: float) -> bool:
    """該筆成交金額是否達到大單門檻。"""
    return trade_value_twd(trade["price"], trade["size"]) >= threshold_twd
```

- [ ] **Step 5: 執行測試確認通過**

```bash
/Users/chia-chingcho/.local/share/virtualenvs/tw_stock_realtime-JRdNzDPE/bin/python -m pytest tests/test_classify.py -q
```
Expected: PASS（11 passed）

- [ ] **Step 6: Commit**

```bash
git add app/__init__.py app/classify.py tests/test_classify.py
git commit -m "feat: add verified trade-side and large-order classification"
```

---

## Task 2: 單股聚合器（價位階梯 + 五檔快照 + 可調大單門檻）

**Files:**
- Create: `app/aggregator.py`
- Test: `tests/test_aggregator.py`

**Interfaces:**
- Consumes: `app.classify.classify_side / trade_value_twd / BUY / SELL / AUCTION / UNKNOWN`
  （**刻意不用 `is_large_order`**：它讀 `trade["size"]`，而 `set_threshold` 手上是
  正規化紀錄、該欄位叫 `lots`，直接套用會 `KeyError`。兩處一律以 `value_twd`
  與門檻比較，保持單一寫法。）
- Produces:
  - `class SymbolAggregator(symbol: str, large_order_twd: float)`
    - `add_trade(trade: dict) -> dict | None` — 回傳正規化紀錄；重複 serial 回傳 `None`
    - `update_book(book: dict) -> None`
    - `set_threshold(threshold_twd: float) -> None` — 重算大單階梯
    - `snapshot() -> dict`
  - 正規化紀錄欄位：`symbol, serial, time, price, lots, shares, side, value_twd, is_large`

- [ ] **Step 1: 寫失敗的測試**

建立 `tests/test_aggregator.py`：

```python
from app.aggregator import SymbolAggregator


AT_ASK = {"symbol": "2330", "price": 2405, "size": 2, "bid": 2400, "ask": 2405,
          "time": 1785902209312276, "serial": 1}
AT_BID = {"symbol": "2330", "price": 2400, "size": 3, "bid": 2400, "ask": 2405,
          "time": 1785902195644483, "serial": 2}
AT_BID_AGAIN = {"symbol": "2330", "price": 2400, "size": 1, "bid": 2400, "ask": 2405,
                "time": 1785902198947291, "serial": 3}
AUCTION = {"price": 2385, "size": 2024, "time": 1785891605048734, "serial": 4}

BOOK = {
    "symbol": "2330",
    "bids": [{"price": 2390, "size": 226}, {"price": 2385, "size": 284}],
    "asks": [{"price": 2395, "size": 343}, {"price": 2400, "size": 481}],
    "time": 1785907321886311,
}


def test_add_trade_returns_normalised_record():
    agg = SymbolAggregator("2330", large_order_twd=1_000_000)
    record = agg.add_trade(AT_ASK)
    assert record["symbol"] == "2330"
    assert record["serial"] == 1
    assert record["lots"] == 2
    assert record["shares"] == 2000
    assert record["side"] == "buy"
    assert record["value_twd"] == 4_810_000
    assert record["is_large"] is True


def test_duplicate_serial_is_ignored():
    agg = SymbolAggregator("2330", large_order_twd=1_000_000)
    assert agg.add_trade(AT_ASK) is not None
    assert agg.add_trade(AT_ASK) is None
    assert agg.snapshot()["ladder"][0]["buy_lots"] == 2   # 沒有被重複累加


def test_ladder_splits_buy_and_sell_per_price():
    agg = SymbolAggregator("2330", large_order_twd=1_000_000)
    for t in (AT_ASK, AT_BID, AT_BID_AGAIN):
        agg.add_trade(t)
    ladder = {row["price"]: row for row in agg.snapshot()["ladder"]}
    assert ladder[2405]["buy_lots"] == 2
    assert ladder[2405]["sell_lots"] == 0
    assert ladder[2400]["sell_lots"] == 4      # 3 + 1
    assert ladder[2400]["buy_lots"] == 0


def test_auction_volume_is_separate_from_buy_and_sell():
    agg = SymbolAggregator("2330", large_order_twd=1_000_000)
    agg.add_trade(AUCTION)
    row = {r["price"]: r for r in agg.snapshot()["ladder"]}[2385]
    assert row["auction_lots"] == 2024
    assert row["buy_lots"] == 0
    assert row["sell_lots"] == 0


def test_ladder_is_sorted_by_price_descending():
    agg = SymbolAggregator("2330", large_order_twd=1_000_000)
    for t in (AT_BID, AT_ASK, AUCTION):
        agg.add_trade(t)
    prices = [row["price"] for row in agg.snapshot()["ladder"]]
    assert prices == sorted(prices, reverse=True)


def test_fractional_prices_are_kept_distinct():
    agg = SymbolAggregator("2317", large_order_twd=1_000_000)
    agg.add_trade({"symbol": "2317", "price": 257.5, "size": 1, "bid": 257.5,
                   "ask": 258, "time": 1, "serial": 1})
    agg.add_trade({"symbol": "2317", "price": 258, "size": 1, "bid": 257.5,
                   "ask": 258, "time": 2, "serial": 2})
    prices = [row["price"] for row in agg.snapshot()["ladder"]]
    assert prices == [258, 257.5]


def test_large_ladder_only_counts_trades_over_threshold():
    # 2317 @ 258：1 張 = 25.8 萬，門檻 100 萬 -> 需 4 張
    agg = SymbolAggregator("2317", large_order_twd=1_000_000)
    agg.add_trade({"symbol": "2317", "price": 258, "size": 3, "bid": 257.5,
                   "ask": 258, "time": 1, "serial": 1})   # 77.4 萬，不算
    agg.add_trade({"symbol": "2317", "price": 258, "size": 5, "bid": 257.5,
                   "ask": 258, "time": 2, "serial": 2})   # 129 萬，算
    snap = agg.snapshot()
    assert {r["price"]: r["buy_lots"] for r in snap["ladder"]} == {258: 8}
    assert {r["price"]: r["buy_lots"] for r in snap["large_ladder"]} == {258: 5}


def test_set_threshold_recomputes_large_ladder_without_replaying_feed():
    agg = SymbolAggregator("2317", large_order_twd=1_000_000)
    agg.add_trade({"symbol": "2317", "price": 258, "size": 3, "bid": 257.5,
                   "ask": 258, "time": 1, "serial": 1})   # 77.4 萬
    assert agg.snapshot()["large_ladder"] == []

    agg.set_threshold(500_000)                            # 降門檻後 77.4 萬達標
    assert {r["price"]: r["buy_lots"] for r in agg.snapshot()["large_ladder"]} == {258: 3}

    agg.set_threshold(2_000_000)                          # 升門檻後又不達標
    assert agg.snapshot()["large_ladder"] == []


def test_snapshot_carries_book_and_last_price():
    agg = SymbolAggregator("2330", large_order_twd=1_000_000)
    agg.add_trade(AT_ASK)
    agg.update_book(BOOK)
    snap = agg.snapshot()
    assert snap["symbol"] == "2330"
    assert snap["last_price"] == 2405
    assert snap["bids"][0] == {"price": 2390, "size": 226}
    assert snap["asks"][0] == {"price": 2395, "size": 343}
    assert snap["large_order_twd"] == 1_000_000


def test_snapshot_totals_reconcile_with_ladder():
    agg = SymbolAggregator("2330", large_order_twd=1_000_000)
    for t in (AT_ASK, AT_BID, AT_BID_AGAIN, AUCTION):
        agg.add_trade(t)
    snap = agg.snapshot()
    assert snap["totals"]["buy_lots"] == 2
    assert snap["totals"]["sell_lots"] == 4
    assert snap["totals"]["auction_lots"] == 2024
    assert snap["totals"]["buy_lots"] == sum(r["buy_lots"] for r in snap["ladder"])


def test_recent_trades_are_capped_and_newest_first():
    agg = SymbolAggregator("2330", large_order_twd=1_000_000, recent_limit=2)
    for serial in (1, 2, 3):
        agg.add_trade({**AT_ASK, "serial": serial, "time": serial})
    recent = agg.snapshot()["recent_trades"]
    assert [r["serial"] for r in recent] == [3, 2]
```

- [ ] **Step 2: 執行測試確認失敗**

```bash
/Users/chia-chingcho/.local/share/virtualenvs/tw_stock_realtime-JRdNzDPE/bin/python -m pytest tests/test_aggregator.py -q
```
Expected: FAIL，`ModuleNotFoundError: No module named 'app.aggregator'`

- [ ] **Step 3: 實作**

建立 `app/aggregator.py`：

```python
"""單一股票的即時聚合狀態。

保留完整逐筆明細的原因：大單門檻可在 Web UI 即時調整，改門檻時只需就地
重算大單階梯，不必斷線重連或重跑程式。
"""

from __future__ import annotations

from app.classify import (
    AUCTION, BUY, SELL, UNKNOWN,
    SHARES_PER_LOT, classify_side, trade_value_twd,
)

_SIDE_FIELDS = {BUY: "buy_lots", SELL: "sell_lots",
                AUCTION: "auction_lots", UNKNOWN: "unknown_lots"}


def _empty_level(price: float) -> dict:
    return {"price": price, "buy_lots": 0, "sell_lots": 0,
            "auction_lots": 0, "unknown_lots": 0}


def _ladder_rows(levels: dict) -> list[dict]:
    """價位階梯，價高在上（與券商看盤習慣一致）。"""
    return [levels[p] for p in sorted(levels, reverse=True)]


class SymbolAggregator:
    def __init__(self, symbol: str, large_order_twd: float,
                 recent_limit: int = 200) -> None:
        self.symbol = symbol
        self.large_order_twd = large_order_twd
        self.recent_limit = recent_limit
        self.trades: list[dict] = []
        self._serials: set = set()
        self._levels: dict[float, dict] = {}
        self._large_levels: dict[float, dict] = {}
        self.bids: list[dict] = []
        self.asks: list[dict] = []
        self.last_price: float | None = None
        self.last_trade_time: int | None = None
        self.book_time: int | None = None

    # -- ingest ---------------------------------------------------------
    def add_trade(self, trade: dict) -> dict | None:
        """累加一筆成交。重複的 serial 回傳 None 且不影響任何統計。"""
        serial = trade.get("serial")
        if serial is not None:
            if serial in self._serials:
                return None
            self._serials.add(serial)

        lots = trade["size"]
        price = trade["price"]
        side = classify_side(trade)
        value = trade_value_twd(price, lots)
        record = {
            "symbol": self.symbol,
            "serial": serial,
            "time": trade["time"],
            "price": price,
            "lots": lots,
            "shares": lots * SHARES_PER_LOT,
            "side": side,
            "value_twd": value,
            "is_large": value >= self.large_order_twd,
        }
        self.trades.append(record)
        self.last_price = price
        self.last_trade_time = trade["time"]

        field = _SIDE_FIELDS[side]
        self._levels.setdefault(price, _empty_level(price))[field] += lots
        if record["is_large"]:
            self._large_levels.setdefault(price, _empty_level(price))[field] += lots
        return record

    def update_book(self, book: dict) -> None:
        self.bids = list(book.get("bids") or [])
        self.asks = list(book.get("asks") or [])
        self.book_time = book.get("time")

    # -- threshold ------------------------------------------------------
    def set_threshold(self, threshold_twd: float) -> None:
        """就地改門檻並重算大單階梯；逐筆明細已在記憶體，不需重連。"""
        self.large_order_twd = threshold_twd
        self._large_levels = {}
        for record in self.trades:
            record["is_large"] = record["value_twd"] >= threshold_twd
            if record["is_large"]:
                field = _SIDE_FIELDS[record["side"]]
                self._large_levels.setdefault(
                    record["price"], _empty_level(record["price"]))[field] += record["lots"]

    # -- read -----------------------------------------------------------
    def snapshot(self) -> dict:
        ladder = _ladder_rows(self._levels)
        totals = {field: sum(row[field] for row in ladder)
                  for field in _SIDE_FIELDS.values()}
        return {
            "symbol": self.symbol,
            "last_price": self.last_price,
            "last_trade_time": self.last_trade_time,
            "book_time": self.book_time,
            "bids": self.bids,
            "asks": self.asks,
            "ladder": ladder,
            "large_ladder": _ladder_rows(self._large_levels),
            "large_order_twd": self.large_order_twd,
            "totals": totals,
            "trade_count": len(self.trades),
            "recent_trades": list(reversed(self.trades[-self.recent_limit:])),
            "recent_large_trades": list(reversed(
                [t for t in self.trades if t["is_large"]][-self.recent_limit:])),
        }
```

- [ ] **Step 4: 執行測試確認通過**

```bash
/Users/chia-chingcho/.local/share/virtualenvs/tw_stock_realtime-JRdNzDPE/bin/python -m pytest tests/test_aggregator.py -q
```
Expected: PASS（11 passed）

- [ ] **Step 5: Commit**

```bash
git add app/aggregator.py tests/test_aggregator.py
git commit -m "feat: add per-symbol ladder aggregation with live-adjustable large-order threshold"
```

---

## Task 3: Parquet 緩衝寫入器

**Files:**
- Create: `app/storage.py`
- Test: `tests/test_storage.py`
- Modify: `requirements.txt`（新增 `pyarrow`）
- Modify: `Pipfile`（`[packages]` 新增 `pyarrow = "*"`）

**Interfaces:**
- Consumes: Task 2 的正規化紀錄欄位（`symbol, serial, time, price, lots, shares, side, value_twd, is_large`）
- Produces:
  - `TRADE_SCHEMA: pyarrow.Schema`
  - `class ParquetTradeWriter(path: pathlib.Path, batch_size: int = 500)`
    - `append(record: dict) -> None`
    - `flush() -> None`
    - `close() -> None`
    - 支援 context manager（`__enter__` / `__exit__`）
  - `output_path(base_dir, date_str, kind, symbol) -> pathlib.Path`

- [ ] **Step 1: 安裝相依套件**

一次裝齊後續任務要用的三個套件（Task 5 的 `app/web.py` 與 Task 7 的
`app/__main__.py` 在 import 時就需要 fastapi / uvicorn，缺了會讓那兩個任務的
測試連收集階段都失敗）：

```bash
cd /Users/chia-chingcho/Documents/vscode_project/tw_stock_realtime
/Users/chia-chingcho/.local/share/virtualenvs/tw_stock_realtime-JRdNzDPE/bin/python \
  -m pip install pyarrow fastapi uvicorn httpx
```

（`httpx` 是 `fastapi.testclient.TestClient` 的相依套件，Task 5 測試需要。）

- [ ] **Step 2: 寫失敗的測試**

建立 `tests/test_storage.py`：

```python
import pyarrow.parquet as pq

from app.storage import ParquetTradeWriter, output_path


RECORD = {
    "symbol": "2330", "serial": 13674373, "time": 1785902209312276,
    "price": 2405.0, "lots": 2, "shares": 2000, "side": "buy",
    "value_twd": 4_810_000.0, "is_large": True,
}


def test_writer_persists_records(tmp_path):
    path = tmp_path / "trades_2330.parquet"
    with ParquetTradeWriter(path) as writer:
        writer.append(RECORD)
        writer.append({**RECORD, "serial": 13674374, "side": "sell", "price": 2400.0})

    table = pq.read_table(path)
    assert table.num_rows == 2
    rows = table.to_pylist()
    assert rows[0]["side"] == "buy"
    assert rows[1]["price"] == 2400.0


def test_fractional_price_survives_round_trip(tmp_path):
    path = tmp_path / "trades_2317.parquet"
    with ParquetTradeWriter(path) as writer:
        writer.append({**RECORD, "symbol": "2317", "price": 257.5})
    assert pq.read_table(path).to_pylist()[0]["price"] == 257.5


def test_auction_trade_without_serial_is_written(tmp_path):
    """集合競價那筆在部分情況沒有 serial —— 不可因此丟資料。"""
    path = tmp_path / "trades.parquet"
    with ParquetTradeWriter(path) as writer:
        writer.append({**RECORD, "serial": None, "side": "auction", "lots": 2024})
    row = pq.read_table(path).to_pylist()[0]
    assert row["serial"] is None
    assert row["lots"] == 2024


def test_records_are_buffered_until_batch_size(tmp_path):
    path = tmp_path / "trades.parquet"
    writer = ParquetTradeWriter(path, batch_size=3)
    writer.append(RECORD)
    writer.append(RECORD)
    assert not path.exists(), "未達批次量不應寫檔"
    writer.append(RECORD)
    assert path.exists(), "達到批次量應寫出"
    writer.close()
    assert pq.read_table(path).num_rows == 3


def test_close_flushes_partial_batch(tmp_path):
    path = tmp_path / "trades.parquet"
    writer = ParquetTradeWriter(path, batch_size=100)
    writer.append(RECORD)
    writer.close()
    assert pq.read_table(path).num_rows == 1


def test_close_without_records_creates_no_file(tmp_path):
    path = tmp_path / "trades.parquet"
    ParquetTradeWriter(path).close()
    assert not path.exists()


def test_output_path_layout(tmp_path):
    path = output_path(tmp_path, "2026-08-05", "trades", "2330")
    assert path == tmp_path / "2026-08-05" / "trades_2330.parquet"
```

- [ ] **Step 3: 執行測試確認失敗**

```bash
/Users/chia-chingcho/.local/share/virtualenvs/tw_stock_realtime-JRdNzDPE/bin/python -m pytest tests/test_storage.py -q
```
Expected: FAIL，`ModuleNotFoundError: No module named 'app.storage'`

- [ ] **Step 4: 實作**

建立 `app/storage.py`：

```python
"""Parquet 緩衝寫入。

逐筆成交量大（單日單檔實測逾 1 萬筆），逐筆開檔寫入成本過高，因此累積到
batch_size 才寫一個 row group；close() 會把未滿的批次補寫出去。
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

TRADE_SCHEMA = pa.schema([
    ("symbol", pa.string()),
    ("serial", pa.int64()),          # 集合競價事件可能沒有 -> 允許 null
    ("time", pa.int64()),            # epoch microseconds
    ("price", pa.float64()),         # 可能為小數，例如 2317 的 257.5
    ("lots", pa.int64()),            # 張
    ("shares", pa.int64()),          # 股 = 張 × 1000
    ("side", pa.string()),           # buy / sell / auction / unknown
    ("value_twd", pa.float64()),
    ("is_large", pa.bool_()),
])


def output_path(base_dir: Path, date_str: str, kind: str, symbol: str) -> Path:
    return Path(base_dir) / date_str / f"{kind}_{symbol}.parquet"


class ParquetTradeWriter:
    def __init__(self, path: Path, batch_size: int = 500) -> None:
        self.path = Path(path)
        self.batch_size = batch_size
        self._buffer: list[dict] = []
        self._writer: pq.ParquetWriter | None = None

    def __enter__(self) -> "ParquetTradeWriter":
        return self

    def __exit__(self, *_exc) -> None:
        self.close()

    def append(self, record: dict) -> None:
        self._buffer.append({name: record.get(name) for name in TRADE_SCHEMA.names})
        if len(self._buffer) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        if not self._buffer:
            return
        if self._writer is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._writer = pq.ParquetWriter(self.path, TRADE_SCHEMA)
        self._writer.write_table(pa.Table.from_pylist(self._buffer, schema=TRADE_SCHEMA))
        self._buffer.clear()

    def close(self) -> None:
        self.flush()
        if self._writer is not None:
            self._writer.close()
            self._writer = None
```

- [ ] **Step 5: 執行測試確認通過**

```bash
/Users/chia-chingcho/.local/share/virtualenvs/tw_stock_realtime-JRdNzDPE/bin/python -m pytest tests/test_storage.py -q
```
Expected: PASS（7 passed）

- [ ] **Step 6: 更新相依宣告**

`requirements.txt` 改為：
```
fugle-marketdata
certifi
pyarrow
fastapi
uvicorn
```

`Pipfile` 的 `[packages]` 改為：
```toml
[packages]
fugle-marketdata = "*"
pyarrow = "*"
fastapi = "*"
uvicorn = "*"
```

- [ ] **Step 7: Commit**

```bash
git add app/storage.py tests/test_storage.py requirements.txt Pipfile
git commit -m "feat: add buffered parquet trade writer"
```

---

## Task 4: Fugle 行情接線（單一連線、多檔多頻道）

**Files:**
- Create: `app/feed.py`
- Test: `tests/test_feed.py`

**Interfaces:**
- Consumes: 無（僅相依 fugle-marketdata SDK）
- Produces:
  - `class FugleFeed(api_key: str, symbols: list[str], on_trade: Callable[[dict], None], on_book: Callable[[dict], None], include_trials: bool = False)`
    - `run() -> None` — 阻塞式，供背景執行緒呼叫
    - `handle_message(raw: str) -> None` — 供測試直接注入訊息
  - `CHANNELS: tuple[str, ...] = ("trades", "books")`

- [ ] **Step 1: 寫失敗的測試**

建立 `tests/test_feed.py`：

```python
"""單一連線約束與訂閱時序。

實測事實：
- 一把 API key 只能開一條 WebSocket 連線，超過會被以 close frame
  "Maximum number of connections reached" 斷線。
- SDK 在 connect 事件內送出 auth frame 但不等回覆，因此必須等
  authenticated 事件才能訂閱，否則被拒為 "Forbidden resource"。
"""

import json
import sys
import types

import pytest

from app.feed import CHANNELS, FugleFeed


class FakeStock:
    def __init__(self, timeline):
        self.timeline = timeline
        self.handlers = {}
        self.subscriptions = []

    def on(self, event, listener):
        self.handlers[event] = listener

    def subscribe(self, params):
        self.subscriptions.append(params)
        self.timeline.append(("subscribe", params["channel"], params["symbol"]))

    def connect(self):
        self.timeline.append(("auth_frame_sent",))
        self.handlers["connect"]()
        self.timeline.append(("server_authenticated",))
        self.handlers["authenticated"]('{"event":"authenticated","data":{}}')


@pytest.fixture
def fake_sdk(monkeypatch):
    timeline = []
    stock = FakeStock(timeline)
    created = []

    def make_client(**kwargs):
        created.append(kwargs)
        return types.SimpleNamespace(stock=stock)

    sdk = types.ModuleType("fugle_marketdata")
    sdk.WebSocketClient = make_client
    monkeypatch.setitem(sys.modules, "fugle_marketdata", sdk)
    certifi = types.ModuleType("certifi")
    certifi.where = lambda: "/dev/null"
    monkeypatch.setitem(sys.modules, "certifi", certifi)
    return types.SimpleNamespace(stock=stock, timeline=timeline, created=created)


def test_uses_exactly_one_connection_for_all_symbols(fake_sdk):
    FugleFeed("k", ["2330", "2317", "2454"], lambda t: None, lambda b: None).run()
    assert len(fake_sdk.created) == 1, "API key 只允許一條連線"


def test_subscribes_every_symbol_on_both_channels(fake_sdk):
    FugleFeed("k", ["2330", "2317"], lambda t: None, lambda b: None).run()
    subscribed = {(s["channel"], s["symbol"]) for s in fake_sdk.stock.subscriptions}
    assert subscribed == {(c, s) for c in CHANNELS for s in ("2330", "2317")}


def test_all_subscriptions_happen_after_authentication(fake_sdk):
    FugleFeed("k", ["2330", "2317"], lambda t: None, lambda b: None).run()
    auth_at = fake_sdk.timeline.index(("server_authenticated",))
    first_sub = min(i for i, e in enumerate(fake_sdk.timeline) if e[0] == "subscribe")
    assert first_sub > auth_at, f"訂閱早於認證會被拒為 Forbidden resource: {fake_sdk.timeline}"


def test_trade_events_reach_the_trade_callback(fake_sdk):
    trades = []
    feed = FugleFeed("k", ["2330"], trades.append, lambda b: None)
    feed.handle_message(json.dumps({
        "event": "data", "channel": "trades",
        "data": {"symbol": "2330", "price": 2405, "size": 1, "bid": 2400,
                 "ask": 2405, "time": 1, "serial": 7},
    }))
    assert [t["serial"] for t in trades] == [7]


def test_book_events_reach_the_book_callback(fake_sdk):
    books = []
    feed = FugleFeed("k", ["2330"], lambda t: None, books.append)
    feed.handle_message(json.dumps({
        "event": "data", "channel": "books",
        "data": {"symbol": "2330", "bids": [{"price": 2390, "size": 226}],
                 "asks": [{"price": 2395, "size": 343}], "time": 2},
    }))
    assert books[0]["bids"][0]["price"] == 2390


def test_trial_matches_are_skipped_by_default(fake_sdk):
    trades = []
    feed = FugleFeed("k", ["2330"], trades.append, lambda b: None)
    feed.handle_message(json.dumps({
        "event": "data", "channel": "trades",
        "data": {"symbol": "2330", "price": 2385, "size": 2113, "isTrial": True,
                 "time": 3, "serial": 8},
    }))
    assert trades == []


def test_trial_matches_kept_when_requested(fake_sdk):
    trades = []
    feed = FugleFeed("k", ["2330"], trades.append, lambda b: None, include_trials=True)
    feed.handle_message(json.dumps({
        "event": "data", "channel": "trades",
        "data": {"symbol": "2330", "price": 2385, "size": 2113, "isTrial": True,
                 "time": 3, "serial": 8},
    }))
    assert [t["serial"] for t in trades] == [8]


def test_untracked_symbol_is_ignored(fake_sdk):
    trades = []
    feed = FugleFeed("k", ["2330"], trades.append, lambda b: None)
    feed.handle_message(json.dumps({
        "event": "data", "channel": "trades",
        "data": {"symbol": "2454", "price": 1000, "size": 1, "bid": 999,
                 "ask": 1000, "time": 4, "serial": 9},
    }))
    assert trades == []


def test_malformed_message_does_not_kill_the_feed(fake_sdk):
    trades = []
    feed = FugleFeed("k", ["2330"], trades.append, lambda b: None)
    feed.handle_message("not json")                      # 不可拋出
    feed.handle_message(json.dumps({
        "event": "data", "channel": "trades",
        "data": {"symbol": "2330", "price": 2405, "size": 1, "bid": 2400,
                 "ask": 2405, "time": 5, "serial": 10},
    }))
    assert [t["serial"] for t in trades] == [10]
```

- [ ] **Step 2: 執行測試確認失敗**

```bash
/Users/chia-chingcho/.local/share/virtualenvs/tw_stock_realtime-JRdNzDPE/bin/python -m pytest tests/test_feed.py -q
```
Expected: FAIL，`ModuleNotFoundError: No module named 'app.feed'`

- [ ] **Step 3: 實作**

建立 `app/feed.py`：

```python
"""Fugle 行情接線。

單一連線約束：一把 API key 只能開一條 WebSocket 連線（實測超過會收到
close frame "Maximum number of connections reached"），所有股票與頻道都
必須擠在這一條上。實測單一連線同時承載 2 檔 × 2 頻道共 4 個訂閱，零錯誤。
"""

from __future__ import annotations

import json
import os
import sys
from typing import Callable

CHANNELS = ("trades", "books")


class FugleFeed:
    def __init__(self, api_key: str, symbols: list[str],
                 on_trade: Callable[[dict], None],
                 on_book: Callable[[dict], None],
                 include_trials: bool = False) -> None:
        self.api_key = api_key
        self.symbols = list(symbols)
        self.on_trade = on_trade
        self.on_book = on_book
        self.include_trials = include_trials
        self._stock = None

    # -- message handling ------------------------------------------------
    def handle_message(self, raw: str) -> None:
        """處理一則訊息。任何單則訊息的問題都不得中斷整條行情。"""
        try:
            event = json.loads(raw)
            name = event.get("event")
            if name == "authenticated":
                print("API Key authenticated.", flush=True)
                return
            if name == "subscribed":
                data = event.get("data") or {}
                print(f"Subscribed: {data.get('channel')} {data.get('symbol')}", flush=True)
                return
            if name == "error":
                print(f"Fugle API error: {event.get('data')}", file=sys.stderr, flush=True)
                return
            if name != "data":
                return

            channel = event.get("channel")
            data = event.get("data") or {}
            if data.get("symbol") not in self.symbols:
                return
            if channel == "trades":
                if data.get("isTrial", False) and not self.include_trials:
                    return
                self.on_trade(data)
            elif channel == "books":
                self.on_book(data)
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as error:
            print(f"Unable to process message: {error}", file=sys.stderr, flush=True)

    # -- lifecycle -------------------------------------------------------
    def _subscribe_all(self, _message=None) -> None:
        # 只在伺服器確認認證後才送訂閱：SDK 在 connect 事件內送出 auth frame
        # 卻不等回覆，此時訂閱會被拒為 "Forbidden resource"。
        for symbol in self.symbols:
            for channel in CHANNELS:
                self._stock.subscribe({"channel": channel, "symbol": symbol})

    def run(self) -> None:
        """阻塞式連線；請在背景執行緒呼叫。"""
        import certifi
        from fugle_marketdata import WebSocketClient

        os.environ.setdefault("SSL_CERT_FILE", certifi.where())
        os.environ.setdefault("REQUESTS_CA_BUNDLE", certifi.where())

        client = WebSocketClient(api_key=self.api_key)
        self._stock = client.stock
        self._stock.on("message", self.handle_message)
        self._stock.on("authenticated", self._subscribe_all)
        self._stock.on("connect", lambda: print("Connected. Authenticating...", flush=True))
        self._stock.on("error", lambda e: print(f"WebSocket error: {e}",
                                                file=sys.stderr, flush=True))
        self._stock.connect()
```

- [ ] **Step 4: 執行測試確認通過**

```bash
/Users/chia-chingcho/.local/share/virtualenvs/tw_stock_realtime-JRdNzDPE/bin/python -m pytest tests/test_feed.py -q
```
Expected: PASS（9 passed）

- [ ] **Step 5: Commit**

```bash
git add app/feed.py tests/test_feed.py
git commit -m "feat: add single-connection multi-symbol Fugle feed with auth-gated subscribe"
```

---

## Task 5: Web 服務（快照 API、門檻調整、WebSocket 推送）

**Files:**
- Create: `app/web.py`
- Test: `tests/test_web.py`

**Interfaces:**
- Consumes: `app.aggregator.SymbolAggregator`
- Produces:
  - `class Broadcaster()` — 執行緒安全的廣播器
    - `bind_loop(loop: asyncio.AbstractEventLoop) -> None`
    - `publish(symbol: str) -> None`（可從非同步以外的執行緒呼叫）
    - `subscribe() -> asyncio.Queue`
    - `unsubscribe(queue) -> None`
  - `class MarketState(symbols: list[str], large_order_twd: float)`
    - `aggregator(symbol) -> SymbolAggregator`
    - `snapshot(symbol) -> dict`
    - `snapshot_all() -> dict`
    - `set_threshold(threshold_twd: float) -> None`
  - `create_app(state: MarketState, broadcaster: Broadcaster) -> fastapi.FastAPI`
  - 路由：`GET /`、`GET /api/symbols`、`GET /api/snapshot/{symbol}`、`POST /api/threshold`、`WS /ws`

- [ ] **Step 1: 寫失敗的測試**

建立 `tests/test_web.py`：

```python
import asyncio

from fastapi.testclient import TestClient

from app.web import Broadcaster, MarketState, create_app


AT_ASK = {"symbol": "2330", "price": 2405, "size": 2, "bid": 2400, "ask": 2405,
          "time": 1785902209312276, "serial": 1}
BOOK = {"symbol": "2330",
        "bids": [{"price": 2390, "size": 226}],
        "asks": [{"price": 2395, "size": 343}], "time": 2}


def build(symbols=("2330", "2317"), threshold=1_000_000):
    state = MarketState(list(symbols), large_order_twd=threshold)
    return state, TestClient(create_app(state, Broadcaster()))


def test_symbols_endpoint_lists_tracked_symbols():
    _, client = build()
    body = client.get("/api/symbols").json()
    assert body["symbols"] == ["2330", "2317"]
    assert body["large_order_twd"] == 1_000_000


def test_snapshot_endpoint_returns_ladder_and_book():
    state, client = build()
    state.aggregator("2330").add_trade(AT_ASK)
    state.aggregator("2330").update_book(BOOK)

    body = client.get("/api/snapshot/2330").json()
    assert body["last_price"] == 2405
    assert body["bids"][0]["price"] == 2390
    assert body["ladder"][0]["buy_lots"] == 2
    assert body["totals"]["buy_lots"] == 2


def test_snapshot_of_untracked_symbol_is_404():
    _, client = build()
    assert client.get("/api/snapshot/9999").status_code == 404


def test_threshold_endpoint_recomputes_every_symbol():
    state, client = build(threshold=10_000_000)
    state.aggregator("2330").add_trade(AT_ASK)          # 481 萬
    assert state.snapshot("2330")["large_ladder"] == []

    response = client.post("/api/threshold", json={"large_order_twd": 1_000_000})
    assert response.status_code == 200
    assert response.json()["large_order_twd"] == 1_000_000
    assert state.snapshot("2330")["large_ladder"][0]["buy_lots"] == 2


def test_threshold_endpoint_rejects_non_positive():
    _, client = build()
    assert client.post("/api/threshold", json={"large_order_twd": 0}).status_code == 422


def test_index_page_is_served():
    _, client = build()
    response = client.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]


def test_websocket_sends_full_snapshot_on_connect():
    state, client = build()
    state.aggregator("2330").add_trade(AT_ASK)
    with client.websocket_connect("/ws") as ws:
        first = ws.receive_json()                        # 連上先給完整快照
        assert first["type"] == "init"
        assert first["snapshots"]["2330"]["last_price"] == 2405
        assert set(first["snapshots"]) == {"2330", "2317"}


def test_broadcaster_publish_is_safe_from_another_thread():
    """行情在 SDK 執行緒收，推送在 asyncio 迴圈 —— 必須跨執行緒安全。"""
    import threading

    broadcaster = Broadcaster()

    async def scenario():
        broadcaster.bind_loop(asyncio.get_running_loop())
        queue = broadcaster.subscribe()
        threading.Thread(target=broadcaster.publish, args=("2330",)).start()
        return await asyncio.wait_for(queue.get(), timeout=2)

    assert asyncio.run(scenario()) == "2330"


def test_broadcaster_drops_symbols_when_no_loop_bound():
    """尚未綁定事件迴圈時 publish 不得拋例外（行情可能早於 web 啟動）。"""
    Broadcaster().publish("2330")
```

- [ ] **Step 2: 執行測試確認失敗**

```bash
/Users/chia-chingcho/.local/share/virtualenvs/tw_stock_realtime-JRdNzDPE/bin/python -m pytest tests/test_web.py -q
```
Expected: FAIL，`ModuleNotFoundError: No module named 'app.web'`

- [ ] **Step 3: 實作**

建立 `app/web.py`：

```python
"""Web 服務：快照 API、門檻調整、瀏覽器 WebSocket 推送。

執行緒模型：Fugle SDK 是同步／執行緒式，FastAPI 是 asyncio。行情在 SDK
執行緒收，透過 Broadcaster 以 loop.call_soon_threadsafe 丟進 asyncio 佇列，
再由 /ws 的協程推給瀏覽器。
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from app.aggregator import SymbolAggregator

_STATIC = Path(__file__).resolve().parent / "static"

# 五檔每秒可有數次更新，逐一推送對瀏覽器沒有意義；同一檔在此間隔內合併。
PUSH_INTERVAL_SECONDS = 0.2


class ThresholdIn(BaseModel):
    large_order_twd: float = Field(gt=0)


class Broadcaster:
    """跨執行緒的「某檔有更新」通知。內容不進佇列，只送 symbol，
    推送時再取當下快照，天然達成合併效果。"""

    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._queues: set[asyncio.Queue] = set()

    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop

    def subscribe(self) -> asyncio.Queue:
        queue: asyncio.Queue = asyncio.Queue()
        self._queues.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue) -> None:
        self._queues.discard(queue)

    def publish(self, symbol: str) -> None:
        """可從任何執行緒呼叫。web 尚未啟動時安靜丟棄。"""
        loop = self._loop
        if loop is None:
            return
        for queue in list(self._queues):
            loop.call_soon_threadsafe(queue.put_nowait, symbol)


class MarketState:
    def __init__(self, symbols: list[str], large_order_twd: float) -> None:
        self.symbols = list(symbols)
        self.large_order_twd = large_order_twd
        self._aggregators = {
            s: SymbolAggregator(s, large_order_twd) for s in self.symbols
        }

    def aggregator(self, symbol: str) -> SymbolAggregator:
        if symbol not in self._aggregators:
            raise KeyError(symbol)
        return self._aggregators[symbol]

    def snapshot(self, symbol: str) -> dict:
        return self.aggregator(symbol).snapshot()

    def snapshot_all(self) -> dict:
        return {s: a.snapshot() for s, a in self._aggregators.items()}

    def set_threshold(self, threshold_twd: float) -> None:
        self.large_order_twd = threshold_twd
        for aggregator in self._aggregators.values():
            aggregator.set_threshold(threshold_twd)


def create_app(state: MarketState, broadcaster: Broadcaster) -> FastAPI:
    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        # 廣播器要在事件迴圈起來後才能綁定。使用 lifespan 而非
        # @app.on_event("startup")：後者在 FastAPI 0.109+ 已棄用。
        broadcaster.bind_loop(asyncio.get_running_loop())
        yield

    app = FastAPI(title="台股即時大單追蹤", lifespan=lifespan)

    @app.get("/")
    def index() -> FileResponse:
        return FileResponse(_STATIC / "index.html")

    @app.get("/api/symbols")
    def symbols() -> dict:
        return {"symbols": state.symbols, "large_order_twd": state.large_order_twd}

    @app.get("/api/snapshot/{symbol}")
    def snapshot(symbol: str) -> dict:
        try:
            return state.snapshot(symbol)
        except KeyError:
            raise HTTPException(status_code=404, detail=f"symbol not tracked: {symbol}")

    @app.post("/api/threshold")
    def set_threshold(body: ThresholdIn) -> dict:
        state.set_threshold(body.large_order_twd)
        for symbol in state.symbols:
            broadcaster.publish(symbol)
        return {"large_order_twd": state.large_order_twd}

    @app.websocket("/ws")
    async def stream(websocket: WebSocket) -> None:
        await websocket.accept()
        await websocket.send_json({"type": "init", "snapshots": state.snapshot_all(),
                                   "large_order_twd": state.large_order_twd})
        queue = broadcaster.subscribe()
        try:
            while True:
                symbol = await queue.get()
                # 合併間隔內的重複通知，避免逐事件推送
                await asyncio.sleep(PUSH_INTERVAL_SECONDS)
                pending = {symbol}
                while not queue.empty():
                    pending.add(queue.get_nowait())
                for name in pending:
                    await websocket.send_json({"type": "update",
                                               "snapshot": state.snapshot(name)})
        except WebSocketDisconnect:
            pass
        finally:
            broadcaster.unsubscribe(queue)

    return app
```

- [ ] **Step 4: 建立畫面佔位檔讓 `GET /` 可通過**

```bash
mkdir -p app/static
printf '<!doctype html><meta charset="utf-8"><title>台股即時大單追蹤</title>\n' > app/static/index.html
```

- [ ] **Step 5: 執行測試確認通過**

```bash
/Users/chia-chingcho/.local/share/virtualenvs/tw_stock_realtime-JRdNzDPE/bin/python -m pytest tests/test_web.py -q
```
Expected: PASS（9 passed）

- [ ] **Step 6: Commit**

```bash
git add app/web.py app/static/index.html tests/test_web.py
git commit -m "feat: add web API with thread-safe broadcaster and websocket push"
```

---

## Task 6: 看盤畫面

**Files:**
- Modify: `app/static/index.html`（取代 Task 5 的佔位內容）

**Interfaces:**
- Consumes: `GET /api/symbols`、`WS /ws` 的 `init` / `update` 訊息、`POST /api/threshold`
- Produces: 無程式介面（純前端）

- [ ] **Step 1: 寫出完整畫面**

以下列內容取代 `app/static/index.html`：

```html
<!doctype html>
<meta charset="utf-8">
<title>台股即時大單追蹤</title>
<style>
  :root { color-scheme: light dark; }
  body { font: 14px/1.5 -apple-system, "PingFang TC", sans-serif; margin: 1rem; }
  header { display: flex; gap: 1rem; align-items: center; flex-wrap: wrap; }
  .tabs { display: flex; gap: .5rem; }
  .tab { padding: .3rem .8rem; border: 1px solid #8884; border-radius: 4px; cursor: pointer; }
  .tab.active { background: #8883; font-weight: 600; }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 1rem; margin-top: 1rem; }
  section { border: 1px solid #8884; border-radius: 6px; padding: .6rem; }
  h2 { font-size: 1rem; margin: 0 0 .5rem; }
  table { width: 100%; border-collapse: collapse; }
  th, td { padding: .2rem .4rem; text-align: right; border-bottom: 1px solid #8882; }
  th:first-child, td:first-child { text-align: left; }
  .buy { color: #c0392b; }        /* 外盤，紅 */
  .sell { color: #27ae60; }       /* 內盤，綠 */
  .auction { background: #8882; } /* 集合競價 */
  .scroll { max-height: 320px; overflow-y: auto; }
  .price { font-size: 1.6rem; font-weight: 700; }
  .muted { opacity: .65; font-size: .85em; }
</style>

<header>
  <strong>台股即時大單追蹤</strong>
  <div class="tabs" id="tabs"></div>
  <label>大單門檻
    <input id="threshold" type="number" min="1" step="100000" style="width: 9rem">
    元
  </label>
  <button id="apply">套用</button>
  <span id="status" class="muted">連線中…</span>
</header>

<div class="grid" id="panels"></div>

<script>
const state = { snapshots: {}, symbols: [], active: null };
const fmt = n => (n ?? 0).toLocaleString("zh-TW");
const money = n => (n ?? 0).toLocaleString("zh-TW", { maximumFractionDigits: 0 });
const clock = us => us ? new Date(us / 1000).toLocaleTimeString("zh-TW", { hour12: false }) : "—";

function sideLabel(side) {
  return { buy: "買", sell: "賣", auction: "競價", unknown: "?" }[side] ?? side;
}

function ladderTable(rows, title) {
  if (!rows.length) return `<h2>${title}</h2><p class="muted">尚無資料</p>`;
  const body = rows.map(r => `<tr class="${r.auction_lots ? "auction" : ""}">
      <td>${r.price}</td>
      <td class="buy">${fmt(r.buy_lots)}</td>
      <td class="sell">${fmt(r.sell_lots)}</td>
      ${r.auction_lots ? `<td>${fmt(r.auction_lots)}</td>` : "<td>—</td>"}
    </tr>`).join("");
  return `<h2>${title}</h2><div class="scroll"><table>
    <tr><th>價格</th><th>買(外盤)</th><th>賣(內盤)</th><th>集合競價</th></tr>
    ${body}</table></div>`;
}

function tradeTable(rows, title) {
  if (!rows.length) return `<h2>${title}</h2><p class="muted">尚無資料</p>`;
  const body = rows.map(t => `<tr>
      <td>${clock(t.time)}</td>
      <td>${t.price}</td>
      <td class="${t.side}">${sideLabel(t.side)}</td>
      <td>${fmt(t.lots)}</td>
      <td>${money(t.value_twd)}</td>
    </tr>`).join("");
  return `<h2>${title}</h2><div class="scroll"><table>
    <tr><th>時間</th><th>價格</th><th>別</th><th>張數</th><th>金額</th></tr>
    ${body}</table></div>`;
}

function bookTable(snap) {
  const rows = [];
  for (let i = snap.asks.length - 1; i >= 0; i--) {
    rows.push(`<tr><td class="sell">賣${i + 1}</td><td>${snap.asks[i].price}</td><td>${fmt(snap.asks[i].size)}</td></tr>`);
  }
  snap.bids.forEach((b, i) => {
    rows.push(`<tr><td class="buy">買${i + 1}</td><td>${b.price}</td><td>${fmt(b.size)}</td></tr>`);
  });
  return `<h2>五檔 <span class="muted">${clock(snap.book_time)}</span></h2>
    <table><tr><th>檔位</th><th>價格</th><th>張數</th></tr>${rows.join("")}</table>`;
}

function render() {
  const snap = state.snapshots[state.active];
  if (!snap) return;
  document.getElementById("panels").innerHTML = `
    <section>
      <h2>${snap.symbol}</h2>
      <div class="price">${snap.last_price ?? "—"}</div>
      <div class="muted">最新成交 ${clock(snap.last_trade_time)}｜共 ${fmt(snap.trade_count)} 筆</div>
      <table style="margin-top:.5rem">
        <tr><th>買(外盤)</th><td class="buy">${fmt(snap.totals.buy_lots)} 張</td></tr>
        <tr><th>賣(內盤)</th><td class="sell">${fmt(snap.totals.sell_lots)} 張</td></tr>
        <tr><th>集合競價</th><td>${fmt(snap.totals.auction_lots)} 張</td></tr>
        ${snap.totals.unknown_lots ? `<tr><th>未分類</th><td>${fmt(snap.totals.unknown_lots)} 張</td></tr>` : ""}
      </table>
    </section>
    <section>${bookTable(snap)}</section>
    <section>${ladderTable(snap.ladder, "當日每價位成交量")}</section>
    <section>${ladderTable(snap.large_ladder, `大單每價位成交量（≥ ${money(snap.large_order_twd)} 元）`)}</section>
    <section>${tradeTable(snap.recent_trades, "近期成交明細")}</section>
    <section>${tradeTable(snap.recent_large_trades, "近期大單明細")}</section>`;
}

function renderTabs() {
  document.getElementById("tabs").innerHTML = state.symbols.map(s =>
    `<span class="tab ${s === state.active ? "active" : ""}" data-symbol="${s}">${s}</span>`).join("");
  document.querySelectorAll(".tab").forEach(el =>
    el.onclick = () => { state.active = el.dataset.symbol; renderTabs(); render(); });
}

const ws = new WebSocket(`ws://${location.host}/ws`);
ws.onopen = () => document.getElementById("status").textContent = "已連線";
ws.onclose = () => document.getElementById("status").textContent = "已斷線，請重整頁面";
ws.onmessage = event => {
  const msg = JSON.parse(event.data);
  if (msg.type === "init") {
    state.snapshots = msg.snapshots;
    state.symbols = Object.keys(msg.snapshots);
    state.active = state.active ?? state.symbols[0];
    document.getElementById("threshold").value = msg.large_order_twd;
    renderTabs();
  } else if (msg.type === "update") {
    state.snapshots[msg.snapshot.symbol] = msg.snapshot;
    if (msg.snapshot.symbol !== state.active) return;
  }
  render();
};

document.getElementById("apply").onclick = async () => {
  const value = Number(document.getElementById("threshold").value);
  if (!(value > 0)) return;
  await fetch("/api/threshold", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ large_order_twd: value }),
  });
};
</script>
```

- [ ] **Step 2: 確認 Task 5 的測試仍通過**

```bash
/Users/chia-chingcho/.local/share/virtualenvs/tw_stock_realtime-JRdNzDPE/bin/python -m pytest tests/test_web.py -q
```
Expected: PASS（9 passed）

- [ ] **Step 3: Commit**

```bash
git add app/static/index.html
git commit -m "feat: add live market dashboard page"
```

---

## Task 7: CLI 進入點（端到端組裝）

**Files:**
- Create: `app/__main__.py`
- Test: `tests/test_main.py`

**Interfaces:**
- Consumes: `app.feed.FugleFeed`、`app.web.MarketState / Broadcaster / create_app`、`app.storage.ParquetTradeWriter / output_path`、`collector.load_dotenv`
- Produces:
  - `parse_args(argv: list[str]) -> argparse.Namespace`
  - `class Pipeline(state, broadcaster, writers)` — `handle_trade(trade)`、`handle_book(book)`、`close()`
  - `main(argv: list[str] | None = None) -> None`

- [ ] **Step 1: 寫失敗的測試**

建立 `tests/test_main.py`：

```python
import pyarrow.parquet as pq
import pytest

from app.__main__ import Pipeline, parse_args
from app.storage import ParquetTradeWriter
from app.web import Broadcaster, MarketState


AT_ASK = {"symbol": "2330", "price": 2405, "size": 2, "bid": 2400, "ask": 2405,
          "time": 1785902209312276, "serial": 1}
BOOK = {"symbol": "2330", "bids": [{"price": 2390, "size": 226}],
        "asks": [{"price": 2395, "size": 343}], "time": 2}


def test_parse_args_accepts_comma_separated_symbols():
    args = parse_args(["--symbols", "2330,2317,2454", "--large-order", "1000000"])
    assert args.symbols == ["2330", "2317", "2454"]
    assert args.large_order == 1_000_000


def test_parse_args_strips_whitespace_around_symbols():
    assert parse_args(["--symbols", " 2330 , 2317 "]).symbols == ["2330", "2317"]


def test_parse_args_rejects_non_positive_threshold():
    with pytest.raises(SystemExit):
        parse_args(["--symbols", "2330", "--large-order", "0"])


def build_pipeline(tmp_path, symbols=("2330",), threshold=1_000_000):
    state = MarketState(list(symbols), large_order_twd=threshold)
    writers = {s: ParquetTradeWriter(tmp_path / f"trades_{s}.parquet", batch_size=1)
               for s in symbols}
    return state, Pipeline(state, Broadcaster(), writers)


def test_pipeline_aggregates_and_persists_trade(tmp_path):
    state, pipeline = build_pipeline(tmp_path)
    pipeline.handle_trade(AT_ASK)
    pipeline.close()

    assert state.snapshot("2330")["ladder"][0]["buy_lots"] == 2
    row = pq.read_table(tmp_path / "trades_2330.parquet").to_pylist()[0]
    assert row["side"] == "buy"
    assert row["value_twd"] == 4_810_000


def test_pipeline_updates_book(tmp_path):
    state, pipeline = build_pipeline(tmp_path)
    pipeline.handle_book(BOOK)
    assert state.snapshot("2330")["asks"][0]["price"] == 2395


def test_pipeline_does_not_persist_duplicate_serial(tmp_path):
    _, pipeline = build_pipeline(tmp_path)
    pipeline.handle_trade(AT_ASK)
    pipeline.handle_trade(AT_ASK)
    pipeline.close()
    assert pq.read_table(tmp_path / "trades_2330.parquet").num_rows == 1


def test_pipeline_ignores_untracked_symbol(tmp_path):
    _, pipeline = build_pipeline(tmp_path)
    pipeline.handle_trade({**AT_ASK, "symbol": "9999"})   # 不可拋出
    pipeline.close()
    assert not (tmp_path / "trades_9999.parquet").exists()
```

- [ ] **Step 2: 執行測試確認失敗**

```bash
/Users/chia-chingcho/.local/share/virtualenvs/tw_stock_realtime-JRdNzDPE/bin/python -m pytest tests/test_main.py -q
```
Expected: FAIL，`ModuleNotFoundError: No module named 'app.__main__'`

- [ ] **Step 3: 實作**

建立 `app/__main__.py`：

```python
"""CLI 進入點：一個指令同時啟動行情收集與看盤網頁。

行情在背景執行緒跑（Fugle SDK 是阻塞式），uvicorn 在主執行緒跑。
"""

from __future__ import annotations

import argparse
import datetime
import os
import sys
import threading
from pathlib import Path

import uvicorn

import collector                       # 沿用既有的 .env 載入器
from app.feed import FugleFeed
from app.storage import ParquetTradeWriter, output_path
from app.web import Broadcaster, MarketState, create_app


class Pipeline:
    """把行情事件接到聚合器、寫檔器與廣播器。"""

    def __init__(self, state: MarketState, broadcaster: Broadcaster,
                 writers: dict[str, ParquetTradeWriter]) -> None:
        self.state = state
        self.broadcaster = broadcaster
        self.writers = writers

    def handle_trade(self, trade: dict) -> None:
        symbol = trade.get("symbol")
        try:
            aggregator = self.state.aggregator(symbol)
        except KeyError:
            return
        record = aggregator.add_trade(trade)
        if record is None:            # 重複 serial，不重複寫檔
            return
        writer = self.writers.get(symbol)
        if writer is not None:
            writer.append(record)
        self.broadcaster.publish(symbol)

    def handle_book(self, book: dict) -> None:
        symbol = book.get("symbol")
        try:
            self.state.aggregator(symbol).update_book(book)
        except KeyError:
            return
        self.broadcaster.publish(symbol)

    def close(self) -> None:
        for writer in self.writers.values():
            writer.close()


def _symbol_list(raw: str) -> list[str]:
    symbols = [s.strip() for s in raw.split(",") if s.strip()]
    if not symbols:
        raise argparse.ArgumentTypeError("至少要指定一個股票代碼")
    return symbols


def _positive(raw: str) -> float:
    value = float(raw)
    if value <= 0:
        raise argparse.ArgumentTypeError("大單門檻必須大於 0")
    return value


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python3 -m app", description="台股多檔即時大單追蹤")
    parser.add_argument("--symbols", type=_symbol_list, default=["2330"],
                        help="逗號分隔的股票代碼，例如 2330,2317,2454")
    parser.add_argument("--large-order", type=_positive, default=1_000_000,
                        help="大單門檻（台幣），金額 = 價格 × 張數 × 1000")
    parser.add_argument("--output-dir", type=Path, default=Path("data"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--include-trials", action="store_true",
                        help="一併記錄開盤前試撮")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    collector.load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    api_key = os.getenv("FUGLE_API_KEY")
    if not api_key or api_key == "replace_with_your_api_key":
        raise SystemExit("FUGLE_API_KEY is not set. Copy .env.example to .env and add your API key.")

    today = datetime.date.today().isoformat()
    state = MarketState(args.symbols, large_order_twd=args.large_order)
    broadcaster = Broadcaster()
    writers = {
        symbol: ParquetTradeWriter(output_path(args.output_dir, today, "trades", symbol))
        for symbol in args.symbols
    }
    pipeline = Pipeline(state, broadcaster, writers)
    feed = FugleFeed(api_key, args.symbols, pipeline.handle_trade,
                     pipeline.handle_book, include_trials=args.include_trials)

    # 一把 API key 只能開一條連線：確認沒有其他收集器在跑，否則會被伺服器
    # 以 "Maximum number of connections reached" 斷線。
    threading.Thread(target=feed.run, daemon=True).start()
    print(f"追蹤 {', '.join(args.symbols)}｜大單門檻 {args.large_order:,.0f} 元", flush=True)
    print(f"看盤畫面 http://{args.host}:{args.port}", flush=True)
    try:
        uvicorn.run(create_app(state, broadcaster), host=args.host, port=args.port,
                    log_level="warning")
    except KeyboardInterrupt:
        pass
    finally:
        pipeline.close()
        print("\n已停止，資料已寫入 " + str(args.output_dir), flush=True)


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: 執行測試確認通過**

```bash
/Users/chia-chingcho/.local/share/virtualenvs/tw_stock_realtime-JRdNzDPE/bin/python -m pytest tests/test_main.py -q
```
Expected: PASS（7 passed）

- [ ] **Step 5: 全套件測試**

```bash
/Users/chia-chingcho/.local/share/virtualenvs/tw_stock_realtime-JRdNzDPE/bin/python -m pytest -q
```
Expected: PASS（含既有 `test_collector.py` 的 8 項）

- [ ] **Step 6: Commit**

```bash
git add app/__main__.py tests/test_main.py
git commit -m "feat: add CLI entry point wiring feed, aggregation, storage and web"
```

---

## Task 8: CSV 匯出

**Files:**
- Create: `app/export.py`
- Test: `tests/test_export.py`

**Interfaces:**
- Consumes: `app.storage.TRADE_SCHEMA`（Parquet 欄位）
- Produces:
  - `export_csv(parquet_path: Path, csv_path: Path) -> int` — 回傳寫出的列數
  - `main(argv: list[str] | None = None) -> None` — `python3 -m app.export --date 2026-08-05`

- [ ] **Step 1: 寫失敗的測試**

建立 `tests/test_export.py`：

```python
import csv

import pytest

from app.export import export_csv
from app.storage import ParquetTradeWriter


RECORD = {"symbol": "2330", "serial": 1, "time": 1785902209312276, "price": 2405.0,
          "lots": 2, "shares": 2000, "side": "buy", "value_twd": 4_810_000.0,
          "is_large": True}


def test_export_writes_all_rows(tmp_path):
    source = tmp_path / "trades_2330.parquet"
    with ParquetTradeWriter(source) as writer:
        writer.append(RECORD)
        writer.append({**RECORD, "serial": 2, "side": "sell"})

    target = tmp_path / "trades_2330.csv"
    assert export_csv(source, target) == 2

    rows = list(csv.DictReader(target.open(encoding="utf-8")))
    assert [r["side"] for r in rows] == ["buy", "sell"]
    assert rows[0]["price"] == "2405.0"


def test_export_includes_taipei_time_column(tmp_path):
    source = tmp_path / "trades.parquet"
    with ParquetTradeWriter(source) as writer:
        writer.append(RECORD)
    target = tmp_path / "trades.csv"
    export_csv(source, target)
    row = next(iter(csv.DictReader(target.open(encoding="utf-8"))))
    assert row["time_taipei"].startswith("2026-08-05T11:56:49")


def test_export_missing_source_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        export_csv(tmp_path / "nope.parquet", tmp_path / "out.csv")
```

- [ ] **Step 2: 執行測試確認失敗**

```bash
/Users/chia-chingcho/.local/share/virtualenvs/tw_stock_realtime-JRdNzDPE/bin/python -m pytest tests/test_export.py -q
```
Expected: FAIL，`ModuleNotFoundError: No module named 'app.export'`

- [ ] **Step 3: 實作**

建立 `app/export.py`：

```python
"""Parquet -> CSV 匯出。

平時以 Parquet 落地（體積小、事後分析快），需要用 Excel 或 tail 直接看時
再匯出成 CSV，並附上台北時間欄位方便人眼閱讀。
"""

from __future__ import annotations

import argparse
import csv
import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq

from app.storage import TRADE_SCHEMA

_TAIPEI = ZoneInfo("Asia/Taipei")
CSV_FIELDS = ["time_taipei", *TRADE_SCHEMA.names]


def _taipei(microseconds: int | None) -> str:
    if microseconds is None:
        return ""
    moment = datetime.datetime.fromtimestamp(microseconds / 1_000_000,
                                             tz=datetime.timezone.utc)
    return moment.astimezone(_TAIPEI).isoformat()


def export_csv(parquet_path: Path, csv_path: Path) -> int:
    parquet_path = Path(parquet_path)
    if not parquet_path.exists():
        raise FileNotFoundError(parquet_path)
    rows = pq.read_table(parquet_path).to_pylist()
    csv_path = Path(csv_path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({"time_taipei": _taipei(row.get("time")), **row})
    return len(rows)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python3 -m app.export",
                                     description="把當日 Parquet 匯出成 CSV")
    parser.add_argument("--date", required=True, help="資料日期，例如 2026-08-05")
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    args = parser.parse_args(argv)

    day_dir = args.data_dir / args.date
    sources = sorted(day_dir.glob("*.parquet"))
    if not sources:
        raise SystemExit(f"{day_dir} 下沒有 parquet 檔")
    for source in sources:
        target = source.with_suffix(".csv")
        print(f"{source.name} -> {target.name}: {export_csv(source, target)} 列")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: 執行測試確認通過**

```bash
/Users/chia-chingcho/.local/share/virtualenvs/tw_stock_realtime-JRdNzDPE/bin/python -m pytest tests/test_export.py -q
```
Expected: PASS（3 passed）

- [ ] **Step 5: Commit**

```bash
git add app/export.py tests/test_export.py
git commit -m "feat: add parquet to csv export command"
```

---

## Task 9: 實機驗證與文件

**Files:**
- Modify: `README.md`

**Interfaces:**
- Consumes: 前八個任務的全部成果
- Produces: 無程式介面

- [ ] **Step 1: 確認沒有其他收集器佔用連線**

```bash
ps aux | grep "[c]ollector.py"; ps aux | grep "[a]pp.__main__"
```
Expected: 兩者皆無輸出。**若有，先 `kill` 掉**——一把 API key 只能開一條連線。

- [ ] **Step 2: 盤中實機執行**

台股交易時段（平日 09:00–13:30）執行：

```bash
cd /Users/chia-chingcho/Documents/vscode_project/tw_stock_realtime
/Users/chia-chingcho/.local/share/virtualenvs/tw_stock_realtime-JRdNzDPE/bin/python \
  -m app --symbols 2330,2317 --large-order 1000000
```

Expected 終端輸出依序為：
```
追蹤 2330, 2317｜大單門檻 1,000,000 元
看盤畫面 http://127.0.0.1:8000
Connected. Authenticating...
API Key authenticated.
Subscribed: trades 2330
Subscribed: books 2330
Subscribed: trades 2317
Subscribed: books 2317
```

**不可出現** `Forbidden resource`（代表訂閱早於認證）或 `Maximum number of connections reached`（代表有其他連線）。

- [ ] **Step 3: 檢查畫面**

瀏覽器開 `http://127.0.0.1:8000`，確認：
- 兩個股票代碼分頁可切換
- 目前價格會跳動
- 五檔買賣各 5 檔且有掛單張數
- 「當日每價位成交量」買紅賣綠分開
- 「大單每價位成交量」只含達門檻者
- 把門檻改成 `50000000` 後按套用，大單區塊即時縮減且**連線不中斷**（終端不應出現重連訊息）

- [ ] **Step 4: 用交易所數字對帳**

程式執行中另開終端：

```bash
/Users/chia-chingcho/.local/share/virtualenvs/tw_stock_realtime-JRdNzDPE/bin/python - <<'PY'
import os, pathlib, sys, urllib.request, json
sys.path.insert(0, ".")
import collector
collector.load_dotenv(pathlib.Path(".env"))
from fugle_marketdata import RestClient

mine = json.load(urllib.request.urlopen("http://127.0.0.1:8000/api/snapshot/2330"))["totals"]
theirs = RestClient(api_key=os.environ["FUGLE_API_KEY"]).stock.intraday.quote(symbol="2330")["total"]
print(f"買(外盤) 我們 {mine['buy_lots']:>7,}｜交易所 {theirs['tradeVolumeAtAsk']:>7,}")
print(f"賣(內盤) 我們 {mine['sell_lots']:>7,}｜交易所 {theirs['tradeVolumeAtBid']:>7,}")
print("註：程式啟動前的成交不在我們的統計內，差額應等於啟動前的累計量。")
PY
```

程式若**從 09:00 前就啟動**，兩邊應完全相等。盤中才啟動則差額為啟動前累計量——此時改以「啟動後每筆逐一比對」判斷正確性。

- [ ] **Step 5: 確認落檔與匯出**

```bash
ls -la data/$(date +%F)/
/Users/chia-chingcho/.local/share/virtualenvs/tw_stock_realtime-JRdNzDPE/bin/python -m app.export --date $(date +%F)
head -3 data/$(date +%F)/trades_2330.csv
```
Expected: 每檔股票一個 `trades_<symbol>.parquet`，匯出後有對應 `.csv`，首列含 `time_taipei` 欄位。

- [ ] **Step 6: 更新 README**

在 `README.md` 末尾新增：

````markdown
## 多檔即時大單追蹤（app 套件）

`collector.py` 是單檔逐筆收集器；`app` 套件是多檔版本，額外提供價位聚合、
大單篩選與即時看盤畫面。

```bash
python3 -m app --symbols 2330,2317,2454 --large-order 1000000
# 看盤畫面 http://127.0.0.1:8000
```

| 參數 | 預設 | 說明 |
|------|------|------|
| `--symbols` | `2330` | 逗號分隔的股票代碼 |
| `--large-order` | `1000000` | 大單門檻（台幣）。金額 = 價格 × 張數 × 1000 |
| `--output-dir` | `data` | Parquet 輸出目錄 |
| `--host` / `--port` | `127.0.0.1` / `8000` | 看盤畫面位址 |
| `--include-trials` | 關閉 | 一併記錄開盤前試撮 |

大單門檻也可在畫面上即時調整，會就地重算、不需重連。

### 買賣方向如何判定

Fugle 的成交事件沒有買賣旗標，方向由成交價與當下最佳一檔推得：

| 條件 | 分類 |
|------|------|
| 無 `bid`/`ask` 欄位 | 集合競價（開盤／收盤，無主動方） |
| `price >= ask` | 買（外盤，買方主動） |
| `price <= bid` | 賣（內盤，賣方主動） |

此規則以 2026-08-05 當日 2330 全 10,055 筆逐筆成交驗證過：買 15,113 張、
賣 10,042 張，與交易所 `total.tradeVolumeAtAsk` / `tradeVolumeAtBid` **零差異**；
連續交易時段沒有任何一筆落在五檔中間。

### 連線數限制

**一把 API key 只能開一條 WebSocket 連線。** 執行 `python3 -m app` 期間不可
同時執行 `collector.py`，否則後啟動的那支會被伺服器以
`Maximum number of connections reached` 斷線。

### 資料格式

逐筆成交寫成 `data/<日期>/trades_<代碼>.parquet`，欄位：
`symbol, serial, time, price, lots, shares, side, value_twd, is_large`
（`lots` 單位張、`shares` 單位股、`time` 為 epoch 微秒）。

需要 CSV 時：

```bash
python3 -m app.export --date 2026-08-05
```
````

- [ ] **Step 7: Commit**

```bash
git add README.md
git commit -m "docs: document multi-symbol large-order tracker"
```

---

## Self-Review

**規格涵蓋檢查**

| 規格要求 | 對應任務 |
|---------|---------|
| 輸入多個股票代碼 | Task 7（`--symbols`，`_symbol_list`） |
| 輸入大單成交金額 | Task 7（`--large-order`）＋ Task 5/6（畫面即時調整） |
| 即時取得交易資訊 | Task 4（單一連線訂閱 trades） |
| 統整每個價位交易單量、買賣分開 | Task 2（`ladder`，`buy_lots`/`sell_lots` 分欄） |
| 大單篩選（量 × 價 ≥ 門檻） | Task 1（`is_large_order`）＋ Task 2（`large_ladder`） |
| 大單也依價位統整、買賣分開 | Task 2（`large_ladder` 同結構） |
| 寫入 csv 或 parquet | Task 3（Parquet）＋ Task 8（CSV 匯出） |
| Web UI：股票代號 | Task 6（分頁 + 標題） |
| Web UI：目前價格 | Task 6（`last_price`） |
| Web UI：五檔價格及掛單量 | Task 4（books 頻道）＋ Task 6（`bookTable`） |
| Web UI：當日所有交易單量及價格 | Task 6（`ladder` 表 + 成交明細表） |
| Web UI：當日所有大單交易單量及價格 | Task 6（`large_ladder` 表 + 大單明細表） |

無遺漏項目。

**型別一致性檢查**

- 正規化紀錄欄位在 Task 2 定義（`symbol, serial, time, price, lots, shares, side, value_twd, is_large`），Task 3 的 `TRADE_SCHEMA`、Task 7 的 `Pipeline.handle_trade`、Task 8 的 `CSV_FIELDS` 全部一致。
- `SymbolAggregator.add_trade` 回傳 `dict | None`，Task 7 有處理 `None`（重複 serial）。
- `Broadcaster.publish(symbol: str)` 在 Task 5 定義，Task 7 以相同簽名呼叫。
- `output_path(base_dir, date_str, kind, symbol)` 在 Task 3 定義，Task 7 以相同位置參數呼叫。
- `classify_side` 的四個回傳值與 `aggregator._SIDE_FIELDS` 的四個鍵完全對應。

**尚未涵蓋、留給後續的項目**

- 斷線自動重連：SDK 不內建，目前斷線需手動重啟。逐筆資料已落檔，重啟不影響已收資料，但會遺失斷線空窗期。
- 跨日換檔：程式以啟動當日的日期建目錄，跨午夜不會自動換檔。目前使用情境為單日盤中，不影響。
