"""單一股票的即時聚合狀態。

保留完整逐筆明細的原因：大單門檻可在 Web UI 即時調整，改門檻時只需就地
重算大單階梯與時間桶，不必斷線重連或重跑程式。
"""

from __future__ import annotations

from app.classify import (
    AUCTION, BUY, SELL, UNKNOWN,
    SHARES_PER_LOT, classify_side, trade_value_twd,
)

_SIDE_FIELDS = {BUY: "buy_lots", SELL: "sell_lots",
                AUCTION: "auction_lots", UNKNOWN: "unknown_lots"}

# 柱狀圖只畫有主動方的大單：auction 沒有主動方、unknown 判不出主動方，
# 兩者都不該被畫成「買方進場」或「賣方出場」。
_BAR_FIELDS = {BUY: "buy_lots", SELL: "sell_lots"}

BUCKET_MICROS = 5 * 60 * 1_000_000      # 五分鐘一格


def _empty_level(price: float) -> dict:
    return {"price": price, "buy_lots": 0, "sell_lots": 0,
            "auction_lots": 0, "unknown_lots": 0}


def _ladder_rows(levels: dict) -> list[dict]:
    """價位階梯，價高在上（與券商看盤習慣一致）。"""
    return [dict(levels[p]) for p in sorted(levels, reverse=True)]


def bucket_start(time_us: int) -> int:
    """把成交時間向下取整到五分鐘桶的起始微秒。"""
    return (time_us // BUCKET_MICROS) * BUCKET_MICROS


class SymbolAggregator:
    def __init__(self, symbol: str, large_order_lots: int,
                 recent_limit: int = 200, name: str = "") -> None:
        self.symbol = symbol
        self.name = name
        self.large_order_lots = large_order_lots
        self.recent_limit = recent_limit
        self.trades: list[dict] = []
        self._serials: set = set()
        self._levels: dict[float, dict] = {}
        self._large_levels: dict[float, dict] = {}
        self._buckets: dict[int, dict] = {}
        self.last_price: float | None = None
        self.last_trade_time: int | None = None

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
        record = {
            "symbol": self.symbol,
            "serial": serial,
            "time": trade["time"],
            "price": price,
            "lots": lots,
            "shares": lots * SHARES_PER_LOT,
            "side": side,
            # 門檻改用張數之後金額不再參與判定，但仍要落檔：日後想改回
            # 金額門檻或做金額分析，不必重跑一整天的行情。
            "value_twd": trade_value_twd(price, lots),
            "is_large": lots >= self.large_order_lots,
        }
        self.trades.append(record)
        self.last_price = price
        self.last_trade_time = trade["time"]

        field = _SIDE_FIELDS[side]
        self._levels.setdefault(price, _empty_level(price))[field] += lots
        if record["is_large"]:
            self._large_levels.setdefault(price, _empty_level(price))[field] += lots
        self._add_to_bucket(record)
        return record

    def _add_to_bucket(self, record: dict) -> None:
        start = bucket_start(record["time"])
        bucket = self._buckets.get(start)
        if bucket is None:
            bucket = self._buckets[start] = {
                "t": start, "buy_lots": 0, "sell_lots": 0,
                "close": record["price"],
            }
        # 收盤價取桶內最後一筆成交，不限大單 —— 只看大單的價會在成交稀疏的
        # 桶裡跳來跳去，與畫面上的最新成交價對不起來。
        bucket["close"] = record["price"]
        if record["is_large"]:
            field = _BAR_FIELDS.get(record["side"])
            if field is not None:
                bucket[field] += record["lots"]

    # -- threshold ------------------------------------------------------
    def set_threshold(self, threshold_lots: int) -> None:
        """就地改門檻並重算大單階梯與時間桶；逐筆明細已在記憶體，不需重連。

        桶必須跟著重算，理由與大單階梯相同：兩者都只計大單，門檻一改，
        舊的統計就是用另一個定義算出來的，留著會與畫面上的門檻自相矛盾。
        """
        self.large_order_lots = threshold_lots
        self._large_levels = {}
        # 桶的 close 與門檻無關，但重建比就地清零省事且不會漏掉任何一桶
        self._buckets = {}
        for record in self.trades:
            record["is_large"] = record["lots"] >= threshold_lots
            if record["is_large"]:
                field = _SIDE_FIELDS[record["side"]]
                self._large_levels.setdefault(
                    record["price"], _empty_level(record["price"]))[field] += record["lots"]
            self._add_to_bucket(record)

    # -- read -----------------------------------------------------------
    def snapshot(self) -> dict:
        ladder = _ladder_rows(self._levels)
        totals = {field: sum(row[field] for row in ladder)
                  for field in _SIDE_FIELDS.values()}
        return {
            "symbol": self.symbol,
            "name": self.name,
            "last_price": self.last_price,
            "last_trade_time": self.last_trade_time,
            "ladder": ladder,
            "large_ladder": _ladder_rows(self._large_levels),
            "large_order_lots": self.large_order_lots,
            "totals": totals,
            "trade_count": len(self.trades),
            "recent_trades": [dict(t) for t in reversed(self.trades[-self.recent_limit:])],
            "buckets": [dict(self._buckets[t]) for t in sorted(self._buckets)],
        }
