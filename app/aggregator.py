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
