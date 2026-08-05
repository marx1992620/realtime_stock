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
