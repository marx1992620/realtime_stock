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


def test_only_bid_present_is_unknown_not_auction():
    """漲停時賣方佇列可能為空 —— 少了 ask 不代表這是集合競價，
    誤判會讓漲停日的成交全部灌進 auction_lots。"""
    limit_up = {"symbol": "2330", "price": 2640, "size": 5, "bid": 2640,
                "time": 1785902209312276, "serial": 1}
    assert classify.classify_side(limit_up) == classify.UNKNOWN


def test_only_ask_present_is_unknown_not_auction():
    """跌停時買方佇列可能為空 —— 同上。"""
    limit_down = {"symbol": "2330", "price": 2160, "size": 5, "ask": 2160,
                  "time": 1785902209312276, "serial": 2}
    assert classify.classify_side(limit_down) == classify.UNKNOWN


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


@pytest.mark.parametrize("lots,expected", [(4, False), (5, True), (6, True)])
def test_is_large_order_threshold_boundary(lots, expected):
    """門檻是張數，不是金額：門檻 5 時 4 張不算、5 張算。"""
    trade = {"price": 258, "size": lots, "bid": 257.5, "ask": 258}
    assert classify.is_large_order(trade, 5) is expected


def test_is_large_order_ignores_price():
    """張數門檻與價格無關 —— 同樣 5 張，25.8 萬與 1,202 萬都算大單。"""
    cheap = {"price": 258, "size": 5, "bid": 257.5, "ask": 258}
    dear = {"price": 2405, "size": 5, "bid": 2400, "ask": 2405}
    assert classify.is_large_order(cheap, 5) is True
    assert classify.is_large_order(dear, 5) is True


def test_auction_trade_can_also_be_large_order():
    # 開盤那筆 2,024 張遠超任何合理張數門檻
    assert classify.is_large_order(TRADE_AUCTION, 5) is True
