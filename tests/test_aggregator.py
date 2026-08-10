from app.aggregator import SymbolAggregator


AT_ASK = {"symbol": "2330", "price": 2405, "size": 2, "bid": 2400, "ask": 2405,
          "time": 1785902209312276, "serial": 1}
AT_BID = {"symbol": "2330", "price": 2400, "size": 3, "bid": 2400, "ask": 2405,
          "time": 1785902195644483, "serial": 2}
AT_BID_AGAIN = {"symbol": "2330", "price": 2400, "size": 1, "bid": 2400, "ask": 2405,
                "time": 1785902198947291, "serial": 3}
AUCTION = {"price": 2385, "size": 2024, "time": 1785891605048734, "serial": 4}

# 09:00:00 整，正好是一個五分鐘桶的起點（1785891600 / 300 整除）
BUCKET_A = 1785891600_000000
FIVE_MIN = 300_000_000


def test_add_trade_returns_normalised_record():
    agg = SymbolAggregator("2330", large_order_lots=2)
    record = agg.add_trade(AT_ASK)
    assert record["symbol"] == "2330"
    assert record["serial"] == 1
    assert record["lots"] == 2
    assert record["shares"] == 2000
    assert record["side"] == "buy"
    # value_twd 保留：Parquet 仍落金額，兩種門檻都能事後重算
    assert record["value_twd"] == 4_810_000
    assert record["is_large"] is True


def test_duplicate_serial_is_ignored():
    agg = SymbolAggregator("2330", large_order_lots=1)
    assert agg.add_trade(AT_ASK) is not None
    assert agg.add_trade(AT_ASK) is None
    assert agg.snapshot()["ladder"][0]["buy_lots"] == 2   # 沒有被重複累加


def test_ladder_splits_buy_and_sell_per_price():
    agg = SymbolAggregator("2330", large_order_lots=1)
    for t in (AT_ASK, AT_BID, AT_BID_AGAIN):
        agg.add_trade(t)
    ladder = {row["price"]: row for row in agg.snapshot()["ladder"]}
    assert ladder[2405]["buy_lots"] == 2
    assert ladder[2405]["sell_lots"] == 0
    assert ladder[2400]["sell_lots"] == 4      # 3 + 1
    assert ladder[2400]["buy_lots"] == 0


def test_auction_volume_is_separate_from_buy_and_sell():
    agg = SymbolAggregator("2330", large_order_lots=1)
    agg.add_trade(AUCTION)
    row = {r["price"]: r for r in agg.snapshot()["ladder"]}[2385]
    assert row["auction_lots"] == 2024
    assert row["buy_lots"] == 0
    assert row["sell_lots"] == 0


def test_ladder_is_sorted_by_price_descending():
    agg = SymbolAggregator("2330", large_order_lots=1)
    for t in (AT_BID, AT_ASK, AUCTION):
        agg.add_trade(t)
    prices = [row["price"] for row in agg.snapshot()["ladder"]]
    assert prices == sorted(prices, reverse=True)


def test_fractional_prices_are_kept_distinct():
    agg = SymbolAggregator("2317", large_order_lots=1)
    agg.add_trade({"symbol": "2317", "price": 257.5, "size": 1, "bid": 257.5,
                   "ask": 258, "time": 1, "serial": 1})
    agg.add_trade({"symbol": "2317", "price": 258, "size": 1, "bid": 257.5,
                   "ask": 258, "time": 2, "serial": 2})
    prices = [row["price"] for row in agg.snapshot()["ladder"]]
    assert prices == [258, 257.5]


def test_large_ladder_only_counts_trades_at_or_over_the_lot_threshold():
    agg = SymbolAggregator("2317", large_order_lots=5)
    agg.add_trade({"symbol": "2317", "price": 258, "size": 4, "bid": 257.5,
                   "ask": 258, "time": 1, "serial": 1})   # 4 張，不算
    agg.add_trade({"symbol": "2317", "price": 258, "size": 5, "bid": 257.5,
                   "ask": 258, "time": 2, "serial": 2})   # 5 張，算
    snap = agg.snapshot()
    assert {r["price"]: r["buy_lots"] for r in snap["ladder"]} == {258: 9}
    assert {r["price"]: r["buy_lots"] for r in snap["large_ladder"]} == {258: 5}


def test_set_threshold_recomputes_large_ladder_without_replaying_feed():
    agg = SymbolAggregator("2317", large_order_lots=5)
    agg.add_trade({"symbol": "2317", "price": 258, "size": 3, "bid": 257.5,
                   "ask": 258, "time": 1, "serial": 1})   # 3 張
    assert agg.snapshot()["large_ladder"] == []

    agg.set_threshold(3)                                  # 降門檻後 3 張達標
    assert {r["price"]: r["buy_lots"] for r in agg.snapshot()["large_ladder"]} == {258: 3}

    agg.set_threshold(10)                                 # 升門檻後又不達標
    assert agg.snapshot()["large_ladder"] == []


def test_snapshot_carries_last_price_and_no_book_fields():
    """Task 16 C：五檔報價功能已移除，快照不再帶 bids/asks/book_time/has_book
    ——訂閱預算只有 5 個，不該讓五檔掛單跟股票搶配額。"""
    agg = SymbolAggregator("2330", large_order_lots=5)
    agg.add_trade(AT_ASK)
    snap = agg.snapshot()
    assert snap["symbol"] == "2330"
    assert snap["last_price"] == 2405
    assert snap["large_order_lots"] == 5
    for field in ("bids", "asks", "book_time", "has_book"):
        assert field not in snap


def test_snapshot_carries_name():
    plain = SymbolAggregator("2330", large_order_lots=5)
    assert plain.snapshot()["name"] == ""

    named = SymbolAggregator("2330", large_order_lots=5, name="台積電")
    assert named.snapshot()["name"] == "台積電"


def test_snapshot_totals_reconcile_with_ladder():
    agg = SymbolAggregator("2330", large_order_lots=1)
    for t in (AT_ASK, AT_BID, AT_BID_AGAIN, AUCTION):
        agg.add_trade(t)
    snap = agg.snapshot()
    assert snap["totals"]["buy_lots"] == 2
    assert snap["totals"]["sell_lots"] == 4
    assert snap["totals"]["auction_lots"] == 2024
    assert snap["totals"]["buy_lots"] == sum(r["buy_lots"] for r in snap["ladder"])


def test_recent_trades_are_capped_and_newest_first():
    agg = SymbolAggregator("2330", large_order_lots=1, recent_limit=2)
    for serial in (1, 2, 3):
        agg.add_trade({**AT_ASK, "serial": serial, "time": serial})
    recent = agg.snapshot()["recent_trades"]
    assert [r["serial"] for r in recent] == [3, 2]


def test_snapshot_has_no_per_trade_large_order_list():
    """逐筆大單明細已移除；依價位彙總的 large_ladder 才是使用者要的統計。"""
    agg = SymbolAggregator("2330", large_order_lots=1)
    agg.add_trade(AT_ASK)
    snap = agg.snapshot()
    assert "recent_large_trades" not in snap
    assert snap["recent_trades"]                 # 一般成交明細仍在


def test_snapshot_is_independent_of_later_mutations():
    agg = SymbolAggregator("2330", large_order_lots=1)
    agg.add_trade(AT_ASK)
    snap1 = agg.snapshot()
    row1 = snap1["ladder"][0]
    trade1 = snap1["recent_trades"][0]
    bucket1 = snap1["buckets"][0]

    original_buy_lots = row1["buy_lots"]
    original_trade_serial = trade1["serial"]

    agg.add_trade(AT_BID)
    agg.set_threshold(50)          # 重算 is_large、大單階梯與桶

    assert row1["buy_lots"] == original_buy_lots
    assert trade1["serial"] == original_trade_serial
    assert trade1["is_large"] is True  # original value unchanged
    assert bucket1["buy_lots"] == 2    # 舊快照的桶不得被重算改寫
    assert snap1["large_ladder"] == [{"price": 2405, "buy_lots": 2, "sell_lots": 0,
                                      "auction_lots": 0, "unknown_lots": 0}]
    assert snap1["large_ladder"] != agg.snapshot()["large_ladder"]  # current is different


# -- 五分鐘桶（大單流向圖的資料面）----------------------------------------

def test_buckets_group_large_orders_into_five_minute_slots():
    agg = SymbolAggregator("2330", large_order_lots=5)
    agg.add_trade({**AT_ASK, "serial": 1, "time": BUCKET_A + 1, "size": 5})
    agg.add_trade({**AT_ASK, "serial": 2, "time": BUCKET_A + FIVE_MIN - 1, "size": 7})
    agg.add_trade({**AT_BID, "serial": 3, "time": BUCKET_A + FIVE_MIN, "size": 6})

    buckets = agg.snapshot()["buckets"]
    assert [b["t"] for b in buckets] == [BUCKET_A, BUCKET_A + FIVE_MIN]
    assert (buckets[0]["buy_lots"], buckets[0]["sell_lots"]) == (12, 0)
    assert (buckets[1]["buy_lots"], buckets[1]["sell_lots"]) == (0, 6)


def test_buckets_are_sorted_by_time_even_if_trades_arrive_out_of_order():
    agg = SymbolAggregator("2330", large_order_lots=5)
    agg.add_trade({**AT_ASK, "serial": 1, "time": BUCKET_A + 2 * FIVE_MIN, "size": 5})
    agg.add_trade({**AT_ASK, "serial": 2, "time": BUCKET_A, "size": 5})
    assert [b["t"] for b in agg.snapshot()["buckets"]] == [
        BUCKET_A, BUCKET_A + 2 * FIVE_MIN]


def test_bucket_close_follows_every_trade_but_lots_only_count_large_ones():
    agg = SymbolAggregator("2330", large_order_lots=5)
    agg.add_trade({**AT_ASK, "serial": 1, "time": BUCKET_A + 1,
                   "size": 5, "price": 2405})
    agg.add_trade({**AT_ASK, "serial": 2, "time": BUCKET_A + 2,
                   "size": 1, "price": 2410})          # 小單：不進柱，但改收盤
    bucket = agg.snapshot()["buckets"][0]
    assert bucket["buy_lots"] == 5
    assert bucket["close"] == 2410


def test_auction_and_unknown_large_orders_stay_out_of_the_bars():
    """柱狀圖只畫有主動方的大單；集合競價與未分類仍影響收盤價。"""
    agg = SymbolAggregator("2330", large_order_lots=5)
    agg.add_trade({"symbol": "2330", "price": 2385, "size": 2024,
                   "time": BUCKET_A + 1, "serial": 1})              # auction
    agg.add_trade({"symbol": "2330", "price": 2640, "size": 10, "bid": 2640,
                   "time": BUCKET_A + 2, "serial": 2})              # unknown
    bucket = agg.snapshot()["buckets"][0]
    assert (bucket["buy_lots"], bucket["sell_lots"]) == (0, 0)
    assert bucket["close"] == 2640


def test_set_threshold_recomputes_buckets():
    agg = SymbolAggregator("2330", large_order_lots=10)
    agg.add_trade({**AT_ASK, "serial": 1, "time": BUCKET_A + 1, "size": 5})
    assert agg.snapshot()["buckets"] == [
        {"t": BUCKET_A, "buy_lots": 0, "sell_lots": 0, "close": 2405}]

    agg.set_threshold(5)
    assert agg.snapshot()["buckets"][0]["buy_lots"] == 5

    agg.set_threshold(50)
    assert agg.snapshot()["buckets"][0]["buy_lots"] == 0
    assert agg.snapshot()["buckets"][0]["close"] == 2405   # 收盤與門檻無關
