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


def test_snapshot_is_independent_of_later_mutations():
    agg = SymbolAggregator("2330", large_order_twd=1_000_000)
    agg.add_trade(AT_ASK)
    snap1 = agg.snapshot()
    row1 = snap1["ladder"][0]
    trade1 = snap1["recent_trades"][0]

    # Store original values from snapshot
    original_buy_lots = row1["buy_lots"]
    original_trade_serial = trade1["serial"]
    original_large_count = len(snap1["large_ladder"])

    # Mutate the aggregator with a new trade
    agg.add_trade(AT_BID)

    # Change the threshold, which should recompute is_large fields
    agg.set_threshold(10_000_000)

    # Verify the old snapshot was not mutated
    assert row1["buy_lots"] == original_buy_lots
    assert trade1["serial"] == original_trade_serial
    assert trade1["is_large"] is True  # original value unchanged
    assert snap1["large_ladder"] == [{"price": 2405, "buy_lots": 2, "sell_lots": 0,
                                      "auction_lots": 0, "unknown_lots": 0}]
    assert snap1["large_ladder"] != agg.snapshot()["large_ladder"]  # current is different
