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


def test_broadcaster_drops_symbols_after_loop_closed():
    """關機時迴圈已關、SDK 執行緒仍在送 —— publish 不得從回呼執行緒拋出。"""
    broadcaster = Broadcaster()

    async def bind():
        broadcaster.bind_loop(asyncio.get_running_loop())
        broadcaster.subscribe()

    asyncio.run(bind())          # asyncio.run 結束時會關閉該迴圈
    broadcaster.publish("2330")  # 不得拋 RuntimeError


def test_record_trade_routes_to_the_right_aggregator():
    state, _ = build()
    record = state.record_trade(AT_ASK)
    assert record["symbol"] == "2330"
    assert state.snapshot("2330")["ladder"][0]["buy_lots"] == 2
    assert state.snapshot("2317")["trade_count"] == 0


def test_record_trade_ignores_untracked_symbol():
    state, _ = build()
    assert state.record_trade({**AT_ASK, "symbol": "9999"}) is None


def test_record_trade_ignores_duplicate_serial():
    state, _ = build()
    assert state.record_trade(AT_ASK) is not None
    assert state.record_trade(AT_ASK) is None


def test_record_book_reports_whether_symbol_is_tracked():
    state, _ = build()
    assert state.record_book(BOOK) is True
    assert state.record_book({**BOOK, "symbol": "9999"}) is False
    assert state.snapshot("2330")["bids"][0]["price"] == 2390


def test_threshold_change_is_correct_under_concurrent_ingest():
    """set_threshold 與行情寫入同時發生時，大單階梯不得重複計數。

    未加鎖時這會失敗：set_threshold 重建 _large_levels 的迴圈一旦超過 CPython
    的 GIL 切換間隔（5 ms）就會被切斷，切斷後進來的成交會被算兩次 —— 由
    add_trade 自己一次，再由重算迴圈走到它一次。歷史筆數必須夠多才會跨過
    那條線（實測 10,000 筆 3.7 ms 不會錯，50,000 筆 26.9 ms 必錯）。
    """
    import threading

    state = MarketState(["2330"], large_order_twd=1_000_000)
    for i in range(50_000):                       # 內盤(賣) @2400，重算需 ~27 ms
        state.record_trade({"symbol": "2330", "price": 2400, "size": 1, "bid": 2400,
                            "ask": 2405, "time": i, "serial": i})

    stop = threading.Event()

    def ingest():                                  # 模擬 SDK 行情執行緒：外盤(買) @2405
        serial = 10 ** 8
        while not stop.is_set():
            state.record_trade({"symbol": "2330", "price": 2405, "size": 1, "bid": 2400,
                                "ask": 2405, "time": serial, "serial": serial})
            serial += 1

    thread = threading.Thread(target=ingest)
    thread.start()
    try:
        state.set_threshold(1_000_000)
    finally:
        stop.set()
        thread.join()

    snapshot = state.snapshot("2330")
    fields = {"buy": "buy_lots", "sell": "sell_lots",
              "auction": "auction_lots", "unknown": "unknown_lots"}
    expected: dict = {}
    for trade in state.aggregator("2330").trades:
        if trade["value_twd"] >= snapshot["large_order_twd"]:
            key = (trade["price"], fields[trade["side"]])
            expected[key] = expected.get(key, 0) + trade["lots"]
    actual = {(row["price"], field): row[field]
              for row in snapshot["large_ladder"]
              for field in fields.values() if row[field]}
    assert actual == expected
