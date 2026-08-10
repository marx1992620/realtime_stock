import asyncio
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.web import Broadcaster, MarketState, create_app

INDEX_HTML = (Path(__file__).resolve().parents[1]
              / "app" / "static" / "index.html").read_text(encoding="utf-8")


AT_ASK = {"symbol": "2330", "price": 2405, "size": 2, "bid": 2400, "ask": 2405,
          "time": 1785902209312276, "serial": 1}
BOOK = {"symbol": "2330",
        "bids": [{"price": 2390, "size": 226}],
        "asks": [{"price": 2395, "size": 343}], "time": 2}


def build(symbols=("2330", "2317"), threshold=5):
    state = MarketState(list(symbols), large_order_lots=threshold)
    return state, TestClient(create_app(state, Broadcaster()))


def test_symbols_endpoint_lists_tracked_symbols():
    _, client = build()
    body = client.get("/api/symbols").json()
    assert body["symbols"] == ["2330", "2317"]
    assert body["thresholds"] == {"2330": 5, "2317": 5}


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
    state, client = build(threshold=10)
    state.aggregator("2330").add_trade(AT_ASK)          # 2 張
    assert state.snapshot("2330")["large_ladder"] == []

    response = client.post("/api/threshold", json={"large_order_lots": 2})
    assert response.status_code == 200
    assert response.json()["thresholds"] == {"2330": 2, "2317": 2}
    assert state.snapshot("2330")["large_ladder"][0]["buy_lots"] == 2


def test_threshold_endpoint_rejects_non_positive():
    _, client = build()
    assert client.post("/api/threshold", json={"large_order_lots": 0}).status_code == 422


def test_threshold_endpoint_rejects_fractional_lots():
    """門檻的單位是張，沒有半張這種東西 —— 收下 2.5 只會讓實際門檻
    悄悄變成 3 張或 2 張，兩種都不是使用者要求的。"""
    _, client = build()
    assert client.post("/api/threshold",
                       json={"large_order_lots": 2.5}).status_code == 422


# -- 逐檔門檻 -------------------------------------------------------------

def test_per_symbol_thresholds_from_mapping():
    """張數門檻仍需逐檔設定：成交量大的股票同樣張數不算大單。"""
    state = MarketState(["2330", "2317"],
                        large_order_lots={"2330": 20, "2317": 3})
    assert state.thresholds == {"2330": 20, "2317": 3}
    assert state.snapshot("2330")["large_order_lots"] == 20
    assert state.snapshot("2317")["large_order_lots"] == 3


def test_thresholds_mapping_must_cover_every_symbol():
    with pytest.raises(KeyError):
        MarketState(["2330", "2317"], large_order_lots={"2330": 20})


def test_thresholds_property_returns_a_copy():
    state = MarketState(["2330"], large_order_lots=5)
    state.thresholds["2330"] = 42
    assert state.thresholds == {"2330": 5}


def test_set_threshold_for_one_symbol_leaves_the_others_alone():
    state = MarketState(["2330", "2317"], large_order_lots=5)
    state.set_threshold(20, "2330")
    assert state.thresholds == {"2330": 20, "2317": 5}
    assert state.snapshot("2317")["large_order_lots"] == 5


def test_set_threshold_for_untracked_symbol_raises():
    state = MarketState(["2330"], large_order_lots=5)
    with pytest.raises(KeyError):
        state.set_threshold(20, "9999")


def test_threshold_endpoint_can_target_a_single_symbol():
    state, client = build(threshold=10)
    state.aggregator("2330").add_trade(AT_ASK)          # 2 張

    body = client.post("/api/threshold",
                       json={"large_order_lots": 2, "symbol": "2330"}).json()
    assert body["thresholds"] == {"2330": 2, "2317": 10}
    assert state.snapshot("2330")["large_ladder"][0]["buy_lots"] == 2
    assert state.snapshot("2317")["large_order_lots"] == 10


def test_threshold_endpoint_for_untracked_symbol_is_404():
    _, client = build()
    response = client.post("/api/threshold",
                           json={"large_order_lots": 5, "symbol": "9999"})
    assert response.status_code == 404


def test_threshold_change_of_one_symbol_publishes_only_that_symbol():
    state = MarketState(["2330", "2317"], large_order_lots=5)
    broadcaster = RecordingBroadcaster()
    client = TestClient(create_app(state, broadcaster))
    client.post("/api/threshold", json={"large_order_lots": 20, "symbol": "2317"})
    assert broadcaster.published == ["2317"]


class RecordingBroadcaster(Broadcaster):
    def __init__(self) -> None:
        super().__init__()
        self.published: list[str] = []

    def publish(self, symbol: str) -> None:
        self.published.append(symbol)
        super().publish(symbol)


def test_websocket_init_carries_every_threshold():
    state = MarketState(["2330", "2317"],
                        large_order_lots={"2330": 20, "2317": 3})
    client = TestClient(create_app(state, Broadcaster()))
    with client.websocket_connect("/ws") as ws:
        first = ws.receive_json()
    assert first["thresholds"] == {"2330": 20, "2317": 3}


# -- 事件迴圈 -------------------------------------------------------------

class LoopWatchingState(MarketState):
    """記錄快照是在事件迴圈的執行緒上取的，還是在工作執行緒上。"""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.taken_on_event_loop: list[str] = []

    def _note(self, what: str) -> None:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return
        self.taken_on_event_loop.append(what)

    def snapshot(self, symbol: str) -> dict:
        self._note(f"snapshot:{symbol}")
        return super().snapshot(symbol)

    def snapshot_all(self) -> dict:
        self._note("snapshot_all")
        return super().snapshot_all()


def test_websocket_never_takes_the_lock_on_the_event_loop():
    """snapshot 會取 threading.Lock，而門檻重算持鎖（實測單檔 5 萬筆 27 ms）。
    在事件迴圈上等這把鎖會讓所有連線與所有 HTTP 請求一起停擺。"""
    state = LoopWatchingState(["2330"], large_order_lots=5)
    broadcaster = Broadcaster()
    with TestClient(create_app(state, broadcaster)) as client:
        with client.websocket_connect("/ws") as ws:
            assert ws.receive_json()["type"] == "init"
            broadcaster.publish("2330")
            assert ws.receive_json()["type"] == "update"
    assert state.taken_on_event_loop == []


def test_index_page_is_served():
    _, client = build()
    response = client.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]


# -- 名稱與五檔訂閱旗標 ---------------------------------------------------

def test_snapshot_carries_the_stock_name():
    state = MarketState(["2330", "2317"], large_order_lots=5,
                        names={"2330": "台積電"})
    assert state.snapshot("2330")["name"] == "台積電"
    assert state.snapshot("2317")["name"] == ""      # 取不到名稱不得炸掉


def test_snapshot_reports_whether_the_book_is_subscribed():
    """訂閱預算有限，五檔是選配 —— 畫面要能分辨「沒有買賣盤」與「沒訂」。"""
    state = MarketState(["2330", "2317"], large_order_lots=5,
                        book_symbols=["2330"])
    assert state.snapshot("2330")["has_book"] is True
    assert state.snapshot("2317")["has_book"] is False


def test_book_subscription_defaults_to_none():
    state = MarketState(["2330"], large_order_lots=5)
    assert state.snapshot("2330")["has_book"] is False


# -- 靜態頁面 -------------------------------------------------------------

def test_index_page_drops_the_per_trade_large_order_table():
    assert "近期大單明細" not in INDEX_HTML
    assert "近期成交明細" in INDEX_HTML      # 一般成交明細仍在


def test_index_page_uses_the_validated_chart_palette():
    """配色經 dataviz 驗證器兩模式全項通過，不可自行更換。"""
    assert "#e34948" in INDEX_HTML          # 淺色底：買
    assert "#e66767" in INDEX_HTML          # 深色底：買
    assert "#008300" in INDEX_HTML          # 兩模式：賣
    assert "prefers-color-scheme: dark" in INDEX_HTML
    assert ':root[data-theme="dark"]' in INDEX_HTML


def test_index_page_chart_labels_both_axes_and_handles_empty_data():
    assert "大單張數" in INDEX_HTML          # 左軸單位
    assert "股價" in INDEX_HTML              # 右軸單位
    assert "尚無大單" in INDEX_HTML          # 空資料狀態


def test_index_page_pulls_no_external_resource():
    """看盤畫面不得對外連線：沒有 CDN、沒有外部字型、沒有外部圖片。"""
    for token in ("http://", "https://", "//cdn", "<script src", "<link"):
        assert token not in INDEX_HTML, token


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

    state = MarketState(["2330"], large_order_lots=1)
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
        state.set_threshold(1)
    finally:
        stop.set()
        thread.join()

    snapshot = state.snapshot("2330")
    fields = {"buy": "buy_lots", "sell": "sell_lots",
              "auction": "auction_lots", "unknown": "unknown_lots"}
    trades = state.aggregator("2330").trades
    large = [t for t in trades if t["lots"] >= snapshot["large_order_lots"]]
    expected: dict = {}
    for trade in large:
        key = (trade["price"], fields[trade["side"]])
        expected[key] = expected.get(key, 0) + trade["lots"]
    actual = {(row["price"], field): row[field]
              for row in snapshot["large_ladder"]
              for field in fields.values() if row[field]}
    assert actual == expected

    # 桶在同一把鎖下與大單階梯一起重算，同樣不得重複計數
    assert sum(b["buy_lots"] + b["sell_lots"] for b in snapshot["buckets"]) == \
        sum(t["lots"] for t in large)


# -- /ws 的關閉中斷（C） ----------------------------------------------------
#
# 這裡刻意不用 TestClient.websocket_connect：它在背景執行緒的 anyio portal
# 上跑，結束連線時是「送 disconnect 訊息、幾乎同時 cancel 整個 scope」，時序
# 不受測試控制。直接手刻 ASGI receive/send 佇列，才能精準卡在「連線已訂閱、
# 但 disconnect 訊息還沒送達」這個時間點來斷言。

def _drive_ws_app(app):
    """手動握手一個 /ws 連線，回傳 (task, incoming, sent)。

    incoming 是我們餵給 app 的 ASGI 訊息佇列（app 呼叫 websocket.receive()
    時會從這裡拿）；sent 收集 app 送出的每一則 ASGI 訊息。
    """
    incoming: asyncio.Queue = asyncio.Queue()
    sent: list[dict] = []

    async def receive():
        return await incoming.get()

    async def send(message):
        sent.append(message)

    scope = {
        "type": "websocket", "path": "/ws", "raw_path": b"/ws", "root_path": "",
        "scheme": "ws", "query_string": b"", "headers": [],
        "client": ("test", 1234), "server": ("test", 80),
        "subprotocols": [], "state": {}, "extensions": {},
    }
    task = asyncio.ensure_future(app(scope, receive, send))
    return task, incoming, sent


async def _handshake(incoming, sent):
    """送出 websocket.connect 並等到 accept + init 快照都送出。"""
    await incoming.put({"type": "websocket.connect"})
    for _ in range(200):
        if len(sent) >= 2:
            break
        await asyncio.sleep(0.01)
    assert sent and sent[0]["type"] == "websocket.accept", "應先完成 accept 交握"
    assert len(sent) >= 2, "accept 之後應立即送出 init 快照"


def test_ws_ends_and_unsubscribes_when_client_disconnects():
    """實測缺陷：stream 只送不收，永遠卡在 await queue.get()，瀏覽器行情安靜
    時 uvicorn 的優雅關閉（透過送 websocket.disconnect 訊息）永遠等不到協程
    結束，只能 SIGKILL，finally 沒跑、緩衝區的成交全部遺失。

    這裡直接送一則 disconnect 訊息（不透過 queue）：舊代碼從不呼叫
    websocket.receive()，看不到這則訊息，協程會一直卡著，下面的
    asyncio.wait_for 會逾時而讓測試失敗——精確重現這個缺陷。
    """
    state = MarketState(["2330"], large_order_lots=5)
    broadcaster = Broadcaster()
    app = create_app(state, broadcaster)

    async def scenario():
        broadcaster.bind_loop(asyncio.get_running_loop())
        task, incoming, sent = _drive_ws_app(app)
        await _handshake(incoming, sent)
        assert len(broadcaster._queues) == 1, "交握完成後應已訂閱"

        await incoming.put({"type": "websocket.disconnect", "code": 1000})
        await asyncio.wait_for(task, timeout=2)   # 舊代碼會在這裡逾時
        assert broadcaster._queues == set(), "斷線後訂閱必須被移除"

    asyncio.run(scenario())


def test_ws_still_delivers_a_pending_update_when_it_races_the_disconnect():
    """queue.get() 與 websocket.receive() 用 asyncio.wait(FIRST_COMPLETED) 同時
    等待；即使兩者剛好同一輪都完成，已經從佇列取出的更新也不能被默默丟掉。"""
    state = MarketState(["2330"], large_order_lots=5)
    broadcaster = Broadcaster()
    app = create_app(state, broadcaster)

    async def scenario():
        broadcaster.bind_loop(asyncio.get_running_loop())
        task, incoming, sent = _drive_ws_app(app)
        await _handshake(incoming, sent)

        broadcaster.publish("2330")
        await incoming.put({"type": "websocket.disconnect", "code": 1000})
        await asyncio.wait_for(task, timeout=2)

        kinds = [m.get("type") for m in sent]
        assert kinds.count("websocket.send") >= 2, "更新快照不能因為同時斷線就被丟掉"

    asyncio.run(scenario())


def test_ws_cancellation_still_unsubscribes_without_leaking_tasks():
    """伺服器關閉時協程可能直接被 cancel（而非先收到 disconnect 訊息）。
    asyncio.wait 被取消不會連帶取消傳入的 get_task／recv_task，沒有明確處理
    就會每個 tick 洩漏一個 task；同時 finally 的 unsubscribe 仍必須執行。"""
    state = MarketState(["2330"], large_order_lots=5)
    broadcaster = Broadcaster()
    app = create_app(state, broadcaster)

    async def scenario():
        broadcaster.bind_loop(asyncio.get_running_loop())
        task, incoming, sent = _drive_ws_app(app)
        await _handshake(incoming, sent)
        assert len(broadcaster._queues) == 1

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=2)
        assert broadcaster._queues == set(), "cancel 後也必須移除訂閱"

    asyncio.run(scenario())
