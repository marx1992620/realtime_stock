import asyncio
import sys
import types
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.web import Broadcaster, MarketState, create_app

INDEX_HTML = (Path(__file__).resolve().parents[1]
              / "app" / "static" / "index.html").read_text(encoding="utf-8")


AT_ASK = {"symbol": "2330", "price": 2405, "size": 2, "bid": 2400, "ask": 2405,
          "time": 1785902209312276, "serial": 1}


def build(symbols=("2330", "2317"), threshold=5):
    state = MarketState(list(symbols), large_order_lots=threshold)
    return state, TestClient(create_app(state, Broadcaster()))


def test_symbols_endpoint_lists_tracked_symbols():
    _, client = build()
    body = client.get("/api/symbols").json()
    assert body["symbols"] == ["2330", "2317"]
    assert body["thresholds"] == {"2330": 5, "2317": 5}


def test_snapshot_endpoint_returns_ladder():
    state, client = build()
    state.aggregator("2330").add_trade(AT_ASK)

    body = client.get("/api/snapshot/2330").json()
    assert body["last_price"] == 2405
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


# -- 行情健康度（Task 13 C）-------------------------------------------------
#
# 無聲失敗比失敗本身更糟：feed 傳進 create_app 之後，GET /api/feed 與 ws 的
# init／update 訊息都要能問到目前的連線健康度，不必再靠「時間戳有沒有動」
# 這種脆弱訊號去猜資料還新不新鮮。


class FakeFeed:
    def __init__(self, state="connected", reconnects=0, last_message_ago=1.5,
                 subscription_count=1, reject_symbols=()):
        self._payload = {"state": state, "last_message_ago": last_message_ago,
                         "reconnects": reconnects, "subscription_errors": []}
        # Task 14：新增／移除標的的假物件，讓 test_web.py 能驗證端點呼叫到
        # feed 而不必接一個真的 FugleFeed（測試不得動到那個正在跑的實例）。
        self.subscription_count = subscription_count
        self.added: list[str] = []
        self.removed: list[str] = []
        # Task 16 B：模擬伺服器拒絕訂閱——add_symbol 同步標記結果，讓
        # web.py 的等待迴圈不必真的等（第一次檢查就拿到答案）。
        self._reject = set(reject_symbols)
        self.subscribed_symbols: set[str] = set()
        self.failed_subscriptions: set[str] = set()

    def status(self) -> dict:
        return dict(self._payload)

    def add_symbol(self, symbol: str) -> None:
        self.added.append(symbol)
        self.subscription_count += 1
        if symbol in self._reject:
            self.failed_subscriptions.add(symbol)
        else:
            self.subscribed_symbols.add(symbol)

    def remove_symbol(self, symbol: str) -> None:
        self.removed.append(symbol)
        self.subscription_count -= 1
        self.subscribed_symbols.discard(symbol)
        self.failed_subscriptions.discard(symbol)


def test_feed_status_is_exposed_via_rest_and_ws_messages():
    """Task 19：/api/feed 與 ws 的 "feed" 欄位改成兩段式
    {"stock": ..., "futures": ...}——股票與期貨是兩條獨立連線，健康度不能
    合併成一個欄位，不然使用者分不出是哪一條斷了。"""
    state = MarketState(["2330"], large_order_lots=5)
    feed = FakeFeed(state="reconnecting", reconnects=2)
    client = TestClient(create_app(state, Broadcaster(), feed))

    body = client.get("/api/feed").json()
    assert body["stock"] == {"state": "reconnecting", "last_message_ago": 1.5,
                             "reconnects": 2, "subscription_errors": [],
                             "subscription_count": 1, "subscription_limit": 5}
    # 沒有接 futures_feed（create_app 的選配參數）視為停用，不是「還沒連上」。
    assert body["futures"]["state"] == "disabled"

    with client.websocket_connect("/ws") as ws:
        first = ws.receive_json()
    assert first["feed"]["stock"]["state"] == "reconnecting"
    assert first["feed"]["stock"]["reconnects"] == 2
    assert first["feed"]["futures"]["state"] == "disabled"


def test_feed_status_defaults_to_stopped_when_no_feed_given():
    """create_app 的 feed 參數是選配的（許多測試不需要真的接一個 FugleFeed
    進來）；沒給時 /api/feed 仍要回一個合理的預設值，不是少個欄位或整個 500。"""
    _, client = build()
    body = client.get("/api/feed").json()
    assert body["stock"]["state"] == "stopped"
    assert body["futures"]["state"] == "disabled"


# -- 期貨：獨立的唯讀端點，不與股票的快照／門檻管道混在一起 -----------------

class FakePoller:
    """期交所 MIS 輪詢器的最小替身（app.futures.TaifexFuturesPoller）。"""

    def __init__(self, quote=None, state="connected"):
        self._quote = quote
        self._state = state

    def snapshot(self):
        return self._quote

    def status(self):
        return {"state": self._state, "last_message_ago": 2.0, "reconnects": 0,
                "unavailable_reason": None, "last_error": None,
                "symbol": "TXFH6-F", "name": "臺指期086"}


def test_futures_endpoint_returns_the_quote_and_its_own_status():
    """期貨不再是第二份 MarketState：它沒有逐筆成交可以聚合，形狀與股票
    快照完全不同，所以走自己的 GET /api/futures，而不是擠進
    /api/snapshot/{symbol}（見 app/web.py 的 create_app 說明）。"""
    quote = {"symbol": "TXFH6-F", "name": "臺指期086", "last_price": 46010.0,
             "candles": [{"date": "08:46", "open": 1, "high": 2, "low": 0, "close": 1,
                          "volume": 10}], "ma": {"5": [None]}, "bar_unit": "分"}
    state = MarketState(["2330"], large_order_lots=5)
    client = TestClient(create_app(state, Broadcaster(), futures_poller=FakePoller(quote)))

    body = client.get("/api/futures").json()
    assert body["enabled"] is True
    assert body["quote"]["last_price"] == 46010.0
    assert body["status"]["state"] == "connected"
    assert client.get("/api/feed").json()["futures"]["state"] == "connected"

    # 期貨代碼不在股票那份 state 裡，股票專屬的端點要乾脆地回 404，
    # 不可以回一個看起來像有資料的空快照。
    assert client.get("/api/snapshot/TXFH6-F").status_code == 404
    with client.websocket_connect("/ws") as ws:
        first = ws.receive_json()
    assert list(first["snapshots"]) == ["2330"]


def test_futures_endpoint_reports_disabled_when_no_poller_is_attached():
    """--futures "" 停用時畫面要說得出「未啟用」，而不是留一塊空白面板
    或當成「連線中」讓使用者一直等。"""
    _, client = build()
    body = client.get("/api/futures").json()
    assert body == {"enabled": False, "quote": None,
                    "status": {"state": "disabled", "last_message_ago": None,
                               "reconnects": 0, "unavailable_reason": None,
                               "last_error": None, "symbol": None, "name": None}}


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


# -- 名稱 ------------------------------------------------------------------

def test_snapshot_carries_the_stock_name():
    state = MarketState(["2330", "2317"], large_order_lots=5,
                        names={"2330": "台積電"})
    assert state.snapshot("2330")["name"] == "台積電"
    assert state.snapshot("2317")["name"] == ""      # 取不到名稱不得炸掉


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


# -- Task 14：動態增刪監控標的 -----------------------------------------------
#
# 測試從簡：每個端點一個測試即可，重點是功能會動，不窮舉邊界。

class RecordingPipeline:
    """假的 writer 管理者：只記錄呼叫過誰，不真的碰檔案系統。"""

    def __init__(self) -> None:
        self.added: list[str] = []
        self.removed: list[str] = []

    def add_writer(self, symbol: str) -> None:
        self.added.append(symbol)

    def remove_writer(self, symbol: str) -> None:
        self.removed.append(symbol)


def test_post_symbols_adds_a_new_symbol_and_subscribes_it_on_the_feed():
    state = MarketState(["2330"], large_order_lots=5)
    feed = FakeFeed(subscription_count=1)
    pipeline = RecordingPipeline()
    client = TestClient(create_app(state, Broadcaster(), feed, pipeline=pipeline,
                                   lookup_name=lambda symbol: "聯發科",
                                   default_large_order_lots=10))

    response = client.post("/api/symbols", json={"symbol": "2454"})

    assert response.status_code == 200
    assert response.json()["symbols"] == ["2330", "2454"]
    assert state.snapshot("2454")["name"] == "聯發科"      # 名稱有帶到
    assert feed.added == ["2454"]                          # feed 收到訂閱呼叫
    assert pipeline.added == ["2454"]                       # writer 也建立了


def test_post_symbols_over_budget_is_409():
    state = MarketState(["2330"], large_order_lots=5)
    feed = FakeFeed(subscription_count=5)          # 已經滿額（上限 5）

    def must_not_be_called(symbol):
        raise AssertionError("預算超過就該擋下，不該再去查名稱")

    client = TestClient(create_app(state, Broadcaster(), feed,
                                   lookup_name=must_not_be_called,
                                   default_large_order_lots=10))

    response = client.post("/api/symbols", json={"symbol": "2454"})

    assert response.status_code == 409
    assert "5" in response.json()["detail"]
    assert "2454" not in state.symbols
    assert feed.added == []


def test_post_symbols_unknown_symbol_from_rest_is_404():
    """REST 查不到名稱視為打錯代碼，比訂閱送出去才發現沒資料好。"""
    state = MarketState(["2330"], large_order_lots=5)
    feed = FakeFeed(subscription_count=1)
    client = TestClient(create_app(state, Broadcaster(), feed,
                                   lookup_name=lambda symbol: None,
                                   default_large_order_lots=10))

    response = client.post("/api/symbols", json={"symbol": "9999"})

    assert response.status_code == 404
    assert "9999" not in state.symbols
    assert feed.added == []


# -- Task 16 B：訂閱被拒不可靜默留在清單 --------------------------------------

def test_post_symbols_rolls_back_when_subscription_is_rejected():
    """實測缺陷：訂閱被伺服器拒絕（Subscription limit exceeded）過去會
    靜默留在追蹤清單裡，畫面看起來正常但永遠不會有資料。POST /api/symbols
    必須等到 feed 確認訂閱，被標記失敗（或逾時）就整段回滾：聚合器、
    writer、feed 內部的訂閱嘗試都要清乾淨，不留下任何痕跡。"""
    state = MarketState(["2330"], large_order_lots=5)
    feed = FakeFeed(subscription_count=1, reject_symbols={"2454"})
    pipeline = RecordingPipeline()
    client = TestClient(create_app(state, Broadcaster(), feed, pipeline=pipeline,
                                   lookup_name=lambda symbol: "聯發科",
                                   default_large_order_lots=10))

    response = client.post("/api/symbols", json={"symbol": "2454"})

    assert response.status_code == 409
    assert "2454" not in state.symbols
    assert pipeline.removed == ["2454"], "writer 必須被關閉，不然緩衝資料遺失"
    assert feed.removed == ["2454"], "feed 內部的訂閱嘗試也要回滾，不然佔用訂閱數"
    with pytest.raises(KeyError):
        state.aggregator("2454")


def test_delete_symbols_removes_symbol_closes_writer_and_unsubscribes():
    state = MarketState(["2330", "2317"], large_order_lots=5)
    feed = FakeFeed(subscription_count=2)
    pipeline = RecordingPipeline()
    client = TestClient(create_app(state, Broadcaster(), feed, pipeline=pipeline))

    response = client.delete("/api/symbols/2317")

    assert response.status_code == 200
    assert response.json()["symbols"] == ["2330"]           # 清單變短
    assert feed.removed == ["2317"]                          # feed 收到退訂呼叫
    assert pipeline.removed == ["2317"]                      # writer 被關閉（close 在 Pipeline.remove_writer 內）
    with pytest.raises(KeyError):
        state.aggregator("2317")


# -- GET /api/history/{symbol}（Task 17）-----------------------------------

def fake_candles_module(monkeypatch, candles):
    """比照 test_history.py：換掉整個 fugle_marketdata 模組，
    GET /api/history 內部經 app.history.fetch_daily_candles 用到它，
    不必真的打 API。"""
    sdk = types.ModuleType("fugle_marketdata")

    class RestClient:
        def __init__(self, **kwargs):
            self.stock = types.SimpleNamespace(
                historical=types.SimpleNamespace(candles=candles))

    sdk.RestClient = RestClient
    monkeypatch.setitem(sys.modules, "fugle_marketdata", sdk)


def _raw_candle(date, close):
    return {"date": date, "open": close, "high": close, "low": close,
            "close": close, "volume": 1000}


def test_get_history_returns_candles_and_moving_averages(monkeypatch):
    calls = []

    def candles(symbol, **params):
        calls.append(params)
        return {"data": [_raw_candle("2026-01-03", 103),
                         _raw_candle("2026-01-02", 102),
                         _raw_candle("2026-01-01", 101)]}

    fake_candles_module(monkeypatch, candles)
    state = MarketState(["2330"], large_order_lots=5)
    client = TestClient(create_app(state, Broadcaster(),
                                   lookup_name=lambda symbol: "台積電",
                                   history_api_key="fake-key"))

    response = client.get("/api/history/2330")

    assert response.status_code == 200
    body = response.json()
    assert body["symbol"] == "2330"
    assert body["name"] == "台積電"
    assert [c["date"] for c in body["candles"]] == ["2026-01-01", "2026-01-02", "2026-01-03"]
    assert set(body["ma"].keys()) == {"5", "10", "20", "60", "120"}
    assert len(body["ma"]["5"]) == 3
    # 預設抓的天數（DEFAULT_HISTORY_DAYS）超過單次請求上限，
    # fetch_daily_candles 內部本來就會分段成多次請求，這裡只確認真的打了 API。
    assert len(calls) >= 1


def test_get_history_of_untracked_symbol_still_works(monkeypatch):
    """純 REST，不佔訂閱配額——未追蹤的代碼也允許查詢（例如指數 IX0001）。"""
    fake_candles_module(monkeypatch, lambda symbol, **params:
                        {"data": [_raw_candle("2026-01-01", 100)]})
    state = MarketState(["2330"], large_order_lots=5)
    client = TestClient(create_app(state, Broadcaster(),
                                   lookup_name=lambda symbol: "發行量加權股價指數",
                                   history_api_key="fake-key"))

    response = client.get("/api/history/IX0001")

    assert response.status_code == 200
    assert response.json()["symbol"] == "IX0001"
    assert "IX0001" not in state.symbols


def test_get_history_of_invalid_symbol_is_404():
    state = MarketState(["2330"], large_order_lots=5)
    client = TestClient(create_app(state, Broadcaster(),
                                   lookup_name=lambda symbol: None,
                                   history_api_key="fake-key"))

    assert client.get("/api/history/9999").status_code == 404


def test_get_history_second_request_uses_cache_not_api(monkeypatch):
    calls = []

    def candles(symbol, **params):
        calls.append(params)
        return {"data": [_raw_candle("2026-01-01", 100)]}

    fake_candles_module(monkeypatch, candles)
    state = MarketState(["2330"], large_order_lots=5)
    client = TestClient(create_app(state, Broadcaster(),
                                   lookup_name=lambda symbol: "台積電",
                                   history_api_key="fake-key"))

    first = client.get("/api/history/2330")
    calls_after_first = len(calls)
    second = client.get("/api/history/2330")

    assert first.status_code == second.status_code == 200
    assert first.json() == second.json()
    assert calls_after_first > 0, "第一次請求要真的打到 API"
    assert len(calls) == calls_after_first, "第二次請求應該走快取，不重打 API"


def test_get_history_without_api_key_configured_is_500():
    state = MarketState(["2330"], large_order_lots=5)
    client = TestClient(create_app(state, Broadcaster(),
                                   lookup_name=lambda symbol: "台積電"))
    assert client.get("/api/history/2330").status_code == 500


class FakeDailyPoller(FakePoller):
    """帶 product 的替身：/api/futures/daily 用它決定要查哪個商品。"""
    product = "TXF"


def test_futures_daily_endpoint_serves_the_cached_daily_candles(monkeypatch):
    """日 K 與即時報價分成兩支端點：日 K 一天只變一次、一份約 250 根，
    塞進每 5 秒輪詢的 /api/futures 等於每小時重傳 720 遍同樣的東西。"""
    import app.web as web
    calls = []
    monkeypatch.setattr(web.DailyCandleCache, "get",
                        lambda self, product, *a, **k: calls.append(product) or {
                            "symbol": product, "name": "TXF 連續近月", "bar_unit": "日",
                            "candles": [{"date": "2026-08-12", "open": 45350.0,
                                         "high": 45561.0, "low": 45156.0,
                                         "close": 45528.0, "volume": 48409.0}],
                            "ma": {"5": [None]}})
    state = MarketState(["2330"], large_order_lots=5)
    client = TestClient(create_app(state, Broadcaster(),
                                   futures_poller=FakeDailyPoller()))

    body = client.get("/api/futures/daily").json()
    assert calls == ["TXF"]
    assert body["bar_unit"] == "日"
    assert body["candles"][0]["close"] == 45528.0


def test_futures_daily_endpoint_404s_when_futures_is_disabled():
    _, client = build()
    assert client.get("/api/futures/daily").status_code == 404


# -- 關閉時的收尾（實地事故）-------------------------------------------------
#
# 實測 uvicorn 0.52.1：收到 SIGTERM 之後它會做完整的優雅關閉（Shutting down →
# lifespan shutdown → Finished server process），但**不會從 uvicorn.run() 返回**
# ——其後的 finally 不執行，atexit 也不執行。收尾原本掛在 main() 的 finally，
# 於是 ParquetTradeWriter.close() 從來沒被呼叫過，每個 parquet 都停在沒有 footer
# 的 .partial：2026-08-12、08-13 兩個交易日的逐筆資料因此整批讀不出來。
#
# 唯一在 SIGTERM 下還活著的鉤子是 lifespan shutdown，收尾必須掛在那裡。

def test_lifespan_shutdown_stops_feeds_then_closes_writers():
    calls = []
    feed = types.SimpleNamespace(stop=lambda: calls.append("feed"))
    poller = types.SimpleNamespace(stop=lambda: calls.append("futures"))
    pipeline = types.SimpleNamespace(close=lambda: calls.append("pipeline"))

    app = create_app(MarketState(["2330"], large_order_lots=5), Broadcaster(),
                     feed, pipeline=pipeline, futures_poller=poller)
    with TestClient(app):
        assert calls == [], "還在服務中就不該收尾"

    assert calls == ["feed", "futures", "pipeline"], (
        "順序必須是先停行情、再關 writer——反過來的話，重連迴圈或補資料還可能"
        f"在 close() 之後繼續寫進已關閉的檔案：{calls}")


def test_lifespan_shutdown_works_without_the_optional_collaborators():
    """feed／pipeline／futures_poller 都是選配的（測試常常不給），關閉時
    不能因為它們是 None 就炸掉——那會讓整個 app 連乾淨結束都做不到。"""
    app = create_app(MarketState(["2330"], large_order_lots=5), Broadcaster())
    with TestClient(app):
        pass
