"""單一連線約束與訂閱時序。

實測事實：
- 一把 API key 只能開一條 WebSocket 連線，超過會被以 close frame
  "Maximum number of connections reached" 斷線。
- SDK 在 connect 事件內送出 auth frame 但不等回覆，因此必須等
  authenticated 事件才能訂閱，否則被拒為 "Forbidden resource"。
- 退訂必須帶伺服器在 subscribed 事件裡給的 id：unsubscribe 帶
  {"channel": ..., "symbol": ...} 會被拒為 "id should not be empty"，
  且訂閱槽永遠不會釋放，之後任何新訂閱都會撞上 Subscription limit exceeded。
"""

import json
import sys
import threading
import time
import types

import pytest

from app.feed import FugleFeed


class FakeStock:
    def __init__(self, timeline):
        self.timeline = timeline
        self.handlers = {}
        self.subscriptions = []
        # 退訂實測必須帶伺服器給的 id（{"id": ...}），不是 channel+symbol；
        # 這裡原樣記錄送出的參數，讓測試能斷言送的是哪一種形狀。
        self.unsubscribe_calls: list[dict] = []
        # 讓測試能精準等到「fake 已經跑完 connect+subscribe」，不必用 sleep 賭時序。
        self.connected = threading.Event()

    def on(self, event, listener):
        self.handlers[event] = listener

    def subscribe(self, params):
        self.subscriptions.append(params)
        self.timeline.append(("subscribe", params["channel"], params["symbol"]))

    def unsubscribe(self, params):
        self.unsubscribe_calls.append(dict(params))
        self.timeline.append(("unsubscribe", dict(params)))

    def connect(self):
        self.timeline.append(("auth_frame_sent",))
        self.handlers["connect"]()
        self.timeline.append(("server_authenticated",))
        self.handlers["authenticated"]('{"event":"authenticated","data":{}}')
        self.connected.set()

    def disconnect(self):
        self.timeline.append(("disconnect",))
        handler = self.handlers.get("disconnect")
        if handler:
            handler(None, None)


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
    sdk.HealthCheckConfig = lambda **kwargs: kwargs
    monkeypatch.setitem(sys.modules, "fugle_marketdata", sdk)
    certifi = types.ModuleType("certifi")
    certifi.where = lambda: "/dev/null"
    monkeypatch.setitem(sys.modules, "certifi", certifi)
    return types.SimpleNamespace(stock=stock, timeline=timeline, created=created)


def run_briefly(feed: FugleFeed, stock: FakeStock) -> None:
    """run() 現在是重連迴圈，不會自己返回；背景跑，等 fake 做完一次
    connect+subscribe 後呼叫 stop()，確認迴圈確實乾淨結束。"""
    thread = threading.Thread(target=feed.run, daemon=True)
    thread.start()
    assert stock.connected.wait(timeout=2), "連線流程沒有在時限內完成"
    feed.stop()
    thread.join(timeout=2)
    assert not thread.is_alive(), "feed.run() 沒有在 stop() 後結束"


def test_uses_exactly_one_connection_for_all_symbols(fake_sdk):
    feed = FugleFeed("k", ["2330", "2317", "2454"], lambda t: None)
    run_briefly(feed, fake_sdk.stock)
    assert len(fake_sdk.created) == 1, "API key 只允許一條連線"


def test_subscribes_trades_only(fake_sdk):
    """一條連線最多 5 個訂閱（實測第 6 個回 Subscription limit exceeded）。
    Task 16 起五檔報價功能已移除，訂閱數就等於追蹤的代碼數。"""
    feed = FugleFeed("k", ["2330", "2317"], lambda t: None)
    run_briefly(feed, fake_sdk.stock)
    subscribed = {(s["channel"], s["symbol"]) for s in fake_sdk.stock.subscriptions}
    assert subscribed == {("trades", "2330"), ("trades", "2317")}


def test_subscription_limit_error_is_recorded_not_just_printed(fake_sdk, capsys):
    """超額訂閱目前只印一行 stderr 就繼續跑，那些股票整天沒有資料，
    使用者無從察覺 —— 必須留下可供上層顯示的紀錄。"""
    feed = FugleFeed("k", ["2330"], lambda t: None)
    feed.handle_message(json.dumps({
        "event": "error", "data": {"message": "Subscription limit exceeded"}}))
    assert feed.subscription_errors == ["Subscription limit exceeded"]
    assert "Subscription limit exceeded" in capsys.readouterr().err


def test_other_api_errors_are_not_counted_as_subscription_errors(fake_sdk, capsys):
    feed = FugleFeed("k", ["2330"], lambda t: None)
    feed.handle_message(json.dumps({
        "event": "error", "data": {"message": "Forbidden resource"}}))
    assert feed.subscription_errors == []
    assert "Forbidden resource" in capsys.readouterr().err


def test_all_subscriptions_happen_after_authentication(fake_sdk):
    feed = FugleFeed("k", ["2330", "2317"], lambda t: None)
    run_briefly(feed, fake_sdk.stock)
    auth_at = fake_sdk.timeline.index(("server_authenticated",))
    first_sub = min(i for i, e in enumerate(fake_sdk.timeline) if e[0] == "subscribe")
    assert first_sub > auth_at, f"訂閱早於認證會被拒為 Forbidden resource: {fake_sdk.timeline}"


def test_trade_events_reach_the_trade_callback(fake_sdk):
    trades = []
    feed = FugleFeed("k", ["2330"], trades.append)
    feed.handle_message(json.dumps({
        "event": "data", "channel": "trades",
        "data": {"symbol": "2330", "price": 2405, "size": 1, "bid": 2400,
                 "ask": 2405, "time": 1, "serial": 7},
    }))
    assert [t["serial"] for t in trades] == [7]


def test_trial_matches_are_skipped_by_default(fake_sdk):
    trades = []
    feed = FugleFeed("k", ["2330"], trades.append)
    feed.handle_message(json.dumps({
        "event": "data", "channel": "trades",
        "data": {"symbol": "2330", "price": 2385, "size": 2113, "isTrial": True,
                 "time": 3, "serial": 8},
    }))
    assert trades == []


def test_trial_matches_kept_when_requested(fake_sdk):
    trades = []
    feed = FugleFeed("k", ["2330"], trades.append, include_trials=True)
    feed.handle_message(json.dumps({
        "event": "data", "channel": "trades",
        "data": {"symbol": "2330", "price": 2385, "size": 2113, "isTrial": True,
                 "time": 3, "serial": 8},
    }))
    assert [t["serial"] for t in trades] == [8]


def test_untracked_symbol_is_ignored(fake_sdk):
    trades = []
    feed = FugleFeed("k", ["2330"], trades.append)
    feed.handle_message(json.dumps({
        "event": "data", "channel": "trades",
        "data": {"symbol": "2454", "price": 1000, "size": 1, "bid": 999,
                 "ask": 1000, "time": 4, "serial": 9},
    }))
    assert trades == []


TRADE_EVENT = json.dumps({
    "event": "data", "channel": "trades",
    "data": {"symbol": "2330", "price": 2405, "size": 1, "bid": 2400,
             "ask": 2405, "time": 5, "serial": 10},
})


def test_downstream_failure_is_not_reported_as_a_bad_message(fake_sdk, capsys):
    """pa.lib.ArrowInvalid 繼承自 ValueError：若回呼留在解析的 try 內，
    聚合／寫檔／廣播的失敗會被當成「訊息壞掉」吞掉，畫面照常更新
    而資料早已停止落檔。"""
    import pyarrow as pa

    def explode(_trade):
        raise pa.lib.ArrowInvalid("Parquet magic bytes not found in footer")

    feed = FugleFeed("k", ["2330"], explode)
    feed.handle_message(TRADE_EVENT)

    err = capsys.readouterr().err
    assert "Trade callback failed" in err
    assert "ArrowInvalid" in err
    assert "Unable to process message" not in err, "下游失敗不可被誤報為訊息解析失敗"


def test_callback_failure_does_not_stop_the_feed(fake_sdk, capsys):
    seen = []
    calls = []

    def flaky(trade):
        calls.append(trade)
        if len(calls) == 1:
            raise RuntimeError("first one blows up")
        seen.append(trade)

    feed = FugleFeed("k", ["2330"], flaky)
    feed.handle_message(TRADE_EVENT)                       # 第一筆炸掉
    feed.handle_message(json.dumps({**json.loads(TRADE_EVENT)}))

    assert [t["serial"] for t in seen] == [10], "回呼失敗不得中斷行情迴圈"
    err = capsys.readouterr().err
    assert "Trade callback failed" in err


def test_trade_event_without_symbol_is_counted_as_dropped(fake_sdk, capsys):
    """沒有 symbol 的事件（最可能是開盤集合競價）若被靜默丟棄，
    auction_lots 全日為 0 卻沒有任何錯誤。"""
    trades = []
    feed = FugleFeed("k", ["2330"], trades.append)
    payload = {"price": 2385, "size": 2024, "volume": 2024,
               "time": 1785891605048734, "serial": 131115}
    feed.handle_message(json.dumps(
        {"event": "data", "channel": "trades", "data": payload}))

    assert trades == []
    assert feed.dropped_events["trades"] == 1
    err = capsys.readouterr().err
    assert "trades" in err and "symbol=None" in err
    assert "serial" in err, "應附上鍵名清單以便診斷"
    assert "2385" not in err, "不要印整個 payload"


def test_repeated_drops_are_counted_but_printed_once(fake_sdk, capsys):
    feed = FugleFeed("k", ["2330"], lambda t: None)
    event = json.dumps({"event": "data", "channel": "trades",
                        "data": {"price": 2385, "size": 1, "time": 1}})
    for _ in range(3):
        feed.handle_message(event)
    assert feed.dropped_events["trades"] == 3
    assert capsys.readouterr().err.count("Dropping") == 1


def test_unrecognised_channel_for_tracked_symbol_is_dropped_and_counted(fake_sdk):
    """symbol 過濾之後才判斷 channel。trades 以外的 channel（例如
    candles）目前直接 `return None`，跳過 `_note_dropped`，屬於計數器
    要消滅的那種看不見的丟棄——與沒有 symbol 的事件同一類問題。"""
    trades = []
    feed = FugleFeed("k", ["2330"], trades.append)
    feed.handle_message(json.dumps({
        "event": "data", "channel": "candles",
        "data": {"symbol": "2330", "open": 2400, "close": 2405, "time": 8},
    }))
    assert trades == []
    assert feed.dropped_events["candles"] == 1


def test_malformed_message_does_not_kill_the_feed(fake_sdk):
    trades = []
    feed = FugleFeed("k", ["2330"], trades.append)
    feed.handle_message("not json")                       # JSONDecodeError
    feed.handle_message(json.dumps([1, 2, 3]))             # top level not an object
    feed.handle_message(json.dumps({
        "event": "data", "channel": "trades", "data": [1, 2, 3],
    }))                                                    # "data" not an object
    feed.handle_message(json.dumps({
        "event": "data", "channel": "trades",
        "data": {"symbol": "2330", "price": 2405, "size": 1, "bid": 2400,
                 "ask": 2405, "time": 5, "serial": 10},
    }))
    assert [t["serial"] for t in trades] == [10]


# -- Task 13：連線韌性 ------------------------------------------------------
#
# 實地發生過的事：Mac 睡眠、TCP 半開死亡，程式沒有任何反應——沒有錯誤、
# 沒有重連、時間戳就這樣凍住，95 分鐘資料悄悄消失，使用者也無從分辨
# 「沒有大單」是行情清淡還是連線早就斷了。以下四個測試對應 A～D，
# 各證明一件事就好，不窮舉邊界。


class FailingStock:
    """每次 connect() 都立刻失敗（模擬連續認證失敗／連不上），
    用來證明重連迴圈會建立全新的 client，而且重試之間確實有退避等待。"""

    def __init__(self):
        self.handlers = {}

    def on(self, event, listener):
        self.handlers[event] = listener

    def subscribe(self, params):
        pass

    def connect(self):
        raise RuntimeError("auth failed")

    def disconnect(self):
        pass


def test_a_reconnects_with_a_new_client_and_backoff_after_repeated_failures(monkeypatch):
    """A：假的 SDK 讓 connect() 立即失敗（等同立即返回）兩次後停止——
    確認每次重試都建立了全新的 WebSocketClient，且重試之間有退避等待。"""
    created = []

    def make_client(**kwargs):
        stock = FailingStock()
        created.append(stock)
        return types.SimpleNamespace(stock=stock)

    sdk = types.ModuleType("fugle_marketdata")
    sdk.WebSocketClient = make_client
    sdk.HealthCheckConfig = lambda **kwargs: kwargs
    monkeypatch.setitem(sys.modules, "fugle_marketdata", sdk)
    certifi = types.ModuleType("certifi")
    certifi.where = lambda: "/dev/null"
    monkeypatch.setitem(sys.modules, "certifi", certifi)

    feed = FugleFeed("k", ["2330"], lambda t: None)

    waits = []

    def fake_wait(seconds):
        waits.append(seconds)
        if len(created) >= 2:
            feed.stop()                       # 兩次嘗試後停止，測試才會結束
        return feed._stop_event.is_set()

    feed._wait_before_reconnect = fake_wait
    feed.run()

    assert len(created) == 2, "每次重連都要是全新的 WebSocketClient，不能重用舊的"
    assert created[0] is not created[1]
    assert waits == [1.0, 2.0], f"退避秒數應該是 1s 之後翻倍成 2s：{waits}"
    assert feed.reconnects == 2
    assert feed.status()["state"] == "stopped"


def test_b_watchdog_disconnects_when_the_feed_goes_stale():
    """B：睡眠造成的半開連線不會回報關閉，只能自己盯 last_message_at。
    超過 stale_after_seconds 沒收到任何訊息（含 ping/pong 的 pong）就要
    主動呼叫 disconnect() 觸發 A 的重連；還新鮮時不該誤觸發。"""
    calls = []
    feed = FugleFeed("k", ["2330"], lambda t: None, stale_after_seconds=90)
    feed._stock = types.SimpleNamespace(disconnect=lambda: calls.append("disconnect"))

    feed.last_message_at = time.monotonic()           # 剛收到訊息
    feed._watchdog_tick()
    assert calls == [], "還新鮮就不該觸發"

    feed.last_message_at = time.monotonic() - 91       # 超過 90 秒沒收到任何訊息
    feed._watchdog_tick()
    assert calls == ["disconnect"], "閒置過久要主動斷線觸發重連"


def test_c_status_reports_state_freshness_and_reconnect_count():
    """C：status() 要能分辨連線中／重連中，並帶上距離上一則訊息多久。"""
    feed = FugleFeed("k", ["2330"], lambda t: None)
    assert feed.status()["state"] == "reconnecting"   # 還沒連上也算「還在嘗試連線」
    assert feed.status()["last_message_ago"] is None

    feed._state = "connected"
    feed.last_message_at = time.monotonic() - 5
    connected = feed.status()
    assert connected["state"] == "connected"
    assert connected["last_message_ago"] == pytest.approx(5, abs=0.5)
    assert connected["reconnects"] == 0

    feed._state = "reconnecting"
    feed.reconnects = 3
    reconnecting = feed.status()
    assert reconnecting["state"] == "reconnecting"
    assert reconnecting["reconnects"] == 3


def test_d_backfill_replays_missed_rest_trades_into_on_trade(monkeypatch):
    """D：重連後（reconnects > 0）要用 REST 補回缺漏的成交，逐筆送進
    on_trade；REST 逐筆資料沒有 symbol 欄位，補資料時要自己補上。"""
    seen = []
    feed = FugleFeed("k", ["2330"], seen.append)
    feed.reconnects = 1                                # 模擬已經重連過一次

    calls = []

    def fake_trades(**params):
        calls.append(params)
        if params["offset"] == 0:
            return {"symbol": "2330", "data": [
                {"price": 100, "size": 3, "time": 1, "serial": 501},
                {"price": 101, "size": 1, "time": 2, "serial": 502},
            ]}
        return {"symbol": "2330", "data": []}

    class RestClient:
        def __init__(self, **kwargs):
            self.stock = types.SimpleNamespace(
                intraday=types.SimpleNamespace(trades=fake_trades))

    sdk = types.ModuleType("fugle_marketdata")
    sdk.RestClient = RestClient
    monkeypatch.setitem(sys.modules, "fugle_marketdata", sdk)

    feed._backfill()

    assert [t["serial"] for t in seen] == [501, 502]
    assert all(t["symbol"] == "2330" for t in seen), "REST 沒有 symbol 欄位，要自己補上"
    assert calls[0] == {"symbol": "2330", "limit": 500, "offset": 0}


# -- Task 14 C：盤中動態增刪標的 ----------------------------------------------
#
# 測試從簡：一個測試證明重連會用「當下」的清單重新訂閱就好，不窮舉。

def test_reconnect_resubscribes_the_current_symbol_list_not_the_startup_one(fake_sdk):
    """add_symbol/remove_symbol 之後若斷線重連，_subscribe_all 必須用當下的
    self.symbols，不是啟動時那份——動態加過的代碼要被訂回來，動態移除的
    代碼不該再被訂閱回去。"""
    feed = FugleFeed("k", ["2330"], lambda t: None)
    thread = threading.Thread(target=feed.run, daemon=True)
    thread.start()
    assert fake_sdk.stock.connected.wait(timeout=2), "初次連線沒有在時限內完成"
    feed._wait_before_reconnect = lambda seconds: feed._stop_event.wait(timeout=0)

    feed.add_symbol("2454")            # 盤中動態加入
    feed.remove_symbol("2330")         # 盤中動態移除
    assert feed.subscription_count == 1

    fake_sdk.stock.connected.clear()
    fake_sdk.stock.subscriptions.clear()
    fake_sdk.stock.disconnect()        # 觸發重連

    assert fake_sdk.stock.connected.wait(timeout=2), "應該要重新連線"
    feed.stop()
    thread.join(timeout=2)

    subscribed = {(s["channel"], s["symbol"]) for s in fake_sdk.stock.subscriptions}
    assert subscribed == {("trades", "2454")}, \
        f"重連應以當下清單訂閱，不是啟動時那份：{subscribed}"


# -- Task 16 A：退訂必須帶訂閱 id --------------------------------------------
#
# 實測對 Fugle 伺服器直接測得：unsubscribe 帶 {"channel": ..., "symbol": ...}
# 會被拒為 "id should not be empty"，且訂閱槽永遠不會釋放；必須帶 subscribed
# 事件回傳的 id 才會被接受。

def test_remove_symbol_unsubscribes_by_id_and_reconnect_clears_the_id_table(fake_sdk):
    feed = FugleFeed("k", ["2330"], lambda t: None)
    thread = threading.Thread(target=feed.run, daemon=True)
    thread.start()
    assert fake_sdk.stock.connected.wait(timeout=2), "初次連線沒有在時限內完成"

    # 模擬伺服器確認訂閱，回傳這筆訂閱的 id。
    feed.handle_message(json.dumps({
        "event": "subscribed",
        "data": {"channel": "trades", "symbol": "2330", "id": "abc123"},
    }))
    assert feed.subscribed_symbols == {"2330"}

    feed.remove_symbol("2330")
    assert fake_sdk.stock.unsubscribe_calls == [{"id": "abc123"}], \
        "退訂必須帶伺服器給的 id，不是 channel+symbol"

    # 重連：新連線的訂閱 id 對舊表已經無效，必須清空重建。
    feed._wait_before_reconnect = lambda seconds: feed._stop_event.wait(timeout=0)
    fake_sdk.stock.connected.clear()
    fake_sdk.stock.disconnect()
    assert fake_sdk.stock.connected.wait(timeout=2), "應該要重新連線"

    assert feed._subscription_ids == {}, "重連後舊的訂閱 id 表必須清空"
    feed.stop()
    thread.join(timeout=2)


def test_remove_symbol_without_a_confirmed_id_skips_the_doomed_request(fake_sdk, capsys):
    """還沒收到 subscribed 確認（例如剛重連）就退訂，不該送出必然被拒的
    請求——那正是實測踩過的坑：channel+symbol 被拒，訂閱槽永遠不會釋放。"""
    feed = FugleFeed("k", ["2330"], lambda t: None)
    thread = threading.Thread(target=feed.run, daemon=True)
    thread.start()
    assert fake_sdk.stock.connected.wait(timeout=2)

    feed.remove_symbol("2330")             # 從未收到 subscribed 事件

    assert fake_sdk.stock.unsubscribe_calls == [], "沒有 id 就不該送退訂請求"
    assert "無法退訂" in capsys.readouterr().err
    feed.stop()
    thread.join(timeout=2)
