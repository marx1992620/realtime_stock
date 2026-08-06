"""單一連線約束與訂閱時序。

實測事實：
- 一把 API key 只能開一條 WebSocket 連線，超過會被以 close frame
  "Maximum number of connections reached" 斷線。
- SDK 在 connect 事件內送出 auth frame 但不等回覆，因此必須等
  authenticated 事件才能訂閱，否則被拒為 "Forbidden resource"。
"""

import json
import sys
import types

import pytest

from app.feed import CHANNELS, FugleFeed


class FakeStock:
    def __init__(self, timeline):
        self.timeline = timeline
        self.handlers = {}
        self.subscriptions = []

    def on(self, event, listener):
        self.handlers[event] = listener

    def subscribe(self, params):
        self.subscriptions.append(params)
        self.timeline.append(("subscribe", params["channel"], params["symbol"]))

    def connect(self):
        self.timeline.append(("auth_frame_sent",))
        self.handlers["connect"]()
        self.timeline.append(("server_authenticated",))
        self.handlers["authenticated"]('{"event":"authenticated","data":{}}')


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
    monkeypatch.setitem(sys.modules, "fugle_marketdata", sdk)
    certifi = types.ModuleType("certifi")
    certifi.where = lambda: "/dev/null"
    monkeypatch.setitem(sys.modules, "certifi", certifi)
    return types.SimpleNamespace(stock=stock, timeline=timeline, created=created)


def test_uses_exactly_one_connection_for_all_symbols(fake_sdk):
    FugleFeed("k", ["2330", "2317", "2454"], lambda t: None, lambda b: None).run()
    assert len(fake_sdk.created) == 1, "API key 只允許一條連線"


def test_subscribes_every_symbol_on_both_channels(fake_sdk):
    FugleFeed("k", ["2330", "2317"], lambda t: None, lambda b: None).run()
    subscribed = {(s["channel"], s["symbol"]) for s in fake_sdk.stock.subscriptions}
    assert subscribed == {(c, s) for c in CHANNELS for s in ("2330", "2317")}


def test_all_subscriptions_happen_after_authentication(fake_sdk):
    FugleFeed("k", ["2330", "2317"], lambda t: None, lambda b: None).run()
    auth_at = fake_sdk.timeline.index(("server_authenticated",))
    first_sub = min(i for i, e in enumerate(fake_sdk.timeline) if e[0] == "subscribe")
    assert first_sub > auth_at, f"訂閱早於認證會被拒為 Forbidden resource: {fake_sdk.timeline}"


def test_trade_events_reach_the_trade_callback(fake_sdk):
    trades = []
    feed = FugleFeed("k", ["2330"], trades.append, lambda b: None)
    feed.handle_message(json.dumps({
        "event": "data", "channel": "trades",
        "data": {"symbol": "2330", "price": 2405, "size": 1, "bid": 2400,
                 "ask": 2405, "time": 1, "serial": 7},
    }))
    assert [t["serial"] for t in trades] == [7]


def test_book_events_reach_the_book_callback(fake_sdk):
    books = []
    feed = FugleFeed("k", ["2330"], lambda t: None, books.append)
    feed.handle_message(json.dumps({
        "event": "data", "channel": "books",
        "data": {"symbol": "2330", "bids": [{"price": 2390, "size": 226}],
                 "asks": [{"price": 2395, "size": 343}], "time": 2},
    }))
    assert books[0]["bids"][0]["price"] == 2390


def test_trial_matches_are_skipped_by_default(fake_sdk):
    trades = []
    feed = FugleFeed("k", ["2330"], trades.append, lambda b: None)
    feed.handle_message(json.dumps({
        "event": "data", "channel": "trades",
        "data": {"symbol": "2330", "price": 2385, "size": 2113, "isTrial": True,
                 "time": 3, "serial": 8},
    }))
    assert trades == []


def test_trial_matches_kept_when_requested(fake_sdk):
    trades = []
    feed = FugleFeed("k", ["2330"], trades.append, lambda b: None, include_trials=True)
    feed.handle_message(json.dumps({
        "event": "data", "channel": "trades",
        "data": {"symbol": "2330", "price": 2385, "size": 2113, "isTrial": True,
                 "time": 3, "serial": 8},
    }))
    assert [t["serial"] for t in trades] == [8]


def test_untracked_symbol_is_ignored(fake_sdk):
    trades = []
    feed = FugleFeed("k", ["2330"], trades.append, lambda b: None)
    feed.handle_message(json.dumps({
        "event": "data", "channel": "trades",
        "data": {"symbol": "2454", "price": 1000, "size": 1, "bid": 999,
                 "ask": 1000, "time": 4, "serial": 9},
    }))
    assert trades == []


def test_malformed_message_does_not_kill_the_feed(fake_sdk):
    trades = []
    feed = FugleFeed("k", ["2330"], trades.append, lambda b: None)
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
