"""台指期即時捕捉（Task 19）。測試從簡：每項一個測試，全部用假物件；
本輪無法對真實伺服器驗證，見 app/futures.py 檔頭的假設清單。"""

import json
import sys
import threading
import types

from app.aggregator import SymbolAggregator
from app.feed import FugleFeed
from app.futures import (
    CONTRACT_MULTIPLIER,
    MAX_FUTURES_CONNECT_ATTEMPTS,
    FuturesFeed,
    futures_trade_value_twd,
)


class FakeStock:
    """比照 tests/test_feed.py 的 FakeStock：股票端一切正常，用來證明
    期貨連線失敗不會拖累它。這裡只複製測試需要的最小子集，刻意不跨檔
    共用，兩個測試檔各自獨立。"""

    def __init__(self):
        self.handlers = {}
        self.connected = threading.Event()

    def on(self, event, listener):
        self.handlers[event] = listener

    def subscribe(self, params):
        pass

    def unsubscribe(self, params):
        pass

    def connect(self):
        self.handlers["connect"]()
        self.handlers["authenticated"]('{"event":"authenticated","data":{}}')
        self.connected.set()

    def disconnect(self):
        handler = self.handlers.get("disconnect")
        if handler:
            handler(None, None)


class RejectingFutStock:
    """模擬今天實測到的情況：futopt 連線每次都被拒
    （"Maximum number of connections reached"）。"""

    def __init__(self, attempts):
        self.handlers = {}
        self._attempts = attempts

    def on(self, event, listener):
        self.handlers[event] = listener

    def subscribe(self, params):
        raise AssertionError("連線都沒成功，不該送出訂閱")

    def connect(self):
        self._attempts.append(1)
        raise RuntimeError("Maximum number of connections reached")

    def disconnect(self):
        pass


def _patch_sdk(monkeypatch, make_client):
    sdk = types.ModuleType("fugle_marketdata")
    sdk.WebSocketClient = make_client
    sdk.HealthCheckConfig = lambda **kwargs: kwargs
    monkeypatch.setitem(sys.modules, "fugle_marketdata", sdk)
    certifi = types.ModuleType("certifi")
    certifi.where = lambda: "/dev/null"
    monkeypatch.setitem(sys.modules, "certifi", certifi)


# -- B：連續失敗 5 次後停止重試 ------------------------------------------

def test_futures_feed_stops_retrying_after_five_rejections(monkeypatch):
    attempts = []

    def make_client(**kwargs):
        return types.SimpleNamespace(futopt=RejectingFutStock(attempts))

    _patch_sdk(monkeypatch, make_client)

    feed = FuturesFeed("k", "TXFR1", lambda t: None)
    feed._wait_before_reconnect = lambda seconds: False   # 不必真的等退避

    feed.run()      # 連續失敗滿額後應自行 return，不必呼叫 stop()

    assert len(attempts) == MAX_FUTURES_CONNECT_ATTEMPTS == 5
    assert feed.status()["state"] == "unavailable"
    reason = feed.status()["unavailable_reason"]
    assert "可能是方案未開通期貨行情，或連線配額已被股票佔用" in reason


# -- B：期貨失敗不得影響股票 feed ----------------------------------------

def test_futures_failure_does_not_disturb_the_stock_feed(monkeypatch):
    stock = FakeStock()
    fut_attempts = []

    def make_client(**kwargs):
        # 同一個 WebSocketClient 假物件同時提供 .stock（正常）與 .futopt
        # （每次都被拒），比照真實 SDK 一個 client 物件底下有多個市場命名空間。
        return types.SimpleNamespace(stock=stock, futopt=RejectingFutStock(fut_attempts))

    _patch_sdk(monkeypatch, make_client)

    stock_trades = []
    stock_feed = FugleFeed("k", ["2330"], stock_trades.append)
    futures_feed = FuturesFeed("k", "TXFR1", lambda t: None)
    futures_feed._wait_before_reconnect = lambda seconds: False

    stock_thread = threading.Thread(target=stock_feed.run, daemon=True)
    futures_thread = threading.Thread(target=futures_feed.run, daemon=True)
    stock_thread.start()
    futures_thread.start()

    assert stock.connected.wait(timeout=2), "股票連線應正常完成，不受期貨拖累"
    futures_thread.join(timeout=2)
    assert not futures_thread.is_alive(), "期貨應在連續失敗後自行停止，不會卡住"

    # 股票這時應仍正常運作：送一筆成交驗證行情迴圈沒有被期貨的失敗波及。
    stock_feed.handle_message(json.dumps({
        "event": "data", "channel": "trades",
        "data": {"symbol": "2330", "price": 100, "size": 1, "bid": 99, "ask": 100,
                 "time": 1, "serial": 1},
    }))
    assert [t["serial"] for t in stock_trades] == [1]
    assert stock_feed.status()["state"] == "connected"

    assert futures_feed.status()["state"] == "unavailable"
    assert len(fut_attempts) == MAX_FUTURES_CONNECT_ATTEMPTS

    stock_feed.stop()
    stock_thread.join(timeout=2)


# -- C：重用 classify 判定買賣、value_twd 用契約乘數 200 --------------------

def test_futures_trade_is_classified_and_valued_with_the_contract_multiplier():
    assert CONTRACT_MULTIPLIER == 200
    aggregator = SymbolAggregator("TXFR1", large_order_lots=1)
    trade = {"symbol": "TXFR1", "price": 17000, "size": 3, "bid": 16995,
             "ask": 17000, "time": 1, "serial": 1}

    record = aggregator.add_trade(trade)                  # 重用既有的 classify/聚合邏輯
    record["value_twd"] = futures_trade_value_twd(record["price"], record["lots"])

    assert record["side"] == "buy"                         # price == ask -> 外盤買
    assert record["is_large"] is True                       # 3 口 >= 門檻 1 口
    assert record["value_twd"] == 17000 * 3 * 200 == 10_200_000
