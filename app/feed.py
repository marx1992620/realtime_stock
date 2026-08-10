"""Fugle 行情接線。

單一連線約束：一把 API key 只能開一條 WebSocket 連線（實測超過會收到
close frame "Maximum number of connections reached"），所有股票與頻道都
必須擠在這一條上。

訂閱預算：那條連線最多接受 5 個訂閱，第 6 個起回
{'message': 'Subscription limit exceeded'}。每檔股票的每個頻道各算一個，
所以預設只訂 trades，五檔（books）由呼叫端以 book_symbols 明確指定。

連線韌性（Task 13）：實地發生過 Mac 睡眠、TCP 半開死亡卻沒有任何症狀——
沒有錯誤、沒有重連、時間戳就這樣凍住，95 分鐘資料悄悄消失。因此：
- run() 是個迴圈，斷線後以退避重連，每次都建立全新的 WebSocketClient
  （SDK 的 WebSocketApp 在 __init__ 建立，重用已關閉的物件不可靠）。
- SDK 內建的 ping/pong（health_check）加上自己的閒置看門狗兩層防護半開連線。
- status() 把健康狀態暴露出來，讓上層（web、UI）不必再靠「有沒有印錯誤」
  這種脆弱的訊號去猜資料還新不新鮮。
"""

from __future__ import annotations

import collections
import json
import os
import sys
import threading
import time
from typing import Callable

# 一條連線的訂閱上限（實測值）。預算檢查在 app/__main__ 啟動前做。
MAX_SUBSCRIPTIONS = 5

# 重連退避的上限秒數，以及一頁補資料 REST 請求的上限筆數（SDK/API 實測值）。
MAX_BACKOFF_SECONDS = 30.0
BACKFILL_PAGE_SIZE = 500

_PARSE_ERRORS = (json.JSONDecodeError, AttributeError, KeyError, TypeError, ValueError)


class FugleFeed:
    def __init__(self, api_key: str, symbols: list[str],
                 on_trade: Callable[[dict], None],
                 on_book: Callable[[dict], None],
                 include_trials: bool = False,
                 book_symbols: list[str] | None = None,
                 stale_after_seconds: float = 90.0) -> None:
        self.api_key = api_key
        self.symbols = list(symbols)
        # 只有這些代碼會另外訂 books；未指定即全部不訂，把預算留給 trades。
        self.book_symbols = list(book_symbols or [])
        self.on_trade = on_trade
        self.on_book = on_book
        self.include_trials = include_trials
        # 超額訂閱只印一行 stderr 就繼續跑，那些股票整天不會有任何資料，
        # 使用者無從察覺。留下紀錄讓上層能把它顯示出來。
        self.subscription_errors: list[str] = []
        # 被丟棄的事件必須留下痕跡：某類事件若不帶 symbol（最可能是開盤集合
        # 競價）會被整批濾掉，auction_lots 全日為 0 卻沒有任何錯誤 —— 看起來
        # 就像「今天沒有集合競價」。
        self.dropped_events: collections.Counter = collections.Counter()
        self._stock = None

        # -- 連線韌性狀態（Task 13）------------------------------------
        self.stale_after_seconds = stale_after_seconds
        self.last_message_at: float | None = None
        self.reconnects = 0
        # 尚未連上也算「還在嘗試連線」，狀態機只有三種值，沒有第四種給
        # 「第一次連線中」，所以起始值就是 reconnecting。
        self._state = "reconnecting"
        self._stop_event = threading.Event()
        # 兩個來源都會 set 它：目前這條連線的 "disconnect" 事件，或 stop()。
        # 每輪重連開始前 clear，這樣單一物件就能同時扮演「這條連線斷了」
        # 與「該停了」兩種喚醒訊號。
        self._wake_event = threading.Event()
        self._watchdog_thread: threading.Thread | None = None

    # -- message handling ------------------------------------------------
    def handle_message(self, raw: str) -> None:
        """處理一則訊息。任何單則訊息的問題都不得中斷整條行情。

        解析／路由與下游回呼的錯誤邊界必須分開：pa.lib.ArrowInvalid 繼承自
        ValueError，若回呼留在解析的 try 之內，聚合、寫檔、廣播的任何失敗都會
        被吞成一句「Unable to process message」—— 畫面照常更新，資料卻早已
        停止落檔。
        """
        try:
            route = self._route(raw)
        except _PARSE_ERRORS as error:
            # AttributeError matters: valid JSON that is not an object (a bare
            # array, or an event whose "data" is not a dict) reaches .get() and
            # would otherwise kill the stream.
            print(f"Unable to process message: {error}", file=sys.stderr, flush=True)
            return
        if route is None:
            return

        label, callback, data = route
        try:
            callback(data)
        except Exception as error:
            print(f"{label} callback failed: {type(error).__name__}: {error}",
                  file=sys.stderr, flush=True)

    def _on_message(self, raw: str) -> None:
        """SDK 的 "message" 事件掛這個，而不是直接掛 handle_message。

        任何訊息都算「活著」的證據，包含 ping/pong 的 pong —— 盤中若完全沒
        訂閱 books、又剛好沒有成交，pong 仍會定期進來，看門狗才不會誤判。
        """
        self.last_message_at = time.monotonic()
        self.handle_message(raw)

    def _route(self, raw: str) -> tuple[str, Callable[[dict], None], dict] | None:
        """解析一則訊息並決定該送去哪個回呼；不需要回呼時回傳 None。"""
        event = json.loads(raw)
        name = event.get("event")
        if name == "authenticated":
            print("API Key authenticated.", flush=True)
            return None
        if name == "subscribed":
            data = event.get("data") or {}
            print(f"Subscribed: {data.get('channel')} {data.get('symbol')}", flush=True)
            return None
        if name == "error":
            data = event.get("data")
            print(f"Fugle API error: {data}", file=sys.stderr, flush=True)
            message = data.get("message") if isinstance(data, dict) else None
            text = message if isinstance(message, str) else str(data)
            if "Subscription limit" in text:
                self.subscription_errors.append(text)
            return None
        if name != "data":
            return None

        channel = event.get("channel")
        data = event.get("data") or {}
        if data.get("symbol") not in self.symbols:
            self._note_dropped(channel, data)
            return None
        if channel == "trades":
            if data.get("isTrial", False) and not self.include_trials:
                return None
            return "Trade", self.on_trade, data
        if channel == "books":
            return "Book", self.on_book, data
        self._note_dropped(channel, data)
        return None

    def _note_dropped(self, channel: str | None, data: dict) -> None:
        """首次丟棄某個 channel 時說明一次，之後只累加計數。

        只印鍵名不印 payload：逐筆事件量太大，印全文會把 stderr 淹掉。
        """
        first = channel not in self.dropped_events
        self.dropped_events[channel] += 1
        if first:
            print(f"Dropping {channel} event with symbol={data.get('symbol')!r}; "
                  f"keys={sorted(data)}", file=sys.stderr, flush=True)

    # -- lifecycle -------------------------------------------------------
    def _subscribe_all(self, _message=None) -> None:
        # 只在伺服器確認認證後才送訂閱：SDK 在 connect 事件內送出 auth frame
        # 卻不等回覆，此時訂閱會被拒為 "Forbidden resource"。
        # 先訂 trades（那是主要目的），books 只給指定的代碼。訂閱預算不足時
        # 被拒的會是排在後面的五檔，而不是隨機某檔股票的逐筆。
        for symbol in self.symbols:
            self._stock.subscribe({"channel": "trades", "symbol": symbol})
        for symbol in self.book_symbols:
            self._stock.subscribe({"channel": "books", "symbol": symbol})

    def _make_disconnect_handler(self, stock) -> Callable[..., None]:
        """回傳一個只認「目前這條連線」的斷線 handler。

        重連會換掉 self._stock，但舊 client 的 "disconnect" 事件可能在那之後
        才姍姍來遲（SDK 在自己的背景執行緒裡偵測關閉）。若不比對還是不是
        目前這條連線，這種遲到的事件會誤把新連線的等待喚醒，看起來像新連線
        也斷了。
        """
        def handler(*_args) -> None:
            if self._stock is stock:
                self._wake_event.set()
        return handler

    def _wait_before_reconnect(self, seconds: float) -> bool:
        """退避等待，可被 stop() 提前中斷。回傳 True 表示等待期間被要求停止。"""
        return self._stop_event.wait(timeout=seconds)

    def stop(self) -> None:
        """要求 run() 的重連迴圈停止；關閉時不要無限重連。"""
        self._stop_event.set()
        self._wake_event.set()
        stock = self._stock
        if stock is not None:
            stock.disconnect()

    def run(self) -> None:
        """阻塞式；請在背景執行緒呼叫。

        斷線（含睡眠造成的半開連線、伺服器主動踢）後自動重連，指數退避
        1s → 2s → 4s → … 上限 30s，成功後歸零；直到 stop() 被呼叫為止。
        每次重連都會等前一條連線確實關閉才開新的，一把 key 只能開一條連線，
        新舊並存會被伺服器拒絕。
        """
        import certifi
        from fugle_marketdata import HealthCheckConfig, WebSocketClient

        os.environ.setdefault("SSL_CERT_FILE", certifi.where())
        os.environ.setdefault("REQUESTS_CA_BUNDLE", certifi.where())

        self._start_watchdog()

        backoff = 1.0
        while not self._stop_event.is_set():
            self._wake_event.clear()
            client = WebSocketClient(
                api_key=self.api_key,
                health_check=HealthCheckConfig(enabled=True, ping_interval=30000,
                                               max_missed_pongs=2))
            stock = client.stock
            stock.on("message", self._on_message)
            stock.on("authenticated", self._subscribe_all)
            stock.on("connect", lambda: print("Connected. Authenticating...", flush=True))
            stock.on("error", lambda e: print(f"WebSocket error: {e}",
                                              file=sys.stderr, flush=True))
            stock.on("disconnect", self._make_disconnect_handler(stock))
            self._stock = stock

            try:
                stock.connect()
            except Exception as error:            # noqa: BLE001 - 任何連線失敗都要能重試
                print(f"連線失敗：{type(error).__name__}: {error}",
                      file=sys.stderr, flush=True)
                # 認證失敗這類錯誤不保證 SDK 一定會補發 disconnect 事件，
                # 這裡自己喚醒一次，不然重連迴圈會卡死在等待上。
                self._wake_event.set()
            else:
                self._state = "connected"
                backoff = 1.0
                if self.reconnects > 0:
                    self._backfill()

            self._wake_event.wait()
            if self._stop_event.is_set():
                break

            self.reconnects += 1
            self._state = "reconnecting"
            wait_s = backoff
            print(f"連線中斷，{wait_s:.0f} 秒後進行第 {self.reconnects} 次重連...",
                  file=sys.stderr, flush=True)
            backoff = min(backoff * 2.0, MAX_BACKOFF_SECONDS)
            if self._wait_before_reconnect(wait_s):
                break

        self._state = "stopped"

    # -- 存活偵測（B）------------------------------------------------------
    def _start_watchdog(self) -> None:
        """閒置看門狗：半開連線可能永遠不會回報關閉，只能自己盯著看。

        一次啟動，貫穿整個 run() 的生命週期（含每次重連），不隨重連重開。
        """
        if self._watchdog_thread is not None:
            return

        def loop() -> None:
            # wait(timeout=15) 本身就是「每 15 秒檢查一次」，且一旦 stop()
            # 被呼叫會立刻回傳 True 結束迴圈，不會在 stop 後繼續空轉。
            while not self._stop_event.wait(timeout=15):
                self._watchdog_tick()

        self._watchdog_thread = threading.Thread(target=loop, daemon=True,
                                                  name="feed-watchdog")
        self._watchdog_thread.start()

    def _watchdog_tick(self) -> None:
        """檢查一次是否閒置過久；抽成方法方便測試不必真的等 90 秒。"""
        if self.last_message_at is None:
            return
        idle = time.monotonic() - self.last_message_at
        if idle > self.stale_after_seconds:
            print(f"行情已 {idle:.0f} 秒沒有任何訊息（上限 "
                  f"{self.stale_after_seconds:.0f} 秒），主動斷線觸發重連...",
                  file=sys.stderr, flush=True)
            stock = self._stock
            if stock is not None:
                stock.disconnect()

    # -- 狀態可見（C）------------------------------------------------------
    def status(self) -> dict:
        """給 web 層與 UI 用的健康狀態快照。"""
        last_message_ago = (time.monotonic() - self.last_message_at
                            if self.last_message_at is not None else None)
        return {
            "state": self._state,
            "last_message_ago": last_message_ago,
            "reconnects": self.reconnects,
            "subscription_errors": list(self.subscription_errors),
        }

    # -- 斷線補資料（D）-----------------------------------------------------
    def _backfill(self) -> None:
        """重連成功後，用 REST 把重連期間可能漏掉的成交補回 on_trade。

        只在 run() 判斷 reconnects > 0 時才會被呼叫，程式一開始的第一次連線
        不會多打這輪 REST。serial 去重已經在 SymbolAggregator.add_trade，
        整批餵進去是安全的，不需要精算漏了哪一段。任何失敗都不可讓行情
        跟著死掉。
        """
        try:
            from fugle_marketdata import RestClient
            rest = RestClient(api_key=self.api_key)
            for symbol in self.symbols:
                self._backfill_symbol(rest, symbol)
        except Exception as error:                # noqa: BLE001 - 補資料失敗不可影響行情
            print(f"補資料失敗：{type(error).__name__}: {error}",
                  file=sys.stderr, flush=True)

    def _backfill_symbol(self, rest, symbol: str) -> None:
        offset = 0
        while True:
            page = rest.stock.intraday.trades(symbol=symbol, limit=BACKFILL_PAGE_SIZE,
                                               offset=offset)
            trades = (page or {}).get("data") or []
            if not trades:
                return
            for trade in trades:
                # REST 逐筆資料沒有 symbol 欄位（在回應頂層），不補上會被
                # handle_message／_route 的代碼過濾整批丟棄。
                self.on_trade({**trade, "symbol": symbol})
            if len(trades) < BACKFILL_PAGE_SIZE:
                return
            offset += len(trades)
