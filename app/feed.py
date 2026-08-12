"""Fugle 行情接線。

單一連線約束：一把 API key 只能開一條 WebSocket 連線（實測超過會收到
close frame "Maximum number of connections reached"），所有股票都必須擠在
這一條上。

訂閱預算：那條連線最多接受 5 個訂閱，第 6 個起回
{'message': 'Subscription limit exceeded'}。每檔股票訂 trades 各算一個
（Task 16 起五檔報價功能已移除，不再跟股票搶配額），所以訂閱數就等於追蹤
的代碼數。

連線韌性（Task 13）：實地發生過 Mac 睡眠、TCP 半開死亡卻沒有任何症狀——
沒有錯誤、沒有重連、時間戳就這樣凍住，95 分鐘資料悄悄消失。因此：
- run() 是個迴圈，斷線後以退避重連，每次都建立全新的 WebSocketClient
  （SDK 的 WebSocketApp 在 __init__ 建立，重用已關閉的物件不可靠）。
- SDK 內建的 ping/pong（health_check）加上自己的閒置看門狗兩層防護半開連線。
- status() 把健康狀態暴露出來，讓上層（web、UI）不必再靠「有沒有印錯誤」
  這種脆弱的訊號去猜資料還新不新鮮。

訂閱確認與退訂（Task 16 A/B）：實測對 Fugle 伺服器直接測得——
unsubscribe 帶 {"channel": ..., "symbol": ...} 會被拒為
{'message': 'id should not be empty'}，且訂閱槽**永遠不會釋放**；必須帶
subscribed 事件回傳的 id（{"id": ...}）才會被接受。因此：
- 收到 subscribed 事件要記下 data["id"]，以 symbol 為鍵存進
  self._subscription_ids；remove_symbol 一律用這個 id 退訂，找不到 id 就
  不送必然被拒的請求，只印一行 stderr。
- 收到 unsubscribed 事件、或我們自己送出退訂時，把該筆 id 從表中移除。
- 重連會拿到全新的 id，舊表對新連線已經無效，_subscribe_all 每次都清空
  重建（同一份邏輯也覆蓋第一次連線）。
- 訂閱被拒（Subscription limit exceeded）不可静默留在追蹤清單裡看起來
  正常：self._pending_subscribe 記下最近一次送出訂閱的代碼，被拒時標記進
  self._subscribe_failed；subscribed_symbols / failed_subscriptions 讓上層
  （web.py 的 POST /api/symbols）能等待確認、逾時或失敗就回滾。

期貨（Task 19）：app/futures.py 的 FuturesFeed 複製了這裡的重連／看門狗／
訂閱 id 機制（刻意複製而非共用基底，理由見該檔開頭），但重試語意不同——
連續失敗達上限就永久停止，不像這裡無限重試。修改這裡的連線韌性邏輯時，
記得檢查 FuturesFeed 是否也要同步修正。
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
                 include_trials: bool = False,
                 stale_after_seconds: float = 90.0) -> None:
        self.api_key = api_key
        self.symbols = list(symbols)
        self.on_trade = on_trade
        self.include_trials = include_trials
        # 超額訂閱只印一行 stderr 就繼續跑，那些股票整天不會有任何資料，
        # 使用者無從察覺。留下紀錄讓上層能把它顯示出來。
        self.subscription_errors: list[str] = []
        # 被丟棄的事件必須留下痕跡：某類事件若不帶 symbol（最可能是開盤集合
        # 競價）會被整批濾掉，auction_lots 全日為 0 卻沒有任何錯誤 —— 看起來
        # 就像「今天沒有集合競價」。
        self.dropped_events: collections.Counter = collections.Counter()
        self._stock = None

        # -- 訂閱確認狀態（Task 16 A/B）------------------------------------
        # symbol -> 伺服器在 subscribed 事件裡給的 id；退訂必須用這個。
        self._subscription_ids: dict[str, str] = {}
        # 最近一次送出 subscribe 請求的代碼；不需要完美對應，Subscription
        # limit exceeded 一來就把它標記失敗。
        self._pending_subscribe: str | None = None
        self._subscribe_failed: set[str] = set()

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

        任何訊息都算「活著」的證據，包含 ping/pong 的 pong —— 盤中若剛好沒有
        成交，pong 仍會定期進來，看門狗才不會誤判。
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
            symbol = data.get("symbol")
            sub_id = data.get("id")
            # 記下這筆訂閱的 id：退訂必須帶它，帶 channel+symbol 會被伺服器
            # 拒為 "id should not be empty" 且訂閱槽永遠不會釋放（實測）。
            if symbol is not None and sub_id:
                self._subscription_ids[symbol] = sub_id
            if symbol is not None:
                self._subscribe_failed.discard(symbol)
            print(f"Subscribed: {data.get('channel')} {symbol}", flush=True)
            return None
        if name == "unsubscribed":
            data = event.get("data") or {}
            symbol = data.get("symbol")
            sub_id = data.get("id")
            if symbol is not None:
                current = self._subscription_ids.get(symbol)
                # 只在這則確認對應「目前記錄的那一筆」才清掉：remove_symbol 已
                # 經在送出退訂當下就同步清過一次，這裡多半是 no-op；但若這期間
                # 使用者又很快把同一檔加回來、拿到全新的 id，這則遲到的
                # unsubscribed 確認絕不能誤刪剛建立好的新訂閱。
                if current is None or sub_id is None or current == sub_id:
                    self._subscription_ids.pop(symbol, None)
            print(f"Unsubscribed: {data.get('channel')} {symbol}", flush=True)
            return None
        if name == "error":
            data = event.get("data")
            print(f"Fugle API error: {data}", file=sys.stderr, flush=True)
            message = data.get("message") if isinstance(data, dict) else None
            text = message if isinstance(message, str) else str(data)
            if "Subscription limit" in text:
                self.subscription_errors.append(text)
                # 不需要完美對應：最近一次送出的那個代碼最可能是被拒的那個。
                if self._pending_subscribe is not None:
                    self._subscribe_failed.add(self._pending_subscribe)
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
        #
        # 重連會拿到全新的訂閱 id，舊表對這條新連線已經無效；同一份邏輯也
        # 覆蓋第一次連線，此時只是在空字典上操作、no-op。同理清空
        # _subscribe_failed：新連線值得重新嘗試，不該延續上一條連線的失敗
        # 標記。
        self._subscription_ids.clear()
        self._subscribe_failed.clear()
        for symbol in self.symbols:
            self._pending_subscribe = symbol
            self._stock.subscribe({"channel": "trades", "symbol": symbol})

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

    # -- 盤中動態增刪（Task 14 C）--------------------------------------------
    def add_symbol(self, symbol: str) -> None:
        """在既有連線上訂閱一檔新代碼；不開第二條連線（一把 key 只能一條）。

        先更新 self.symbols 再送訂閱：更新清單本身不需要連線存在，即使目前
        正在重連中途（self._stock 是 None）也不會遺失這個新代碼——下一次
        連線的 _subscribe_all 會用當下這份清單重新訂閱。
        """
        if symbol not in self.symbols:
            self.symbols.append(symbol)
        # 重新嘗試訂閱同一檔，之前的失敗標記就不該再擋著它。
        self._subscribe_failed.discard(symbol)
        if self._stock is None:
            return
        self._pending_subscribe = symbol
        self._stock.subscribe({"channel": "trades", "symbol": symbol})

    def remove_symbol(self, symbol: str) -> None:
        """退訂一檔代碼（trades）並從清單移除。

        清單一移除，handle_message 的代碼過濾立刻生效（就算退訂的 REST/WS
        呼叫本身失敗，之後進來的該代碼事件也不會再被送進聚合器），未來的
        重連也不會再把它訂回來。

        退訂必須帶伺服器在 subscribed 事件裡給的 id——實測 unsubscribe 帶
        {"channel": ..., "symbol": ...} 會被拒為 "id should not be empty"，
        且訂閱槽永遠不會釋放，之後任何新訂閱都會撞上
        "Subscription limit exceeded"。找不到 id 時（例如訂閱還沒被確認、
        剛重連、或訂閱本身早就被伺服器拒絕）就不送這個必然被拒的請求，只留
        一行 stderr 說明。
        """
        if symbol in self.symbols:
            self.symbols.remove(symbol)
        self._subscribe_failed.discard(symbol)
        if self._pending_subscribe == symbol:
            self._pending_subscribe = None
        if self._stock is None:
            return
        sub_id = self._subscription_ids.pop(symbol, None)
        if sub_id is None:
            print(f"無法退訂 {symbol}：沒有對應的訂閱 id（可能還沒收到 subscribed "
                  "確認，或訂閱已被伺服器拒絕），略過退訂請求", file=sys.stderr, flush=True)
            return
        self._stock.unsubscribe({"id": sub_id})

    @property
    def subscription_count(self) -> int:
        """目前訂閱數，供上層在新增前做預算檢查。"""
        return len(self.symbols)

    @property
    def subscribed_symbols(self) -> set[str]:
        """目前確實收到 subscribed 確認的代碼集合（Task 16 B）。

        訂閱被伺服器拒絕時該代碼不會出現在這裡；上層（POST /api/symbols）
        靠這個分辨「訂閱真的成功了」而不是靜默留在清單裡看起來正常。
        """
        return set(self._subscription_ids)

    @property
    def failed_subscriptions(self) -> set[str]:
        """最近一次訂閱被伺服器拒絕（Subscription limit exceeded）的代碼。"""
        return set(self._subscribe_failed)

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
