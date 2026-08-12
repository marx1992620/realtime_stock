"""台指期即時捕捉（Task 19）。

=====================================================================
假設清單 —— 這一輪完全無法對真實伺服器驗證（同一把 key，同一時刻測得：
股票 WS 可認證；期貨 WS 回 "Maximum number of connections reached"；
REST /futopt/* 一律 403）。以下皆為假設，開通期貨行情權限後逐項對照
「確認方式」重新驗證，錯了只需要改對應那一個常數／設定：

1. 台指期近一合約代碼：DEFAULT_FUTURES_SYMBOL = "TXFR1"。
   錯了：改這個常數，或執行時用 --futures 覆寫，不必動程式碼。
   確認方式：權限開通後打 REST 合約清單端點確認代碼格式。
2. 契約乘數（每點金額）：CONTRACT_MULTIPLIER = 200（台指期大台）。
   錯了：改這個常數；迷你台指期（小台）已知是 50，微台指是 10。
   確認方式：對照交易所公告的契約規格表。
3. size 欄位單位是「口」：UNIT_LABEL = "口"。
   錯了：改這個常數，畫面顯示字串會跟著換（見 app/static/index.html）。
   確認方式：拿到第一筆真實成交事件後直接對照欄位語意。
4. 買賣方向判定規則沿用 app/classify.py（price>=ask 買、price<=bid 賣、
   兩者皆缺為集合競價、只缺一邊為 unknown）——這是股票逐筆驗證過的規則，
   期貨的五檔／tick 慣例未經驗證，可能需要另外規則（例如期貨沒有漲跌停
   鎖住某一側報價的情況，或 bid/ask 語意不同）。
   確認方式：取得真實逐筆資料後與交易所 tradeVolumeAtAsk/AtBid 對帳，
   比照 app/classify.py 檔頭當初驗證股票規則的做法。
5. WebSocket 訂閱／頻道形狀：{"channel": "trades", "symbol": <合約代碼>}，
   透過 WebSocketClient(...).futopt，事件形狀（authenticated/subscribed/
   unsubscribed/error/data）假設與 .stock 完全相同。
   確認方式：權限開通後，先用一支獨立最小腳本連線印出原始訊息比對。
6. REST 補資料端點：假設為 rest.futopt.intraday.trades(symbol=, limit=,
   offset=)，比照股票的 rest.stock.intraday.trades；今天實測 403，完全
   沒驗證過這個路徑是否存在、參數形狀是否相同、回應是否一樣分頁。
   確認方式：同上，用最小腳本先試打一次，不要直接讓正式程式碰。
=====================================================================

設計決策——複製而非共用基底：FuturesFeed 的重連／看門狗／訂閱 id 機制
結構上比照 app/feed.py 的 FugleFeed，但刻意複製整份邏輯而非抽共用基底：
(1) feed.py 背後有 165 個通過中的測試緊貼著它的內部屬性與方法名稱，
    任何抽象都有連帶弄壞股票行情這條「不能斷」的路徑的風險；
(2) 兩者的重連語意本質不同——FugleFeed 斷線後無限重試（帳號的連線終究
    會恢復），FuturesFeed 連續失敗達 MAX_FUTURES_CONNECT_ATTEMPTS 次後
    永久停止並轉為 "unavailable"（今天實測帳號沒有期貨權限，這件事不會
    隨時間過去而改變，無限重試沒有意義，也是任務要求的行為）。
    共用基底會需要一個「要不要無限重試」的旗標才能兩邊都用，得不償失。
兩邊互相有指標註解（見 app/feed.py 檔頭），日後修正連線韌性邏輯記得
同步檢查另一邊是否也要跟著改。
"""

from __future__ import annotations

import collections
import json
import os
import sys
import threading
import time
from typing import Callable

# -- 假設 1～3：見檔頭說明 -------------------------------------------------
DEFAULT_FUTURES_SYMBOL = "TXFR1"
CONTRACT_MULTIPLIER = 200
UNIT_LABEL = "口"

# 連續失敗滿此數就停止重試、狀態轉為 "unavailable"（Task 19 B 的硬性要求：
# 帳號目前只有一條 WebSocket 連線的配額，被股票佔用時期貨幾乎必然被拒，
# 不能像股票那樣無限重試空轉幾百次）。
MAX_FUTURES_CONNECT_ATTEMPTS = 5

# 目前設計只訂閱一檔近一合約；與 app/feed.py 的 MAX_SUBSCRIPTIONS（股票
# 連線的訂閱上限）無關，是完全獨立的第二條連線。
MAX_FUTURES_SUBSCRIPTIONS = 1

# 重連退避上限秒數、補資料一頁筆數上限：與 app/feed.py 的同名常數刻意各自
#獨立一份（見檔頭「設計決策」），不是同一個數字有兩個名字。
FUTURES_MAX_BACKOFF_SECONDS = 30.0
FUTURES_BACKFILL_PAGE_SIZE = 500

_PARSE_ERRORS = (json.JSONDecodeError, AttributeError, KeyError, TypeError, ValueError)


def futures_trade_value_twd(price: float, size: int) -> float:
    """期貨成交金額（台幣）＝ price * size * CONTRACT_MULTIPLIER。

    與股票的 price * lots * 1000（app/classify.trade_value_twd，SHARES_PER_LOT
    寫死 1000）是兩套完全不同的算法，刻意不動 classify.py——那支模組只服務
    股票，「契約乘數」是期貨才有的概念，混進去只會讓股票那邊多一個不會用到
    的分支。呼叫端（app/__main__.py 的 Pipeline）在寫入聚合器之後、落檔與
    廣播之前，用這個函式覆寫 record["value_twd"]。
    """
    return price * size * CONTRACT_MULTIPLIER


class FuturesFeed:
    """單一期貨合約的行情連線。

    結構比照 app/feed.py 的 FugleFeed（對照見該檔開頭「單一連線約束」與
    「訂閱確認與退訂」段落），但只訂閱一檔合約（self.symbol 是字串不是
    清單），且連續失敗 MAX_FUTURES_CONNECT_ATTEMPTS 次後停止重試、狀態轉
    為 "unavailable"（見本檔開頭「設計決策」）。

    執行緒模型與 FugleFeed 相同：run() 阻塞、請在獨立的背景執行緒呼叫；
    與股票 feed 是完全獨立的物件、獨立的執行緒、獨立的重連迴圈——任何一邊
    失敗都不會透過共享狀態影響另一邊（兩者只透過各自呼叫 on_trade 間接寫進
    MarketState，MarketState 本身有鎖保護，見 app/web.py）。
    """

    def __init__(self, api_key: str, symbol: str,
                 on_trade: Callable[[dict], None],
                 stale_after_seconds: float = 90.0) -> None:
        self.api_key = api_key
        self.symbol = symbol
        self.on_trade = on_trade
        self.subscription_errors: list[str] = []
        self.dropped_events: collections.Counter = collections.Counter()
        self._futopt = None

        # -- 訂閱確認狀態（比照 FugleFeed，見 app/feed.py Task 16 A/B 說明）--
        self._subscription_ids: dict[str, str] = {}
        self._pending_subscribe: str | None = None
        self._subscribe_failed: set[str] = set()

        # -- 連線韌性狀態（比照 FugleFeed，見 app/feed.py Task 13 說明）-----
        self.stale_after_seconds = stale_after_seconds
        self.last_message_at: float | None = None
        self.reconnects = 0
        self._state = "reconnecting"
        self._stop_event = threading.Event()
        self._wake_event = threading.Event()
        self._watchdog_thread: threading.Thread | None = None

        # -- 這裡是 FuturesFeed 特有、FugleFeed 沒有的狀態：連續失敗計數與
        # 停止重試後的原因（Task 19 B）----------------------------------
        self._last_error: str | None = None
        self._connect_attempts = 0
        self._authenticated_this_attempt = False
        self._unavailable_reason: str | None = None

    # -- message handling（比照 FugleFeed.handle_message，見 app/feed.py）--
    def handle_message(self, raw: str) -> None:
        try:
            route = self._route(raw)
        except _PARSE_ERRORS as error:
            print(f"[期貨] Unable to process message: {error}",
                  file=sys.stderr, flush=True)
            return
        if route is None:
            return

        label, callback, data = route
        try:
            callback(data)
        except Exception as error:
            print(f"[期貨] {label} callback failed: {type(error).__name__}: {error}",
                  file=sys.stderr, flush=True)

    def _on_message(self, raw: str) -> None:
        self.last_message_at = time.monotonic()
        self.handle_message(raw)

    def _route(self, raw: str) -> tuple[str, Callable[[dict], None], dict] | None:
        event = json.loads(raw)
        name = event.get("event")
        if name == "authenticated":
            print("[期貨] API Key authenticated.", flush=True)
            return None
        if name == "subscribed":
            data = event.get("data") or {}
            symbol = data.get("symbol")
            sub_id = data.get("id")
            if symbol is not None and sub_id:
                self._subscription_ids[symbol] = sub_id
            if symbol is not None:
                self._subscribe_failed.discard(symbol)
            print(f"[期貨] Subscribed: {data.get('channel')} {symbol}", flush=True)
            return None
        if name == "unsubscribed":
            data = event.get("data") or {}
            symbol = data.get("symbol")
            sub_id = data.get("id")
            if symbol is not None:
                current = self._subscription_ids.get(symbol)
                if current is None or sub_id is None or current == sub_id:
                    self._subscription_ids.pop(symbol, None)
            print(f"[期貨] Unsubscribed: {data.get('channel')} {symbol}", flush=True)
            return None
        if name == "error":
            data = event.get("data")
            print(f"[期貨] Fugle API error: {data}", file=sys.stderr, flush=True)
            message = data.get("message") if isinstance(data, dict) else None
            text = message if isinstance(message, str) else str(data)
            self._last_error = text
            if "Subscription limit" in text:
                self.subscription_errors.append(text)
                if self._pending_subscribe is not None:
                    self._subscribe_failed.add(self._pending_subscribe)
            return None
        if name != "data":
            return None

        channel = event.get("channel")
        data = event.get("data") or {}
        if data.get("symbol") != self.symbol:
            self._note_dropped(channel, data)
            return None
        if channel == "trades":
            return "Trade", self.on_trade, data
        self._note_dropped(channel, data)
        return None

    def _note_dropped(self, channel: str | None, data: dict) -> None:
        first = channel not in self.dropped_events
        self.dropped_events[channel] += 1
        if first:
            print(f"[期貨] Dropping {channel} event with symbol={data.get('symbol')!r}; "
                  f"keys={sorted(data)}", file=sys.stderr, flush=True)

    # -- lifecycle（比照 FugleFeed，見 app/feed.py）-----------------------
    def _subscribe_all(self, _message=None) -> None:
        """SDK 的 "authenticated" 事件掛這個（比照 FugleFeed._subscribe_all）。

        額外多做兩件 FugleFeed 沒有的事：把 _authenticated_this_attempt
        標成 True、把連續失敗計數歸零——這是「這次真的連上了」的唯一憑證，
        run() 靠它判斷這輪嘗試該不該算進 MAX_FUTURES_CONNECT_ATTEMPTS。
        """
        self._authenticated_this_attempt = True
        self._connect_attempts = 0
        self._state = "connected"
        self._subscription_ids.clear()
        self._subscribe_failed.clear()
        self._pending_subscribe = self.symbol
        self._futopt.subscribe({"channel": "trades", "symbol": self.symbol})

    def _make_disconnect_handler(self, futopt) -> Callable[..., None]:
        def handler(*_args) -> None:
            if self._futopt is futopt:
                self._wake_event.set()
        return handler

    def _wait_before_reconnect(self, seconds: float) -> bool:
        return self._stop_event.wait(timeout=seconds)

    def stop(self) -> None:
        self._stop_event.set()
        self._wake_event.set()
        futopt = self._futopt
        if futopt is not None:
            futopt.disconnect()

    def _on_sdk_error(self, error) -> None:
        """SDK 的 "error" 事件掛這個；只負責記錄原因供 unavailable 顯示，
        不牽動任何狀態機。真正判斷這輪嘗試成功與否的是有沒有收到
        authenticated（見 _subscribe_all／run）。"""
        self._last_error = str(error)
        print(f"[期貨] WebSocket error: {error}", file=sys.stderr, flush=True)

    def run(self) -> None:
        """阻塞式；請在背景執行緒呼叫。

        與 FugleFeed.run() 最大的不同：這裡連續 MAX_FUTURES_CONNECT_ATTEMPTS
        次都沒有收到 authenticated 事件（不論是 connect() 直接拋例外、還是
        連上後被伺服器以錯誤/斷線拒絕）就停止重試、狀態轉為 "unavailable"
        並直接 return，不再繼續空轉——帳號只有一條連線的配額，被股票佔用
        時期貨幾乎必然被拒，這件事不會隨時間過去而改變。
        """
        import certifi
        from fugle_marketdata import HealthCheckConfig, WebSocketClient

        os.environ.setdefault("SSL_CERT_FILE", certifi.where())
        os.environ.setdefault("REQUESTS_CA_BUNDLE", certifi.where())

        self._start_watchdog()

        backoff = 1.0
        while not self._stop_event.is_set():
            self._wake_event.clear()
            self._authenticated_this_attempt = False
            client = WebSocketClient(
                api_key=self.api_key,
                health_check=HealthCheckConfig(enabled=True, ping_interval=30000,
                                               max_missed_pongs=2))
            futopt = client.futopt
            futopt.on("message", self._on_message)
            futopt.on("authenticated", self._subscribe_all)
            futopt.on("connect", lambda: print(
                "[期貨] Connected. Authenticating...", flush=True))
            futopt.on("error", self._on_sdk_error)
            futopt.on("disconnect", self._make_disconnect_handler(futopt))
            self._futopt = futopt

            try:
                futopt.connect()
            except Exception as error:            # noqa: BLE001 - 任何連線失敗都要能重試/計數
                self._last_error = f"{type(error).__name__}: {error}"
                print(f"[期貨] 連線失敗：{self._last_error}",
                      file=sys.stderr, flush=True)
                self._wake_event.set()
            else:
                backoff = 1.0
                if self.reconnects > 0:
                    self._backfill()

            self._wake_event.wait()
            if self._stop_event.is_set():
                break

            if not self._authenticated_this_attempt:
                self._connect_attempts += 1
                print(f"[期貨] 連線第 {self._connect_attempts}/"
                      f"{MAX_FUTURES_CONNECT_ATTEMPTS} 次嘗試失敗"
                      f"{'：' + self._last_error if self._last_error else ''}",
                      file=sys.stderr, flush=True)
                if self._connect_attempts >= MAX_FUTURES_CONNECT_ATTEMPTS:
                    self._mark_unavailable()
                    return

            self.reconnects += 1
            self._state = "reconnecting"
            wait_s = backoff
            print(f"[期貨] 連線中斷，{wait_s:.0f} 秒後進行第 {self.reconnects} 次重連...",
                  file=sys.stderr, flush=True)
            backoff = min(backoff * 2.0, FUTURES_MAX_BACKOFF_SECONDS)
            if self._wait_before_reconnect(wait_s):
                break

        self._state = "stopped"

    def _mark_unavailable(self) -> None:
        self._state = "unavailable"
        reason = self._last_error or "未知原因"
        self._unavailable_reason = (
            f"連續 {self._connect_attempts} 次連線失敗（{reason}）。"
            "可能是方案未開通期貨行情，或連線配額已被股票佔用。已停止重試。"
        )
        print(f"[期貨] 行情不可用：{self._unavailable_reason}",
              file=sys.stderr, flush=True)

    # -- 存活偵測（比照 FugleFeed，見 app/feed.py）-------------------------
    def _start_watchdog(self) -> None:
        if self._watchdog_thread is not None:
            return

        def loop() -> None:
            while not self._stop_event.wait(timeout=15):
                self._watchdog_tick()

        self._watchdog_thread = threading.Thread(
            target=loop, daemon=True, name="futures-feed-watchdog")
        self._watchdog_thread.start()

    def _watchdog_tick(self) -> None:
        if self.last_message_at is None:
            return
        idle = time.monotonic() - self.last_message_at
        if idle > self.stale_after_seconds:
            print(f"[期貨] 行情已 {idle:.0f} 秒沒有任何訊息（上限 "
                  f"{self.stale_after_seconds:.0f} 秒），主動斷線觸發重連...",
                  file=sys.stderr, flush=True)
            futopt = self._futopt
            if futopt is not None:
                futopt.disconnect()

    @property
    def subscription_count(self) -> int:
        return 1

    @property
    def subscribed_symbols(self) -> set[str]:
        return set(self._subscription_ids)

    @property
    def failed_subscriptions(self) -> set[str]:
        return set(self._subscribe_failed)

    # -- 狀態可見 ----------------------------------------------------------
    def status(self) -> dict:
        """給 web 層與 UI 用的健康狀態快照；形狀與 FugleFeed.status() 相同，
        外加 "market" 與 "unavailable_reason"（Task 19 A）。"""
        last_message_ago = (time.monotonic() - self.last_message_at
                            if self.last_message_at is not None else None)
        return {
            "state": self._state,
            "last_message_ago": last_message_ago,
            "reconnects": self.reconnects,
            "subscription_errors": list(self.subscription_errors),
            "market": "futopt",
            "unavailable_reason": self._unavailable_reason,
        }

    # -- 斷線補資料（比照 FugleFeed._backfill；REST 路徑未驗證，見檔頭假設 6）
    def _backfill(self) -> None:
        try:
            from fugle_marketdata import RestClient
            rest = RestClient(api_key=self.api_key)
            offset = 0
            while True:
                page = rest.futopt.intraday.trades(
                    symbol=self.symbol, limit=FUTURES_BACKFILL_PAGE_SIZE, offset=offset)
                trades = (page or {}).get("data") or []
                if not trades:
                    return
                for trade in trades:
                    self.on_trade({**trade, "symbol": self.symbol})
                if len(trades) < FUTURES_BACKFILL_PAGE_SIZE:
                    return
                offset += len(trades)
        except Exception as error:             # noqa: BLE001 - 補資料失敗不可影響行情
            print(f"[期貨] 補資料失敗：{type(error).__name__}: {error}",
                  file=sys.stderr, flush=True)
