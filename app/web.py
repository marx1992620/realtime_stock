"""Web 服務：快照 API、門檻調整、瀏覽器 WebSocket 推送。

執行緒模型：Fugle SDK 是同步／執行緒式，FastAPI 是 asyncio。行情在 SDK
執行緒收，透過 Broadcaster 以 loop.call_soon_threadsafe 丟進 asyncio 佇列，
再由 /ws 的協程推給瀏覽器。
"""

from __future__ import annotations

import asyncio
import contextlib
import re
import sys
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Callable

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from app.aggregator import SymbolAggregator
from app.feed import MAX_SUBSCRIPTIONS
from app.futures import DailyCandleCache, TaifexError
from app.history import HistoryCache, HistoryError

_STATIC = Path(__file__).resolve().parent / "static"

# 代碼格式：台股 4 位數，部分權證／ETF 到 6 位。純粹是形狀檢查，擋掉打錯的
# 輸入；REST 查名稱那一步才是真正判斷「這檔股票是否存在」。
_SYMBOL_RE = re.compile(r"^\d{4,6}$")

# 同一檔在此間隔內合併多次更新，逐一推送對瀏覽器沒有意義。
PUSH_INTERVAL_SECONDS = 0.2

# 新增代碼後等待伺服器確認訂閱的上限秒數（Task 16 B）：訂閱被拒
# （Subscription limit exceeded）不可靜默留在追蹤清單裡看起來正常。
SUBSCRIBE_CONFIRM_TIMEOUT_SECONDS = 3.0
SUBSCRIBE_CONFIRM_POLL_SECONDS = 0.05


class ThresholdIn(BaseModel):
    # 單位是張，必須是正整數：沒有半張這種東西，收下 2.5 只會讓實際門檻
    # 悄悄變成 2 張或 3 張，兩種都不是使用者要求的。
    large_order_lots: int = Field(gt=0)
    # 未指定 symbol 表示套用全部；指定時只改該檔（成交量差很多的股票
    # 合理的張數門檻也差很多）。
    symbol: str | None = None


class SymbolIn(BaseModel):
    symbol: str
    # 選填；未指定時沿用啟動時的預設門檻（由 create_app 的
    # default_large_order_lots 帶入）。
    large_order_lots: int | None = Field(default=None, gt=0)


class Broadcaster:
    """跨執行緒的「某檔有更新」通知。內容不進佇列，只送 symbol，
    推送時再取當下快照，天然達成合併效果。"""

    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._queues: set[asyncio.Queue] = set()

    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop

    def subscribe(self) -> asyncio.Queue:
        queue: asyncio.Queue = asyncio.Queue()
        self._queues.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue) -> None:
        self._queues.discard(queue)

    def publish(self, symbol: str) -> None:
        """可從任何執行緒呼叫。

        兩端都要保護：web 尚未啟動時還沒有迴圈可綁（行情可能早於 uvicorn 就緒），
        關機時迴圈已關閉但 SDK 執行緒仍在送訊息。兩種情況都安靜丟棄，
        不可讓例外從 SDK 的回呼執行緒逸出。
        """
        loop = self._loop
        if loop is None or loop.is_closed():
            return
        for queue in list(self._queues):
            try:
                loop.call_soon_threadsafe(queue.put_nowait, symbol)
            except RuntimeError:
                # 迴圈在上面的檢查之後才關閉 —— 這個競從無法用檢查消除
                return


class MarketState:
    """所有聚合器狀態的持有者，並負責跨執行緒互斥。

    兩個執行緒會寫入：Fugle SDK 的行情執行緒（record_trade），
    以及 FastAPI 跑同步路由的工作執行緒（set_threshold）。所有寫入與讀取都
    走同一把鎖，否則重算大單階梯時被切斷會造成永久性的重複計數。
    """

    def __init__(self, symbols: list[str],
                 large_order_lots: int | dict[str, int],
                 names: dict[str, str] | None = None) -> None:
        self.symbols = list(symbols)
        # 純數字 = 全部同一個門檻；dict 則必須涵蓋每一檔（缺漏就 KeyError，
        # 悄悄套用某個預設值只會讓錯誤的門檻在盤中無聲生效）。
        self._thresholds = {
            s: (large_order_lots[s] if isinstance(large_order_lots, dict)
                else large_order_lots)
            for s in self.symbols
        }
        names = names or {}
        # RLock 而非 Lock：新增／移除標的（Task 14）是「檢查 → 預算檢查 →
        # REST 取名 → 建立聚合器 → 通知 feed」一整串複合動作，全部要在同一次
        # 鎖持有期間完成才不會被另一個並發請求插隊造成重複建立；route handler
        # 因此要能整段包在 `with state.lock:` 裡，同時內部呼叫
        # add_symbol/remove_symbol 這些一樣會自己上鎖的方法——同一執行緒的
        # 重入用一般 Lock 會直接死鎖，必須是 RLock。
        self.lock = threading.RLock()
        self._aggregators = {
            s: SymbolAggregator(s, self._thresholds[s], name=names.get(s, ""))
            for s in self.symbols
        }

    @property
    def thresholds(self) -> dict[str, int]:
        """逐檔門檻的複本 —— 外部改動不得影響內部狀態。"""
        with self.lock:
            return dict(self._thresholds)

    def aggregator(self, symbol: str) -> SymbolAggregator:
        """未加鎖的直接存取，僅供測試與單執行緒檢視。
        正式的寫入路徑一律用 record_trade。"""
        if symbol not in self._aggregators:
            raise KeyError(symbol)
        return self._aggregators[symbol]

    # -- 寫入（加鎖）------------------------------------------------------
    def record_trade(self, trade: dict) -> dict | None:
        """記錄一筆成交，回傳正規化紀錄；重複 serial 或未追蹤代碼回傳 None。"""
        symbol = trade.get("symbol")
        with self.lock:
            aggregator = self._aggregators.get(symbol)
            return aggregator.add_trade(trade) if aggregator is not None else None

    def set_threshold(self, threshold_lots: int, symbol: str | None = None) -> None:
        """symbol 為 None 時套用全部；指定時只重算該檔。未追蹤代碼 raise KeyError。"""
        with self.lock:
            if symbol is None:
                targets = list(self._aggregators)
            elif symbol in self._aggregators:
                targets = [symbol]
            else:
                raise KeyError(symbol)
            for name in targets:
                self._thresholds[name] = threshold_lots
                self._aggregators[name].set_threshold(threshold_lots)

    # -- 動態增刪標的（Task 14，加鎖）---------------------------------------
    def has_symbol(self, symbol: str) -> bool:
        with self.lock:
            return symbol in self._aggregators

    def add_symbol(self, symbol: str, large_order_lots: int, name: str = "") -> None:
        """建立新代碼的聚合器。已在追蹤中則 raise KeyError——正式的呼叫路徑
        應該先用 has_symbol 在同一段鎖內查過，這裡是最後一道防呆，不是
        主要的檢查點。"""
        with self.lock:
            if symbol in self._aggregators:
                raise KeyError(symbol)
            self.symbols.append(symbol)
            self._thresholds[symbol] = large_order_lots
            self._aggregators[symbol] = SymbolAggregator(symbol, large_order_lots, name=name)

    def remove_symbol(self, symbol: str) -> None:
        """移除一檔的聚合器；未追蹤則 raise KeyError。"""
        with self.lock:
            if symbol not in self._aggregators:
                raise KeyError(symbol)
            self.symbols.remove(symbol)
            del self._thresholds[symbol]
            del self._aggregators[symbol]

    # -- 讀取（加鎖）------------------------------------------------------
    def snapshot(self, symbol: str) -> dict:
        with self.lock:
            return self.aggregator(symbol).snapshot()

    def snapshot_all(self) -> dict:
        with self.lock:
            return {s: a.snapshot() for s, a in self._aggregators.items()}


def _feed_status(feed, limit: int = MAX_SUBSCRIPTIONS, *, disabled_state: str = "stopped") -> dict:
    """feed 是選配的（測試常常不需要真的接一個 FugleFeed 進來）；

    沒有 feed 時回一個「未連線」的預設值，而不是讓 /api/feed 或 ws 訊息
    整個少一個欄位——前端不必為兩種形狀各寫一份分支。disabled_state 讓期貨
    （Task 19）能區分「沒接 feed」與「使用者用 --futures "" 主動停用」——
    後者用 "disabled" 而不是 "stopped"，畫面文案才對得上使用者做過的選擇。

    訂閱用量（Task 14 D）也從這裡帶出去：/api/feed 與 ws 的 init／update
    都經過這個函式，畫面才能在同一個既有的輪詢／推播管道上顯示「訂閱 x/N」，
    不必再多開一條路徑。limit 參數化是 Task 19 加的：股票與期貨是兩條各自
    獨立的連線，訂閱上限不是同一個數字（股票 5、期貨目前設計只有 1）。
    """
    if feed is None:
        return {"state": disabled_state, "last_message_ago": None,
                "reconnects": 0, "subscription_errors": [],
                "subscription_count": 0, "subscription_limit": limit}
    payload = dict(feed.status())
    payload["subscription_count"] = feed.subscription_count
    payload["subscription_limit"] = limit
    return payload


def _futures_status(poller) -> dict:
    """期貨那一段的健康度。期貨改走期交所 MIS 輪詢（見 app/futures.py 檔頭）
    之後不再有「訂閱」的概念，所以不套用 _feed_status ——那個函式帶的
    subscription_count／subscription_limit 對輪詢沒有意義，硬填一個數字只
    會讓畫面顯示一個不存在的配額。poller 是 None（--futures "" 停用，或
    create_app 沒收到這個參數的測試）時回 "disabled"，與股票的 "stopped"
    區分得開。"""
    if poller is None:
        return {"state": "disabled", "last_message_ago": None, "reconnects": 0,
                "unavailable_reason": None, "last_error": None,
                "symbol": None, "name": None}
    return poller.status()


def _combined_feed_status(feed, futures_poller) -> dict:
    """GET /api/feed 與 ws 的 "feed" 欄位是兩段式：{"stock": ..., "futures": ...}
    ——股票（Fugle WebSocket）與期貨（期交所 MIS 輪詢）是兩條性質完全不同
    的資料流，健康度不能合併成一個指標，不然使用者分不出是哪一邊斷了。"""
    return {
        "stock": _feed_status(feed, MAX_SUBSCRIPTIONS),
        "futures": _futures_status(futures_poller),
    }


def _annotate_subscribed(snapshot: dict, symbol: str, feed) -> dict:
    """幫快照加上 subscribed: bool（Task 16 B）。

    沒有 feed 時（測試常用）視為已訂閱，不必為此另外接一個假物件；有 feed
    時看 feed.subscribed_symbols——訂閱被拒或還沒確認的代碼要能被畫面
    分辨出來，不能看起來跟正常追蹤中的代碼一樣。
    """
    subscribed = True if feed is None else symbol in feed.subscribed_symbols
    return {**snapshot, "subscribed": subscribed}


def _wait_for_subscription(feed, symbol: str,
                           timeout: float = SUBSCRIBE_CONFIRM_TIMEOUT_SECONDS) -> bool:
    """等到 feed 確認 symbol 的訂閱成功、被標記失敗，或逾時。

    刻意用短輪詢而非跨執行緒的等待物件：確認結果來自 feed 執行緒（SDK 的
    "subscribed"／"error" 事件），呼叫端在 FastAPI 的工作執行緒，這裡不持有
    MarketState 的鎖，短暫輪詢不會擋住任何其他請求或 /ws 推播。
    """
    deadline = time.monotonic() + timeout
    while True:
        if symbol in feed.subscribed_symbols:
            return True
        if symbol in feed.failed_subscriptions:
            return False
        if time.monotonic() >= deadline:
            return symbol in feed.subscribed_symbols
        time.sleep(SUBSCRIBE_CONFIRM_POLL_SECONDS)


def create_app(state: MarketState, broadcaster: Broadcaster, feed=None, *,
               pipeline=None,
               lookup_name: Callable[[str], str | None] | None = None,
               default_large_order_lots: int | None = None,
               history_api_key: str | None = None,
               futures_poller=None) -> FastAPI:
    """pipeline／lookup_name 都是選配的協作物件，讓新增／移除標的的端點能
    分別建立與關閉該代碼的 writer（pipeline.add_writer/remove_writer）、
    以 REST 查名稱（lookup_name）——兩者都注入而不是寫死 import，這樣
    web.py 本身不必知道 fugle_marketdata SDK 或 Pipeline 的具體實作，測試
    可以用假物件替換，__main__.py 才是真正接上 SDK 與檔案系統的地方。

    history_api_key 同理是選配的：GET /api/history 是純 REST（不佔訂閱
    配額），沒有 key 就無法查歷史資料，但這不該讓整個 app 建不起來
    （測試常常不需要這個端點）。HistoryCache 在這裡建立、每個 create_app
    各一份，測試裡的每個 build() 因此天然互不污染彼此的快取。

    futures_poller 同樣選配：期貨已改成「只要即時價格與 K 線」，資料走
    期交所 MIS 輪詢（見 app/futures.py 檔頭），與股票沒有任何共用狀態，
    也不再有逐筆成交可以聚合——所以它**不**是第二份 MarketState，而是一個
    只出不進的快照來源，畫面另外用 GET /api/futures 取。這麼切的好處是
    股票這一側（/ws 推播、大單門檻、動態增減標的、POST/DELETE
    /api/symbols）完全回到期貨出現之前的單純樣子，不必再逐個端點判斷
    「這個代碼屬於哪個市場」。"""
    history_cache = HistoryCache()
    futures_daily_cache = DailyCandleCache()

    def _all_market_snapshots() -> dict:
        return {symbol: {**_annotate_subscribed(snap, symbol, feed), "market": "stock"}
                for symbol, snap in state.snapshot_all().items()}

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        # 廣播器要在事件迴圈起來後才能綁定。使用 lifespan 而非
        # @app.on_event("startup")：後者在 FastAPI 0.109+ 已棄用。
        broadcaster.bind_loop(asyncio.get_running_loop())
        yield
        # 收尾必須掛在這裡，不能掛在呼叫端 uvicorn.run() 之後的 finally。
        # 實測 uvicorn 0.52.1：SIGTERM 之後它會做完整的優雅關閉（Shutting
        # down → 這個 lifespan shutdown → Finished server process），然後
        # **不從 uvicorn.run() 返回**——finally 不執行，atexit 也不執行。
        # 收尾原本就掛在 main() 的 finally，於是 ParquetTradeWriter.close()
        # 從來沒被呼叫過，每個 parquet 都停在沒有 footer 的 .partial，
        # 2026-08-12、08-13 兩個交易日的逐筆資料整批讀不出來。
        #
        # 順序不可對調：先停行情來源，最後才關 writer。反過來的話，重連
        # 迴圈或斷線補資料還可能在 close() 之後繼續送進成交，寫入已經關閉
        # 的檔案（storage.py 的 append 會擋下來只計數，但不該依賴那道保護）。
        for label, collaborator, method in (("feed", feed, "stop"),
                                             ("futures", futures_poller, "stop"),
                                             ("pipeline", pipeline, "close")):
            if collaborator is None:            # 三者都是選配，測試常常不給
                continue
            try:
                getattr(collaborator, method)()
            except Exception as error:          # noqa: BLE001 - 一個失敗不能拖累其他收尾
                print(f"關閉 {label} 時失敗：{type(error).__name__}: {error}",
                      file=sys.stderr, flush=True)

    app = FastAPI(title="台股即時大單追蹤", lifespan=lifespan)

    @app.get("/")
    def index() -> FileResponse:
        path = _STATIC / "index.html"
        if not path.exists():
            raise HTTPException(
                status_code=500,
                detail=f"static index page missing: {path}",
            )
        return FileResponse(path)

    @app.get("/api/symbols")
    def symbols() -> dict:
        # state.symbols 從 Task 14 起會在執行中變長變短；加鎖讀出一份複本，
        # 不然序列化成 JSON 的當下若正好被另一個請求 append/remove，可能讀到
        # 變動中的清單。
        with state.lock:
            return {"symbols": list(state.symbols), "thresholds": state.thresholds}

    @app.post("/api/symbols")
    def add_symbol(body: SymbolIn) -> dict:
        symbol = body.symbol
        if not _SYMBOL_RE.match(symbol):
            raise HTTPException(
                status_code=400,
                detail=f"invalid symbol format (need 4-6 digits): {symbol!r}")
        large_order_lots = body.large_order_lots or default_large_order_lots
        if large_order_lots is None:
            raise HTTPException(status_code=400, detail="large_order_lots is required")

        # 整段流程（重複檢查、預算檢查、REST 取名、建立聚合器與 writer、
        # 通知 feed 訂閱）都在同一次鎖持有期間完成：任何一步失敗都不留下
        # 半成品狀態，並發的兩個新增請求也不會同時通過檢查、各建一份。
        with state.lock:
            if state.has_symbol(symbol):
                raise HTTPException(
                    status_code=409, detail=f"symbol already tracked: {symbol}")

            used = feed.subscription_count if feed is not None else len(state.symbols)
            if used + 1 > MAX_SUBSCRIPTIONS:
                raise HTTPException(
                    status_code=409,
                    detail=f"訂閱數已達上限（目前 {used}/{MAX_SUBSCRIPTIONS}），"
                           "請先移除一檔再新增")

            name = ""
            if lookup_name is not None:
                looked_up = lookup_name(symbol)
                # 取不到名稱視為無效代碼：這同時擋掉打錯的代碼，比訂閱送出去
                # 才發現沒有資料好——那種情況整天不會有任何錯誤。
                if looked_up is None:
                    raise HTTPException(
                        status_code=404, detail=f"symbol not found: {symbol}")
                name = looked_up

            if pipeline is not None:
                pipeline.add_writer(symbol)
            state.add_symbol(symbol, large_order_lots, name=name)
            if feed is not None:
                feed.add_symbol(symbol)

        # 訂閱的確認來自 feed 執行緒的非同步事件，鎖必須先釋放才等——不然
        # 等待期間會擋住所有其他 HTTP 請求與 /ws 推播（實測缺陷：退訂被
        # 伺服器拒絕會靜默燒掉一個訂閱槽，這裡的確認同樣不可以是靜默的）。
        if feed is not None and not _wait_for_subscription(feed, symbol):
            with state.lock:
                if pipeline is not None:
                    pipeline.remove_writer(symbol)
                if state.has_symbol(symbol):
                    state.remove_symbol(symbol)
                feed.remove_symbol(symbol)
                used = feed.subscription_count
            raise HTTPException(
                status_code=409,
                detail=f"訂閱被伺服器拒絕（目前用量 {used}/{MAX_SUBSCRIPTIONS}），"
                       "請稍後重試或先移除一檔")

        broadcaster.publish(symbol)
        return {"symbols": state.symbols, "thresholds": state.thresholds}

    @app.delete("/api/symbols/{symbol}")
    def remove_symbol(symbol: str) -> dict:
        with state.lock:
            if not state.has_symbol(symbol):
                raise HTTPException(
                    status_code=404, detail=f"symbol not tracked: {symbol}")
            if feed is not None:
                feed.remove_symbol(symbol)
            if pipeline is not None:
                # 務必 close()：不關掉的話緩衝在記憶體裡還沒滿一批的成交
                # 會直接遺失，pipeline.remove_writer 內部負責呼叫 close()。
                pipeline.remove_writer(symbol)
            state.remove_symbol(symbol)
        return {"symbols": state.symbols, "thresholds": state.thresholds}

    @app.get("/api/feed")
    def feed_status() -> dict:
        return _combined_feed_status(feed, futures_poller)

    @app.get("/api/futures")
    def futures() -> dict:
        """台指期即時報價 ＋ 當日 1 分 K ＋ 均線。

        刻意做成獨立的輪詢端點而非併進 /ws 推播：來源本身就是每 5 秒輪詢
        一次的 REST（見 app/futures.py），沒有「有新成交就推」這回事，
        接進推播管道只是讓兩種節奏不同的資料共用一條路而已。回傳 status
        讓畫面在還沒有第一筆資料時也能講出原因（連線中／不可用／未啟用），
        不是留一塊空白面板。"""
        if futures_poller is None:
            return {"enabled": False, "status": _futures_status(None), "quote": None}
        return {"enabled": True, "status": futures_poller.status(),
                "quote": futures_poller.snapshot()}

    @app.get("/api/futures/daily")
    def futures_daily() -> dict:
        """台指期歷史日 K ＋均線（連續近月）。

        與 /api/futures 分開兩支端點：日 K 一天只變一次、一份約 250 根，
        塞進每 5 秒輪詢一次的即時報價裡等於每小時重傳 720 遍同樣的東西。
        這裡是阻塞式 I/O（打期交所下載 CSV，首次約 3 秒、之後走當日快取），
        路由刻意用同步 def 讓 FastAPI 丟到工作執行緒——絕不能出現在事件
        迴圈上，否則一次查詢會卡住所有 /ws 推播（與 /api/history 同理）。
        """
        if futures_poller is None:
            raise HTTPException(status_code=404, detail="期貨追蹤未啟用")
        try:
            return futures_daily_cache.get(futures_poller.product or "TXF")
        except TaifexError as error:
            raise HTTPException(status_code=502, detail=str(error)) from error

    @app.get("/api/snapshot/{symbol}")
    def snapshot(symbol: str) -> dict:
        if not state.has_symbol(symbol):
            raise HTTPException(
                status_code=404, detail=f"symbol not tracked: {symbol}"
            ) from None
        snap = state.snapshot(symbol)
        return {**_annotate_subscribed(snap, symbol, feed), "market": "stock"}

    @app.get("/api/history/{symbol}")
    def history(symbol: str) -> dict:
        """歷史日 K ＋均線，通用元件（Task 17）：純 REST，不佔 WebSocket
        訂閱配額，未追蹤的代碼也允許查詢。這是阻塞式 I/O（HTTP 打
        api.fugle.tw），路由刻意用同步 def——FastAPI 會丟到工作執行緒，
        絕不能出現在事件迴圈上，否則一次歷史查詢會卡住所有 /ws 推播與
        其他 HTTP 請求。"""
        if history_api_key is None:
            raise HTTPException(
                status_code=500, detail="history API key not configured")

        name = ""
        if lookup_name is not None:
            looked_up = lookup_name(symbol)
            # 沿用 POST /api/symbols 的判斷方式：查不到名稱＝代碼無效，
            # 回 404 比讓歷史資料 API 自己出錯好——那個錯誤訊息不是給
            # 使用者看的。
            if looked_up is None:
                raise HTTPException(
                    status_code=404, detail=f"symbol not found: {symbol}")
            name = looked_up

        try:
            return history_cache.get(history_api_key, symbol, name)
        except HistoryError as error:
            status = error.status_code if error.status_code == 404 else 502
            raise HTTPException(status_code=status, detail=str(error)) from error

    @app.post("/api/threshold")
    def set_threshold(body: ThresholdIn) -> dict:
        # 大單門檻是股票專屬的概念：期貨已不再收逐筆成交，沒有「大單」可篩。
        try:
            state.set_threshold(body.large_order_lots, body.symbol)
        except KeyError:
            raise HTTPException(
                status_code=404, detail=f"symbol not tracked: {body.symbol}"
            ) from None
        # 同上：symbols 可能在執行中被增刪，取一份複本再迭代。
        with state.lock:
            targets = ([body.symbol] if body.symbol is not None
                      else list(state.symbols))
        for symbol in targets:
            broadcaster.publish(symbol)
        return {"thresholds": state.thresholds}

    @app.websocket("/ws")
    async def stream(websocket: WebSocket) -> None:
        await websocket.accept()
        # snapshot* 會取 MarketState 的鎖，而門檻重算持鎖（實測單檔 5 萬筆
        # 27 ms）。在事件迴圈上等這把鎖會讓所有連線與所有 HTTP 請求一起停擺，
        # 因此把等待丟到工作執行緒。同步 def 路由本來就跑在工作執行緒上。
        snapshots, thresholds = await asyncio.to_thread(
            lambda: (_all_market_snapshots(), state.thresholds))
        # feed.status() 不碰 MarketState 的鎖，只讀幾個屬性，不必 to_thread。
        await websocket.send_json({"type": "init",
                                   "snapshots": snapshots,
                                   "thresholds": thresholds,
                                   "feed": _combined_feed_status(feed, futures_poller)})
        queue = broadcaster.subscribe()
        try:
            while True:
                # 只送不收就永遠看不到客戶端斷線、也看不到伺服器關機時
                # uvicorn 送進來的 disconnect 訊息，優雅關閉只能乾等
                # （實測 Ctrl-C／SIGTERM 都停不掉，只能 SIGKILL，
                # finally 沒跑，緩衝區裡的成交全部遺失）。同時等佇列與
                # 收訊息，任一完成就繼續。
                get_task = asyncio.ensure_future(queue.get())
                recv_task = asyncio.ensure_future(websocket.receive())
                try:
                    done, pending = await asyncio.wait(
                        {get_task, recv_task}, return_when=asyncio.FIRST_COMPLETED)
                except asyncio.CancelledError:
                    # 伺服器可能直接 cancel 這個協程，而不是先送 disconnect
                    # 訊息。asyncio.wait 被取消不會連帶取消傳進去的 task，
                    # 不清掉這兩個就會每個 tick 洩漏一個。
                    get_task.cancel()
                    recv_task.cancel()
                    for leftover in (get_task, recv_task):
                        with contextlib.suppress(asyncio.CancelledError):
                            await leftover
                    raise

                for leftover in pending:                  # 取消沒完成的那一個
                    leftover.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await leftover

                disconnected = False
                if recv_task in done:
                    message = recv_task.result()
                    if message["type"] == "websocket.disconnect":
                        disconnected = True

                if get_task in done:
                    symbol = get_task.result()
                    # 合併間隔內的重複通知，避免逐事件推送
                    await asyncio.sleep(PUSH_INTERVAL_SECONDS)
                    pending_symbols = {symbol}
                    while not queue.empty():
                        pending_symbols.add(queue.get_nowait())
                    for name in pending_symbols:
                        def _fetch(name=name):
                            # 綁 name=name 是必要的：這是 for 迴圈裡定義的閉包，
                            # 不綁的話所有 to_thread 呼叫會共用最後一輪迴圈的
                            # name（經典的閉包晚繫結陷阱）。
                            try:
                                snap = state.snapshot(name)
                            except KeyError:
                                # Task 14：這檔代碼可能在 publish 進佇列之後、
                                # 這裡取快照之前被 DELETE /api/symbols/{symbol}
                                # 移除，安靜跳過即可，不必讓整條 ws 連線掛掉。
                                return None
                            return {**_annotate_subscribed(snap, name, feed),
                                    "market": "stock"}

                        payload = await asyncio.to_thread(_fetch)
                        if payload is None:
                            continue
                        await websocket.send_json({
                            "type": "update", "snapshot": payload,
                            "feed": _combined_feed_status(feed, futures_poller)})

                if disconnected:
                    break
        except WebSocketDisconnect:
            pass
        finally:
            broadcaster.unsubscribe(queue)

    return app
