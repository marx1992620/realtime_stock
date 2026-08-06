"""Web 服務：快照 API、門檻調整、瀏覽器 WebSocket 推送。

執行緒模型：Fugle SDK 是同步／執行緒式，FastAPI 是 asyncio。行情在 SDK
執行緒收，透過 Broadcaster 以 loop.call_soon_threadsafe 丟進 asyncio 佇列，
再由 /ws 的協程推給瀏覽器。
"""

from __future__ import annotations

import asyncio
import threading
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from app.aggregator import SymbolAggregator

_STATIC = Path(__file__).resolve().parent / "static"

# 五檔每秒可有數次更新，逐一推送對瀏覽器沒有意義；同一檔在此間隔內合併。
PUSH_INTERVAL_SECONDS = 0.2


class ThresholdIn(BaseModel):
    large_order_twd: float = Field(gt=0)
    # 未指定 symbol 表示套用全部；指定時只改該檔（高低價股的合理門檻差很多）。
    symbol: str | None = None


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

    兩個執行緒會寫入：Fugle SDK 的行情執行緒（record_trade / record_book），
    以及 FastAPI 跑同步路由的工作執行緒（set_threshold）。所有寫入與讀取都
    走同一把鎖，否則重算大單階梯時被切斷會造成永久性的重複計數。
    """

    def __init__(self, symbols: list[str],
                 large_order_twd: float | dict[str, float]) -> None:
        self.symbols = list(symbols)
        # 純數字 = 全部同一個門檻；dict 則必須涵蓋每一檔（缺漏就 KeyError，
        # 悄悄套用某個預設值只會讓錯誤的門檻在盤中無聲生效）。
        self._thresholds = {
            s: (large_order_twd[s] if isinstance(large_order_twd, dict)
                else large_order_twd)
            for s in self.symbols
        }
        self._lock = threading.Lock()
        self._aggregators = {
            s: SymbolAggregator(s, self._thresholds[s]) for s in self.symbols
        }

    @property
    def thresholds(self) -> dict[str, float]:
        """逐檔門檻的複本 —— 外部改動不得影響內部狀態。"""
        with self._lock:
            return dict(self._thresholds)

    def aggregator(self, symbol: str) -> SymbolAggregator:
        """未加鎖的直接存取，僅供測試與單執行緒檢視。
        正式的寫入路徑一律用 record_trade / record_book。"""
        if symbol not in self._aggregators:
            raise KeyError(symbol)
        return self._aggregators[symbol]

    # -- 寫入（加鎖）------------------------------------------------------
    def record_trade(self, trade: dict) -> dict | None:
        """記錄一筆成交，回傳正規化紀錄；重複 serial 或未追蹤代碼回傳 None。"""
        symbol = trade.get("symbol")
        with self._lock:
            aggregator = self._aggregators.get(symbol)
            return aggregator.add_trade(trade) if aggregator is not None else None

    def record_book(self, book: dict) -> bool:
        """更新五檔快照；未追蹤代碼回傳 False。"""
        symbol = book.get("symbol")
        with self._lock:
            aggregator = self._aggregators.get(symbol)
            if aggregator is None:
                return False
            aggregator.update_book(book)
            return True

    def set_threshold(self, threshold_twd: float, symbol: str | None = None) -> None:
        """symbol 為 None 時套用全部；指定時只重算該檔。未追蹤代碼 raise KeyError。"""
        with self._lock:
            if symbol is None:
                targets = list(self._aggregators)
            elif symbol in self._aggregators:
                targets = [symbol]
            else:
                raise KeyError(symbol)
            for name in targets:
                self._thresholds[name] = threshold_twd
                self._aggregators[name].set_threshold(threshold_twd)

    # -- 讀取（加鎖）------------------------------------------------------
    def snapshot(self, symbol: str) -> dict:
        with self._lock:
            return self.aggregator(symbol).snapshot()

    def snapshot_all(self) -> dict:
        with self._lock:
            return {s: a.snapshot() for s, a in self._aggregators.items()}


def create_app(state: MarketState, broadcaster: Broadcaster) -> FastAPI:
    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        # 廣播器要在事件迴圈起來後才能綁定。使用 lifespan 而非
        # @app.on_event("startup")：後者在 FastAPI 0.109+ 已棄用。
        broadcaster.bind_loop(asyncio.get_running_loop())
        yield

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
        return {"symbols": state.symbols, "thresholds": state.thresholds}

    @app.get("/api/snapshot/{symbol}")
    def snapshot(symbol: str) -> dict:
        try:
            return state.snapshot(symbol)
        except KeyError:
            raise HTTPException(
                status_code=404, detail=f"symbol not tracked: {symbol}"
            ) from None

    @app.post("/api/threshold")
    def set_threshold(body: ThresholdIn) -> dict:
        try:
            state.set_threshold(body.large_order_twd, body.symbol)
        except KeyError:
            raise HTTPException(
                status_code=404, detail=f"symbol not tracked: {body.symbol}"
            ) from None
        for symbol in ([body.symbol] if body.symbol is not None else state.symbols):
            broadcaster.publish(symbol)
        return {"thresholds": state.thresholds}

    @app.websocket("/ws")
    async def stream(websocket: WebSocket) -> None:
        await websocket.accept()
        # snapshot* 會取 MarketState 的鎖，而門檻重算持鎖（實測單檔 5 萬筆
        # 27 ms）。在事件迴圈上等這把鎖會讓所有連線與所有 HTTP 請求一起停擺，
        # 因此把等待丟到工作執行緒。同步 def 路由本來就跑在工作執行緒上。
        snapshots, thresholds = await asyncio.to_thread(
            lambda: (state.snapshot_all(), state.thresholds))
        await websocket.send_json({"type": "init", "snapshots": snapshots,
                                   "thresholds": thresholds})
        queue = broadcaster.subscribe()
        try:
            while True:
                symbol = await queue.get()
                # 合併間隔內的重複通知，避免逐事件推送
                await asyncio.sleep(PUSH_INTERVAL_SECONDS)
                pending = {symbol}
                while not queue.empty():
                    pending.add(queue.get_nowait())
                for name in pending:
                    snapshot = await asyncio.to_thread(state.snapshot, name)
                    await websocket.send_json({"type": "update",
                                               "snapshot": snapshot})
        except WebSocketDisconnect:
            pass
        finally:
            broadcaster.unsubscribe(queue)

    return app
