"""Web 服務：快照 API、門檻調整、瀏覽器 WebSocket 推送。

執行緒模型：Fugle SDK 是同步／執行緒式，FastAPI 是 asyncio。行情在 SDK
執行緒收，透過 Broadcaster 以 loop.call_soon_threadsafe 丟進 asyncio 佇列，
再由 /ws 的協程推給瀏覽器。
"""

from __future__ import annotations

import asyncio
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
        """可從任何執行緒呼叫。web 尚未啟動時安靜丟棄。"""
        loop = self._loop
        if loop is None:
            return
        for queue in list(self._queues):
            loop.call_soon_threadsafe(queue.put_nowait, symbol)


class MarketState:
    def __init__(self, symbols: list[str], large_order_twd: float) -> None:
        self.symbols = list(symbols)
        self.large_order_twd = large_order_twd
        self._aggregators = {
            s: SymbolAggregator(s, large_order_twd) for s in self.symbols
        }

    def aggregator(self, symbol: str) -> SymbolAggregator:
        if symbol not in self._aggregators:
            raise KeyError(symbol)
        return self._aggregators[symbol]

    def snapshot(self, symbol: str) -> dict:
        return self.aggregator(symbol).snapshot()

    def snapshot_all(self) -> dict:
        return {s: a.snapshot() for s, a in self._aggregators.items()}

    def set_threshold(self, threshold_twd: float) -> None:
        self.large_order_twd = threshold_twd
        for aggregator in self._aggregators.values():
            aggregator.set_threshold(threshold_twd)


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
        return FileResponse(_STATIC / "index.html")

    @app.get("/api/symbols")
    def symbols() -> dict:
        return {"symbols": state.symbols, "large_order_twd": state.large_order_twd}

    @app.get("/api/snapshot/{symbol}")
    def snapshot(symbol: str) -> dict:
        try:
            return state.snapshot(symbol)
        except KeyError:
            raise HTTPException(status_code=404, detail=f"symbol not tracked: {symbol}")

    @app.post("/api/threshold")
    def set_threshold(body: ThresholdIn) -> dict:
        state.set_threshold(body.large_order_twd)
        for symbol in state.symbols:
            broadcaster.publish(symbol)
        return {"large_order_twd": state.large_order_twd}

    @app.websocket("/ws")
    async def stream(websocket: WebSocket) -> None:
        await websocket.accept()
        await websocket.send_json({"type": "init", "snapshots": state.snapshot_all(),
                                   "large_order_twd": state.large_order_twd})
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
                    await websocket.send_json({"type": "update",
                                               "snapshot": state.snapshot(name)})
        except WebSocketDisconnect:
            pass
        finally:
            broadcaster.unsubscribe(queue)

    return app
