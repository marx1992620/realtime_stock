"""Fugle 行情接線。

單一連線約束：一把 API key 只能開一條 WebSocket 連線（實測超過會收到
close frame "Maximum number of connections reached"），所有股票與頻道都
必須擠在這一條上。實測單一連線同時承載 2 檔 × 2 頻道共 4 個訂閱，零錯誤。
"""

from __future__ import annotations

import collections
import json
import os
import sys
from typing import Callable

CHANNELS = ("trades", "books")

_PARSE_ERRORS = (json.JSONDecodeError, AttributeError, KeyError, TypeError, ValueError)


class FugleFeed:
    def __init__(self, api_key: str, symbols: list[str],
                 on_trade: Callable[[dict], None],
                 on_book: Callable[[dict], None],
                 include_trials: bool = False) -> None:
        self.api_key = api_key
        self.symbols = list(symbols)
        self.on_trade = on_trade
        self.on_book = on_book
        self.include_trials = include_trials
        # 被丟棄的事件必須留下痕跡：某類事件若不帶 symbol（最可能是開盤集合
        # 競價）會被整批濾掉，auction_lots 全日為 0 卻沒有任何錯誤 —— 看起來
        # 就像「今天沒有集合競價」。
        self.dropped_events: collections.Counter = collections.Counter()
        self._stock = None

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
            print(f"Fugle API error: {event.get('data')}", file=sys.stderr, flush=True)
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
        for symbol in self.symbols:
            for channel in CHANNELS:
                self._stock.subscribe({"channel": channel, "symbol": symbol})

    def run(self) -> None:
        """阻塞式連線；請在背景執行緒呼叫。"""
        import certifi
        from fugle_marketdata import WebSocketClient

        os.environ.setdefault("SSL_CERT_FILE", certifi.where())
        os.environ.setdefault("REQUESTS_CA_BUNDLE", certifi.where())

        client = WebSocketClient(api_key=self.api_key)
        self._stock = client.stock
        self._stock.on("message", self.handle_message)
        self._stock.on("authenticated", self._subscribe_all)
        self._stock.on("connect", lambda: print("Connected. Authenticating...", flush=True))
        self._stock.on("error", lambda e: print(f"WebSocket error: {e}",
                                                file=sys.stderr, flush=True))
        self._stock.connect()
