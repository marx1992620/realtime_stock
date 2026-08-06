"""Fugle 行情接線。

單一連線約束：一把 API key 只能開一條 WebSocket 連線（實測超過會收到
close frame "Maximum number of connections reached"），所有股票與頻道都
必須擠在這一條上。實測單一連線同時承載 2 檔 × 2 頻道共 4 個訂閱，零錯誤。
"""

from __future__ import annotations

import json
import os
import sys
from typing import Callable

CHANNELS = ("trades", "books")


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
        self._stock = None

    # -- message handling ------------------------------------------------
    def handle_message(self, raw: str) -> None:
        """處理一則訊息。任何單則訊息的問題都不得中斷整條行情。"""
        try:
            event = json.loads(raw)
            name = event.get("event")
            if name == "authenticated":
                print("API Key authenticated.", flush=True)
                return
            if name == "subscribed":
                data = event.get("data") or {}
                print(f"Subscribed: {data.get('channel')} {data.get('symbol')}", flush=True)
                return
            if name == "error":
                print(f"Fugle API error: {event.get('data')}", file=sys.stderr, flush=True)
                return
            if name != "data":
                return

            channel = event.get("channel")
            data = event.get("data") or {}
            if data.get("symbol") not in self.symbols:
                return
            if channel == "trades":
                if data.get("isTrial", False) and not self.include_trials:
                    return
                self.on_trade(data)
            elif channel == "books":
                self.on_book(data)
        except (json.JSONDecodeError, AttributeError, KeyError, TypeError, ValueError) as error:
            # AttributeError matters: valid JSON that is not an object (a bare
            # array, or an event whose "data" is not a dict) reaches .get() and
            # would otherwise kill the stream.
            print(f"Unable to process message: {error}", file=sys.stderr, flush=True)

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
