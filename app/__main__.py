"""CLI 進入點：一個指令同時啟動行情收集與看盤網頁。

行情在背景執行緒跑（Fugle SDK 是阻塞式），uvicorn 在主執行緒跑。
"""

from __future__ import annotations

import argparse
import datetime
import os
import sys
import threading
from pathlib import Path

import uvicorn

import collector                       # 沿用既有的 .env 載入器
from app.feed import FugleFeed
from app.storage import ParquetTradeWriter, output_path
from app.web import Broadcaster, MarketState, create_app


class Pipeline:
    """把行情事件接到聚合器、寫檔器與廣播器。"""

    def __init__(self, state: MarketState, broadcaster: Broadcaster,
                 writers: dict[str, ParquetTradeWriter]) -> None:
        self.state = state
        self.broadcaster = broadcaster
        self.writers = writers

    def handle_trade(self, trade: dict) -> None:
        # 走 MarketState 的加鎖寫入口：這個回呼在 Fugle SDK 的執行緒上，
        # 而 POST /api/threshold 會在 FastAPI 的工作執行緒上改同一份狀態。
        record = self.state.record_trade(trade)
        if record is None:            # 未追蹤代碼，或重複 serial
            return
        writer = self.writers.get(record["symbol"])
        if writer is not None:
            writer.append(record)
        self.broadcaster.publish(record["symbol"])

    def handle_book(self, book: dict) -> None:
        if self.state.record_book(book):
            self.broadcaster.publish(book["symbol"])

    def close(self) -> None:
        for writer in self.writers.values():
            writer.close()


def _symbol_list(raw: str) -> list[str]:
    symbols = [s.strip() for s in raw.split(",") if s.strip()]
    if not symbols:
        raise argparse.ArgumentTypeError("至少要指定一個股票代碼")
    return symbols


def _amount(raw: str, what: str) -> float:
    try:
        value = float(raw)
    except ValueError:
        raise argparse.ArgumentTypeError(f"{what}不是數字：{raw!r}") from None
    if value <= 0:
        raise argparse.ArgumentTypeError(f"{what}必須大於 0：{raw!r}")
    return value


def _large_order_spec(raw: str) -> tuple[float | None, dict[str, float]]:
    """解析 --large-order：單一數字、逐檔指定，或兩者混用。

    單一門檻在高低價股之間沒有意義：門檻 100 萬時 2330 一張 240 萬（每筆都是
    大單），2317 一張 25.8 萬（要 4 張才算）。回傳 (預設值, 逐檔覆寫)，實際
    對照哪些代碼要等 --symbols 一起看，因此在 parse_args 收尾時才驗證。
    """
    default: float | None = None
    overrides: dict[str, float] = {}
    for token in (t.strip() for t in raw.split(",")):
        if not token:
            continue
        if "=" in token:
            symbol, _, amount = token.partition("=")
            symbol = symbol.strip()
            if not symbol:
                raise argparse.ArgumentTypeError(f"缺少股票代碼：{token!r}")
            if symbol in overrides:
                raise argparse.ArgumentTypeError(f"{symbol} 的大單門檻指定了兩次")
            overrides[symbol] = _amount(amount.strip(), f"{symbol} 的大單門檻")
        else:
            if default is not None:
                raise argparse.ArgumentTypeError(
                    "預設大單門檻只能出現一次，其餘請寫成 代碼=金額")
            default = _amount(token, "大單門檻")
    if default is None and not overrides:
        raise argparse.ArgumentTypeError("大單門檻不可為空")
    return default, overrides


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python3 -m app", description="台股多檔即時大單追蹤")
    parser.add_argument("--symbols", type=_symbol_list, default=["2330"],
                        help="逗號分隔的股票代碼，例如 2330,2317,2454")
    parser.add_argument("--large-order", type=_large_order_spec,
                        default=(1_000_000.0, {}),
                        help="大單門檻（台幣），金額 = 價格 × 張數 × 1000。"
                             "可寫單一數字（全部套用）、逐檔 2330=5000000,2317=800000，"
                             "或兩者混用 1000000,2330=5000000")
    parser.add_argument("--output-dir", type=Path, default=Path("data"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--include-trials", action="store_true",
                        help="一併記錄開盤前試撮")
    args = parser.parse_args(argv)

    # 逐檔門檻要對上 --symbols 才知道是否齊備，這只能在兩個參數都解析完之後做。
    default, overrides = args.large_order
    unknown = [s for s in overrides if s not in args.symbols]
    if unknown:
        parser.error(f"--large-order 指定了不在 --symbols 內的代碼：{', '.join(unknown)}")
    missing = [s for s in args.symbols if s not in overrides and default is None]
    if missing:
        parser.error(f"--large-order 沒有涵蓋這些代碼，且未給預設值："
                     f"{', '.join(missing)}")
    args.large_order = {s: overrides.get(s, default) for s in args.symbols}
    return args


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    collector.load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    api_key = os.getenv("FUGLE_API_KEY")
    if not api_key or api_key == "replace_with_your_api_key":
        raise SystemExit("FUGLE_API_KEY is not set. Copy .env.example to .env and add your API key.")

    today = datetime.date.today().isoformat()
    state = MarketState(args.symbols, large_order_twd=args.large_order)
    broadcaster = Broadcaster()
    writers = {
        symbol: ParquetTradeWriter(output_path(args.output_dir, today, "trades", symbol))
        for symbol in args.symbols
    }
    pipeline = Pipeline(state, broadcaster, writers)
    feed = FugleFeed(api_key, args.symbols, pipeline.handle_trade,
                     pipeline.handle_book, include_trials=args.include_trials)

    # 一把 API key 只能開一條連線：確認沒有其他收集器在跑，否則會被伺服器
    # 以 "Maximum number of connections reached" 斷線。
    threading.Thread(target=feed.run, daemon=True).start()
    thresholds = "、".join(f"{s} {args.large_order[s]:,.0f}" for s in args.symbols)
    print(f"追蹤 {', '.join(args.symbols)}｜大單門檻（元）{thresholds}", flush=True)
    print(f"看盤畫面 http://{args.host}:{args.port}", flush=True)
    try:
        uvicorn.run(create_app(state, broadcaster), host=args.host, port=args.port,
                    log_level="warning")
    except KeyboardInterrupt:
        pass
    finally:
        pipeline.close()
        print("\n已停止，資料已寫入 " + str(args.output_dir), flush=True)


if __name__ == "__main__":
    sys.exit(main())
