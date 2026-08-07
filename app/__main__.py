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
from app.feed import MAX_SUBSCRIPTIONS, FugleFeed
from app.storage import ParquetTradeWriter, output_path
from app.web import Broadcaster, MarketState, create_app

DEFAULT_LARGE_ORDER_LOTS = 10


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


def _optional_symbol_list(raw: str) -> list[str]:
    """--with-book 允許留空（等於全部不訂五檔）。"""
    return [s.strip() for s in raw.split(",") if s.strip()]


def _lots(raw: str, what: str) -> int:
    """門檻的單位是張，只收正整數。

    刻意用 int() 而非 float()：'nan' 與 'inf' 都能通過 float()，且 nan 之後
    在每一次比較都回 False —— 變成永遠沒有大單卻不會有任何錯誤。'2.5' 同理
    要擋掉，沒有半張這種東西。
    """
    try:
        value = int(raw)
    except ValueError:
        raise argparse.ArgumentTypeError(f"{what}必須是整數張數：{raw!r}") from None
    if value <= 0:
        raise argparse.ArgumentTypeError(f"{what}必須大於 0：{raw!r}")
    return value


def _large_order_spec(raw: str) -> tuple[int | None, dict[str, int]]:
    """解析 --large-order：單一數字、逐檔指定，或兩者混用。

    單一門檻對成交量差很多的股票沒有意義：2330 每天幾萬張，10 張只是散戶單；
    冷門股 10 張已經是當日最大的一筆。回傳 (預設值, 逐檔覆寫)，實際對照哪些
    代碼要等 --symbols 一起看，因此在 parse_args 收尾時才驗證。
    """
    default: int | None = None
    overrides: dict[str, int] = {}
    for token in (t.strip() for t in raw.split(",")):
        if not token:
            continue
        if "=" in token:
            symbol, _, lots = token.partition("=")
            symbol = symbol.strip()
            if not symbol:
                raise argparse.ArgumentTypeError(f"缺少股票代碼：{token!r}")
            if symbol in overrides:
                raise argparse.ArgumentTypeError(f"{symbol} 的大單門檻指定了兩次")
            overrides[symbol] = _lots(lots.strip(), f"{symbol} 的大單門檻")
        else:
            if default is not None:
                raise argparse.ArgumentTypeError(
                    "預設大單門檻只能出現一次，其餘請寫成 代碼=張數")
            default = _lots(token, "大單門檻")
    if default is None and not overrides:
        raise argparse.ArgumentTypeError("大單門檻不可為空")
    return default, overrides


def fetch_names(api_key: str, symbols: list[str]) -> dict[str, str]:
    """用 REST 取每檔股票的中文名稱。REST 不佔 WebSocket 的訂閱預算。

    名稱只是標題上的裝飾：任何一步失敗都以空字串帶過，不可讓程式無法啟動，
    但要在 stderr 留下痕跡，免得使用者以為那檔股票沒有名字。
    """
    names = {symbol: "" for symbol in symbols}
    try:
        from fugle_marketdata import RestClient
        client = RestClient(api_key=api_key)
    except Exception as error:                       # noqa: BLE001 - 名稱不值得中斷啟動
        print(f"取不到股票名稱（{type(error).__name__}: {error}），改以代碼顯示",
              file=sys.stderr, flush=True)
        return names

    for symbol in symbols:
        try:
            quote = client.stock.intraday.quote(symbol=symbol) or {}
            names[symbol] = quote.get("name") or ""
        except Exception as error:                   # noqa: BLE001
            print(f"取不到 {symbol} 的名稱：{type(error).__name__}: {error}",
                  file=sys.stderr, flush=True)
    return names


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python3 -m app", description="台股多檔即時大單追蹤")
    parser.add_argument("--symbols", type=_symbol_list, default=["2330"],
                        help="逗號分隔的股票代碼，例如 2330,2317,2454")
    parser.add_argument("--large-order", type=_large_order_spec,
                        default=(DEFAULT_LARGE_ORDER_LOTS, {}),
                        help="大單門檻（張數）。可寫單一數字（全部套用）、"
                             "逐檔 2330=20,2317=5，或兩者混用 10,2330=20"
                             f"（預設 {DEFAULT_LARGE_ORDER_LOTS} 張）")
    parser.add_argument("--with-book", type=_optional_symbol_list, default=[],
                        help="要另外訂閱五檔報價的代碼，逗號分隔，例如 2330,2317。"
                             f"每個都會多佔 1 個訂閱（一條連線上限 {MAX_SUBSCRIPTIONS} 個），"
                             "未指定即全部不訂")
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

    stray = [s for s in args.with_book if s not in args.symbols]
    if stray:
        parser.error(f"--with-book 指定了不在 --symbols 內的代碼：{', '.join(stray)}")
    args.with_book = list(dict.fromkeys(args.with_book))   # 去重，重複不該多算預算

    # 訂閱預算必須在啟動「之前」擋下來：一條連線只接受 5 個訂閱，超過的會被
    # 回 Subscription limit exceeded 而靜默失效 —— 分頁照開，那些股票整天
    # 沒有資料，使用者無從察覺。
    used = len(args.symbols) + len(args.with_book)
    if used > MAX_SUBSCRIPTIONS:
        parser.error(
            f"訂閱數 {used} 超過上限 {MAX_SUBSCRIPTIONS}"
            f"（{len(args.symbols)} 檔股票 + {len(args.with_book)} 檔五檔報價）。\n"
            "每檔股票佔 1 個訂閱，每個 --with-book 再佔 1 個。\n"
            "請減少 --symbols 或 --with-book。")
    return args


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    collector.load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    api_key = os.getenv("FUGLE_API_KEY")
    if not api_key or api_key == "replace_with_your_api_key":
        raise SystemExit("FUGLE_API_KEY is not set. Copy .env.example to .env and add your API key.")

    today = datetime.date.today().isoformat()
    # REST 取名不佔 WebSocket 的訂閱預算，開連線前先問完。
    names = fetch_names(api_key, args.symbols)
    state = MarketState(args.symbols, large_order_lots=args.large_order,
                        names=names, book_symbols=args.with_book)
    broadcaster = Broadcaster()
    writers = {
        symbol: ParquetTradeWriter(output_path(args.output_dir, today, "trades", symbol))
        for symbol in args.symbols
    }
    pipeline = Pipeline(state, broadcaster, writers)
    feed = FugleFeed(api_key, args.symbols, pipeline.handle_trade,
                     pipeline.handle_book, include_trials=args.include_trials,
                     book_symbols=args.with_book)

    # 一把 API key 只能開一條連線：確認沒有其他收集器在跑，否則會被伺服器
    # 以 "Maximum number of connections reached" 斷線。
    threading.Thread(target=feed.run, daemon=True).start()
    labels = {s: f"{s} {names[s]}".strip() for s in args.symbols}
    thresholds = "、".join(f"{labels[s]} {args.large_order[s]}" for s in args.symbols)
    print(f"追蹤 {', '.join(labels[s] for s in args.symbols)}"
          f"｜大單門檻（張）{thresholds}", flush=True)
    print(f"五檔報價 {', '.join(labels[s] for s in args.with_book) or '未訂閱'}"
          f"｜訂閱數 {len(args.symbols) + len(args.with_book)}/{MAX_SUBSCRIPTIONS}",
          flush=True)
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
