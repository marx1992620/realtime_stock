"""CLI 進入點：一個指令同時啟動行情收集與看盤網頁。

行情在背景執行緒跑（Fugle SDK 是阻塞式），uvicorn 在主執行緒跑。
"""

from __future__ import annotations

import argparse
import datetime
import functools
import os
import sys
import threading
from pathlib import Path
from typing import Callable

import uvicorn

import collector                       # 沿用既有的 .env 載入器
from app.feed import MAX_SUBSCRIPTIONS, FugleFeed
from app.futures import DEFAULT_FUTURES_SYMBOL, FuturesFeed, futures_trade_value_twd
from app.storage import ParquetTradeWriter, output_path
from app.web import Broadcaster, MarketState, create_app

DEFAULT_LARGE_ORDER_LOTS = 10
# 期貨大單門檻預設值與股票剛好同為 10，但語意不同（口 vs 張），刻意分開
# 一個常數，改其中一個不會意外影響另一個（Task 19 D）。
DEFAULT_FUTURES_LARGE_ORDER_LOTS = 10


class Pipeline:
    """把行情事件接到聚合器、寫檔器與廣播器。

    writers 這份字典從 Task 14 起會在執行中改變（新增／移除標的），而
    handle_trade 在 Fugle SDK 的行情執行緒讀它，POST/DELETE /api/symbols
    則在 FastAPI 的工作執行緒寫它——單純的 dict.get/[]=/pop 在 CPython 因
    GIL 本身就是原子操作，不會讀到損毀的字典，但「建立/關閉 writer 再改
    字典」是一個複合動作，用一把獨立的鎖把它與 handle_trade 的讀取序列化，
    確保不會在 writer 已經 close() 到一半時被讀到。（append 在 writer 已關閉
    後仍是安全的——storage.py 的 ParquetTradeWriter.append 會在關閉後只計數
    不寫入——這把鎖只是不必依賴那道保護，讓行為更好推理。）
    """

    def __init__(self, state: MarketState, broadcaster: Broadcaster,
                 writers: dict[str, ParquetTradeWriter],
                 output_dir: Path | None = None, date_str: str | None = None,
                 run_id: str | None = None,
                 value_twd_fn: Callable[[dict], float] | None = None) -> None:
        self.state = state
        self.broadcaster = broadcaster
        self.writers = writers
        # 動態新增的 writer 沿用同一次執行的輸出目錄／日期／run id，落在
        # 同一個 run 底下；只有呼叫 add_writer 時才需要這三個。
        self.output_dir = output_dir
        self.date_str = date_str
        self.run_id = run_id
        self._writers_lock = threading.Lock()
        # 期貨專用（Task 19 C）：SymbolAggregator.add_trade 內部一律用
        # app.classify.trade_value_twd（股票的 price*lots*1000）算 value_twd，
        # 不該為了期貨的契約乘數去改 aggregator.py。給這個 pipeline 一個
        # 專屬 Pipeline 實例，傳入一個包住
        # app.futures.futures_trade_value_twd(price, size) 的函式（接收原始
        # trade dict，回傳金額）；record_trade 拿到的紀錄物件是
        # aggregator.trades 裡的同一個 dict，這裡就地覆寫 value_twd 即可，
        # 股票用的 Pipeline 完全不受影響（不傳這個參數，行為與 Task 19 之前
        # 完全一樣）。
        self.value_twd_fn = value_twd_fn

    def handle_trade(self, trade: dict) -> None:
        # 走 MarketState 的加鎖寫入口：這個回呼在 Fugle SDK 的執行緒上，
        # 而 POST /api/threshold 會在 FastAPI 的工作執行緒上改同一份狀態。
        record = self.state.record_trade(trade)
        if record is None:            # 未追蹤代碼，或重複 serial
            return
        if self.value_twd_fn is not None:
            record["value_twd"] = self.value_twd_fn(trade)
        with self._writers_lock:
            writer = self.writers.get(record["symbol"])
        if writer is not None:
            writer.append(record)
        self.broadcaster.publish(record["symbol"])

    def add_writer(self, symbol: str) -> None:
        """新增一檔的 writer。檔名沿用 Task 12 的 output_path 規則，run id
        用啟動時那個——移除後再新增同一代碼會撞到同一個檔名，此時沿用
        ParquetTradeWriter.flush 既有的 FileExistsError 保護讓它整個往上炸，
        而不是靜默截斷；實務上盤中重複新增同代碼很少見，先不特別處理。"""
        writer = ParquetTradeWriter(
            output_path(self.output_dir, self.date_str, symbol, self.run_id))
        with self._writers_lock:
            self.writers[symbol] = writer

    def remove_writer(self, symbol: str) -> None:
        """關閉並移除一檔的 writer；務必先 close() 才能把緩衝中還沒滿一批
        的成交寫出去，否則移除當下的資料會直接遺失。"""
        with self._writers_lock:
            writer = self.writers.pop(symbol, None)
        if writer is not None:
            writer.close()

    def close(self) -> None:
        with self._writers_lock:
            writers = list(self.writers.values())
        for writer in writers:
            writer.close()


def build_writers(output_dir: Path, date_str: str, symbols: list[str],
                   run_id: str) -> dict[str, ParquetTradeWriter]:
    """每個代碼一個 writer，共用同一個 run id，落在各自的
    data/<date>/<symbol>/<run_id>.parquet —— 同一天重跑一次會是新的 run id、
    新的檔案，不會撞上前一次執行留下的資料。"""
    return {
        symbol: ParquetTradeWriter(output_path(output_dir, date_str, symbol, run_id))
        for symbol in symbols
    }


def _symbol_list(raw: str) -> list[str]:
    symbols = [s.strip() for s in raw.split(",") if s.strip()]
    if not symbols:
        raise argparse.ArgumentTypeError("至少要指定一個股票代碼")
    return symbols


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


def lookup_symbol_name(api_key: str, symbol: str) -> str | None:
    """給 POST /api/symbols 用：REST 查單一代碼的名稱。

    與 fetch_names 不同的是這裡「查不到」要能被上層分辨出來並回 404——
    盤中新增一檔打錯的代碼，比訂閱送出去才發現整天沒有資料好。REST 呼叫
    本身失敗（含代碼不存在）或回應裡沒有 symbol 欄位都視為查不到，回傳
    None 交由呼叫端判斷；名稱欄位是空字串仍算查到（代碼有效，只是沒有
    名稱），回傳空字串。
    """
    try:
        from fugle_marketdata import RestClient
        client = RestClient(api_key=api_key)
        quote = client.stock.intraday.quote(symbol=symbol) or {}
    except Exception as error:                       # noqa: BLE001
        print(f"查無 {symbol} 的行情資料：{type(error).__name__}: {error}",
              file=sys.stderr, flush=True)
        return None
    if not quote.get("symbol"):
        return None
    return quote.get("name") or ""


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
    parser.add_argument("--output-dir", type=Path, default=Path("data"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--include-trials", action="store_true",
                        help="一併記錄開盤前試撮")
    # 期貨（Task 19 D）：預設就是訂閱 TXFR1（使用者要求設為預設訂閱）；
    # 傳空字串 --futures "" 停用。與 --symbols 不同，這裡故意不用
    # _symbol_list 那個「不可為空」的型別——空字串在這裡是合法的「停用」訊號，
    # 不是打錯。走獨立的第二條 WebSocket 連線，不佔股票的 MAX_SUBSCRIPTIONS
    # 訂閱預算。
    parser.add_argument("--futures", default=DEFAULT_FUTURES_SYMBOL,
                        help=f"台指期合約代碼，預設 {DEFAULT_FUTURES_SYMBOL}"
                             "（依需求為預設訂閱）。傳空字串 --futures \"\" 可停用。")
    parser.add_argument("--futures-large-order",
                        type=lambda raw: _lots(raw, "期貨大單門檻"),
                        default=DEFAULT_FUTURES_LARGE_ORDER_LOTS,
                        help="期貨大單門檻（口），預設 "
                             f"{DEFAULT_FUTURES_LARGE_ORDER_LOTS} 口")
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

    # 訂閱預算必須在啟動「之前」擋下來：一條連線只接受 5 個訂閱（每檔股票的
    # 成交資料佔 1 個，Task 16 起五檔報價功能已移除，不再跟股票搶配額），
    # 超過的會被伺服器回 Subscription limit exceeded 而靜默失效 —— 分頁照
    # 開，那些股票整天沒有資料，使用者無從察覺。
    used = len(args.symbols)
    if used > MAX_SUBSCRIPTIONS:
        parser.error(
            f"股票代碼數 {used} 超過訂閱上限 {MAX_SUBSCRIPTIONS}"
            "（每檔股票佔 1 個訂閱）。請減少 --symbols。")

    args.futures = args.futures.strip()
    return args


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    collector.load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    api_key = os.getenv("FUGLE_API_KEY")
    if not api_key or api_key == "replace_with_your_api_key":
        raise SystemExit("FUGLE_API_KEY is not set. Copy .env.example to .env and add your API key.")

    today = datetime.date.today().isoformat()
    # 同一次執行的所有代碼共用一個 run id，方便事後從檔名辨識同一輪；同一天
    # 重跑會拿到新的 run id，落在新的檔案，不會覆寫前一次執行的資料。
    run_id = datetime.datetime.now().strftime("%H%M%S")
    # REST 取名不佔 WebSocket 的訂閱預算，開連線前先問完。
    names = fetch_names(api_key, args.symbols)
    state = MarketState(args.symbols, large_order_lots=args.large_order, names=names)
    broadcaster = Broadcaster()
    writers = build_writers(args.output_dir, today, args.symbols, run_id)
    pipeline = Pipeline(state, broadcaster, writers,
                        output_dir=args.output_dir, date_str=today, run_id=run_id)
    feed = FugleFeed(api_key, args.symbols, pipeline.handle_trade,
                     include_trials=args.include_trials)

    # 一把 API key 只能開一條連線：確認沒有其他收集器在跑，否則會被伺服器
    # 以 "Maximum number of connections reached" 斷線。feed.run() 自己會在
    # 斷線後重連，這條執行緒的壽命等於整個程式的壽命，daemon=True 只是保險
    # （正常關閉一律先呼叫 feed.stop()，見下方 finally）。
    threading.Thread(target=feed.run, daemon=True).start()
    labels = {s: f"{s} {names[s]}".strip() for s in args.symbols}
    thresholds = "、".join(f"{labels[s]} {args.large_order[s]}" for s in args.symbols)
    print(f"追蹤 {', '.join(labels[s] for s in args.symbols)}"
          f"｜大單門檻（張）{thresholds}"
          f"｜訂閱數 {len(args.symbols)}/{MAX_SUBSCRIPTIONS}", flush=True)

    # 期貨（Task 19）：獨立的第二條 WebSocket 連線、獨立的 MarketState、
    # 獨立的 Pipeline，彼此互不共享狀態——期貨連線失敗絕不可影響股票行情，
    # 見 app/futures.py 檔頭的設計決策說明。--futures "" 表示停用，三個物件
    # 都保持 None，create_app 與畫面會顯示「未啟用」而不是空白/出錯的面板。
    futures_symbol = args.futures
    futures_state: MarketState | None = None
    futures_pipeline: Pipeline | None = None
    futures_feed: FuturesFeed | None = None
    if futures_symbol:
        futures_state = MarketState([futures_symbol],
                                    large_order_lots=args.futures_large_order,
                                    names={futures_symbol: "台指期"})
        futures_writers = build_writers(args.output_dir, today, [futures_symbol], run_id)
        futures_pipeline = Pipeline(
            futures_state, broadcaster, futures_writers,
            output_dir=args.output_dir, date_str=today, run_id=run_id,
            value_twd_fn=lambda trade: futures_trade_value_twd(trade["price"], trade["size"]))
        futures_feed = FuturesFeed(api_key, futures_symbol, futures_pipeline.handle_trade)
        threading.Thread(target=futures_feed.run, daemon=True).start()
        print(f"期貨訂閱 {futures_symbol}｜大單門檻（口）{args.futures_large_order}"
              "｜連線失敗不影響股票行情，詳見畫面上的期貨分頁", flush=True)
    else:
        print("期貨追蹤已停用（--futures \"\"）", flush=True)

    print(f"看盤畫面 http://{args.host}:{args.port}", flush=True)
    app = create_app(state, broadcaster, feed, pipeline=pipeline,
                     lookup_name=functools.partial(lookup_symbol_name, api_key),
                     default_large_order_lots=DEFAULT_LARGE_ORDER_LOTS,
                     history_api_key=api_key,
                     futures_feed=futures_feed, futures_state=futures_state)
    try:
        uvicorn.run(app, host=args.host, port=args.port, log_level="warning")
    except KeyboardInterrupt:
        pass
    finally:
        # 先停 feed 再關 writer：關閉順序反過來的話，重連迴圈或補資料還可能
        # 在 writer.close() 之後繼續呼叫 handle_trade，寫進已經關閉的檔案。
        feed.stop()
        if futures_feed is not None:
            futures_feed.stop()
        pipeline.close()
        if futures_pipeline is not None:
            futures_pipeline.close()
        print("\n已停止，資料已寫入 " + str(args.output_dir), flush=True)


if __name__ == "__main__":
    sys.exit(main())
