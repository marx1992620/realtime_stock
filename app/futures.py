"""台指期即時行情：輪詢期交所 MIS 即時行情系統，取即時價格與 1 分 K。

=====================================================================
資料來源的實測結論（2026-08-13 盤中實地打過，不是假設）：

1. 需求指定的 https://openapi.taifex.com.tw **沒有即時行情**。整份 swagger
   共 135 個端點，全部是收盤後的日／週／月統計。與台指期價格最接近的
   /v1/DailyMarketReportFut 只回「最近一個已收盤交易日」的整日 OHLC，
   而且實測 ?date= / ?queryDate= 之類的參數一律被忽略（永遠回同一天）。
   → 即時價格不可能從 openapi 取得，改用同屬期交所的 MIS 即時行情系統。

2. MIS 即時行情（https://mis.taifex.com.tw/futures/）的 JSON 端點免金鑰、
   免登入，實測可用：
   - POST /futures/api/getQuoteList  {"CID": "TXF", ...}
       回傳台指期全部到期月份，依到期日遞增；第一個 "-F" 結尾的就是近月
       （"-S" 結尾的那筆是臺指「現貨」，不是期貨合約，必須跳過）。
       每天靠這支重新查一次就能自動換月，不必在程式裡算結算日。
   - POST /futures/api/getChartData1M {"SymbolID": "TXFH6-F"}
       **一次同時回傳即時報價（Quote）與當日全部 1 分 K（Field/Ticks）**。
       這是本模組每輪只打這一個端點就夠的原因：K 線不必自己從輪詢到的
       價格慢慢累積，程式中途啟動也能拿到從開盤第一根開始的完整當日 K。
   實測 MarketType 欄位送 "0"／"1"／"2" 或整個不送，回應完全相同（伺服器
   忽略它）；仍照網站原本的請求形狀送 "0"，減少被當成異常流量的機會。
   交易時段（Info.Sessions）也一律以伺服器回傳的為準，不在程式裡寫死
   08:45–13:45，夜盤才不會需要改這裡。

3. 這組端點沒有官方文件、不在 openapi 的清單裡，是期交所自家網站前端在用
   的介面 —— 欄位或路徑可能無預警變動。因此所有解析都走會明確丟出
   TaifexError 的路徑（缺欄位、RtCode 非 0、數字轉不動都算失敗），變動時
   畫面會顯示「期貨行情不可用：<原因>」，而不是靜靜地顯示錯誤的數字。

Task 19 的舊實作（Fugle WebSocket 逐筆成交、大單口數、契約乘數換算成交
金額、Parquet 落檔）已整份移除：需求改為「只要即時價格與 K 線，不要逐筆
交易資訊」。期貨因此不再佔用 Fugle 那條連線的訂閱配額（見 app/feed.py 的
「單一連線約束」），也不再需要 MarketState／Pipeline —— 它現在是一條與
股票完全無關、只出不進的唯讀資料流。
=====================================================================
"""

from __future__ import annotations

import csv
import datetime
import io
import json
import ssl
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

from app.history import moving_averages

MIS_BASE = "https://mis.taifex.com.tw/futures/api"

# 歷史日 K 的來源：期交所「每日行情」頁面的下載 CSV（就是那顆「下載 CSV」
# 按鈕打的位址）。走這裡而不是 openapi 的 /v1/DailyMarketReportFut，是因為
# 後者**只回最近一個已收盤交易日**、且日期參數一律被忽略（見檔頭第 1 點），
# 完全無法取得區間。實測結論（2026-08-13）：
#   - 單次查詢的區間上限是「**結束日 ≤ 起始日 + 1 個日曆月**」，超過就改回
#     一頁 HTML 錯誤頁（HTTP 仍是 200，內容不是 CSV）。實測佐證：
#     07/13→08/13（整整一個月）可以，06/11→07/12 不行（多一天），
#     06/13→07/12 又可以——用「幾天」去解釋這三筆會自相矛盾，用日曆月才一致。
#   - 編碼是 BIG5，不是 UTF-8。
#   - 一次請求約 0.14 秒、50 KB，抓滿一年十幾段也只要兩秒出頭。
TAIFEX_DAILY_URL = "https://www.taifex.com.tw/cht/3/futDataDown"

# 分段用 28 天而不是 30/31：上限是日曆月，而「30 天」在跨 2 月時會超過
# （1/31 + 30 天 = 3/2，但 1/31 + 1 個月只到 2/28）。28 天在任何月份、
# 含閏年都不可能超過一個日曆月，是唯一不必特別處理 2 月的固定天數。
DAILY_MAX_SPAN_DAYS = 28
DEFAULT_DAILY_DAYS = 365

# 日 K 的均線週期。沒有放 120：120 日均線要暖機 120 根，得再往回多抓半年
# 才畫得出第一個點，成本翻倍而多數時候只多一條貼著 60 日線的線。
DAILY_MA_PERIODS = (5, 10, 20, 60)

# MIS 的商品代號（CID）與期交所 CSV 的 commodity_id 不同名，實測對照表。
PRODUCT_TO_COMMODITY = {"TXF": "TX", "MXF": "MTX", "TMF": "TMF"}

# 商品代號（CID）：TXF＝臺股期貨（大台）。小台是 MXF、微台是 TMF，換這個
# 常數或用 --futures 覆寫即可，程式其餘部分不必動。
DEFAULT_FUTURES_PRODUCT = "TXF"

# 輪詢間隔：期交所自家網站前端也是 5 秒一次。一次請求約 10 KB，5 秒一輪
# 等於每小時 720 次、約 7 MB，對來源與本機都可以忽略；再快沒有意義，
# 因為 Ticks 的粒度本來就是 1 分鐘一根。
DEFAULT_POLL_SECONDS = 5.0
MAX_POLL_BACKOFF_SECONDS = 60.0
HTTP_TIMEOUT_SECONDS = 10.0

# 1 分 K 的均線週期。刻意不沿用日 K 的 (5,10,20,60,120)：一般交易時段只有
# 300 分鐘，120 期均線要到收盤前才畫得出一小段，佔著圖例卻幾乎沒有資訊。
MA_PERIODS = (5, 10, 20, 60)

# 超過這麼久沒有成功輪詢就在畫面上標成「停滯」。取輪詢間隔的十幾倍，
# 讓偶發的單次逾時不會閃成紅字，但真的斷掉時使用者一定看得到。
STALE_AFTER_SECONDS = 90.0

_HEADERS = {
    "Content-Type": "application/json;charset=UTF-8",
    "Accept": "application/json",
    # 期交所 MIS 會擋掉沒有來源標頭的請求；照網站原本的形狀送。
    "Origin": "https://mis.taifex.com.tw",
    "Referer": "https://mis.taifex.com.tw/futures/",
    "User-Agent": "Mozilla/5.0 (compatible; tw-stock-realtime/1.0)",
}

_ssl_lock = threading.Lock()
_ssl_context: ssl.SSLContext | None = None


class TaifexError(Exception):
    """向期交所 MIS 取資料失敗（連線、HTTP、RtCode、欄位形狀皆算）。"""


class TaifexContractError(TaifexError):
    """查不到指定商品的任何合約 —— 與連線失敗不同，這不會隨時間好轉
    （多半是 --futures 給了不存在的商品代號），輪詢迴圈收到這個就停止
    重試並轉為 "unavailable"，不要無止盡空轉。"""


def _context() -> ssl.SSLContext:
    """用 certifi 的憑證包建 SSL context（與 app/feed.py 同一個理由：
    macOS 的系統 Python 常常沒有可用的 CA 路徑，會 CERTIFICATE_VERIFY_FAILED）。
    certifi 裝不起來時退回預設 context —— 仍然驗證憑證，絕不關掉驗證。"""
    global _ssl_context
    with _ssl_lock:
        if _ssl_context is None:
            try:
                import certifi
                _ssl_context = ssl.create_default_context(cafile=certifi.where())
            except Exception:                          # noqa: BLE001
                _ssl_context = ssl.create_default_context()
        return _ssl_context


def _post(path: str, payload: dict, timeout: float = HTTP_TIMEOUT_SECONDS) -> dict:
    """打一支 MIS 端點，回傳 RtData。

    用 urllib 而非 requests／httpx 是刻意的：這兩個 POST 的請求與回應都很
    單純，為此多一個執行期依賴不划算（requirements.txt 目前沒有 HTTP
    客戶端，httpx 只在測試用的 TestClient 底下才會出現）。
    """
    body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(f"{MIS_BASE}/{path}", data=body,
                                     headers=_HEADERS, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=timeout,
                                    context=_context()) as response:
            raw = response.read()
    except (urllib.error.URLError, OSError) as error:
        raise TaifexError(f"連線期交所 MIS 失敗（{path}）："
                          f"{type(error).__name__}: {error}") from error

    try:
        event = json.loads(raw)
    except ValueError as error:
        raise TaifexError(f"期交所 MIS 回應不是合法 JSON（{path}）：{error}") from error

    code = str(event.get("RtCode", ""))
    if code != "0":
        raise TaifexError(f"期交所 MIS 回應錯誤（{path}）："
                          f"RtCode={code} {event.get('RtMsg') or ''}".strip())

    data = event.get("RtData")
    if not isinstance(data, dict):
        raise TaifexError(f"期交所 MIS 回應缺少 RtData（{path}）")
    return data


def discover_near_month(product: str = DEFAULT_FUTURES_PRODUCT,
                        timeout: float = HTTP_TIMEOUT_SECONDS) -> tuple[str, str]:
    """查 product 的近月合約，回傳 (SymbolID, 中文名)，例如
    ``("TXFH6-F", "臺指期086")``。

    回傳清單本身已依到期日遞增，所以「第一個 -F 結尾的」就是近月；不自己
    從日期推月份代碼（A–L 對應 1–12 月、末碼是民國年個位數）是刻意的：
    近月在每月第三個星期三結算後換月，自己算就得把結算日曆一起搬進來，
    而這支查詢每天只需要成功一次。
    """
    data = _post("getQuoteList", {
        "MarketType": "0", "SymbolType": "F", "KindID": "1",
        "CID": product, "ExpireMonth": "",
        "RowSize": "全部", "PageNo": "", "SortColumn": "", "AscDesc": "A",
    }, timeout)

    quotes = data.get("QuoteList") or []
    for quote in quotes:
        symbol = quote.get("SymbolID") or ""
        # "-S" 是現貨指數（例如 TXF-S 臺指現貨），不是可交易的期貨合約。
        if symbol.endswith("-F"):
            return symbol, (quote.get("DispCName") or symbol)
    raise TaifexContractError(
        f"期交所 MIS 查不到商品 {product!r} 的任何期貨合約"
        f"（回傳 {len(quotes)} 筆，沒有一筆是 -F 結尾）。"
        "請確認商品代號，例如大台 TXF、小台 MXF、微台 TMF。")


def _number(raw) -> float | None:
    """把 MIS 的字串數字轉成 float；空字串／"-"／None 一律回 None。

    尚未成交的遠月合約、盤前的部分欄位都會是空字串，那是正常狀態而不是
    錯誤，所以這裡回 None 讓呼叫端自己決定要不要當缺值，不丟例外。
    """
    if raw is None:
        return None
    text = str(raw).strip().replace(",", "")
    if not text or text in {"-", "--", "NULL"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _hhmm(raw: str) -> str:
    """Ticks 的時間欄是 "HHMMSS"（例如 "084600"），轉成 "08:46"。"""
    text = str(raw).strip()
    if len(text) >= 4 and text[:4].isdigit():
        return f"{text[:2]}:{text[2:4]}"
    return text


def parse_chart(data: dict, symbol: str, name: str = "") -> dict:
    """把 getChartData1M 的 RtData 轉成畫面要的形狀。

    K 棒欄位刻意取名 date/open/high/low/close/volume，與 app/history.py 的
    日 K 完全一致（date 這裡放的是 "HH:MM"），前端的蠟燭圖元件因此可以
    原封不動共用一份，不必為期貨另外寫一個畫圖函式。
    """
    fields = data.get("Field") or []
    index = {name_: i for i, name_ in enumerate(fields)}
    missing = [key for key in ("T", "O", "H", "L", "C", "V") if key not in index]
    if missing:
        raise TaifexError(
            f"期交所 MIS 的 1 分 K 少了欄位 {missing}（實際欄位 {fields}）"
            "；來源格式可能已變動")

    candles: list[dict] = []
    for row in data.get("Ticks") or []:
        try:
            values = {key: _number(row[index[key]]) for key in ("O", "H", "L", "C")}
            stamp = row[index["T"]]
            volume = _number(row[index["V"]])
        except (IndexError, TypeError):
            continue                       # 單一根壞掉不值得讓整輪輪詢失敗
        if any(value is None for value in values.values()):
            continue
        candles.append({
            "date": _hhmm(stamp),
            "open": values["O"], "high": values["H"],
            "low": values["L"], "close": values["C"],
            "volume": volume or 0,
        })

    quote = data.get("Quote") or {}
    last = _number(quote.get("CLastPrice"))
    reference = _number(quote.get("CRefPrice"))
    # 最新價在盤前可能還沒有，就退而用最後一根 K 的收盤 —— 兩者盤中一定
    # 一致，這只是讓剛開盤那幾秒的畫面不要空著。
    if last is None and candles:
        last = candles[-1]["close"]
    change = last - reference if (last is not None and reference is not None) else None

    ma = moving_averages(candles, periods=MA_PERIODS) if candles else {
        period: [] for period in MA_PERIODS}

    return {
        "symbol": symbol,
        "name": name or data.get("DispCName") or symbol,
        "last_price": last,
        "ref_price": reference,
        "change": change,
        "change_pct": (change / reference * 100
                       if change is not None and reference else None),
        "open": _number(quote.get("COpenPrice")),
        "high": _number(quote.get("CHighPrice")),
        "low": _number(quote.get("CLowPrice")),
        "volume": _number(quote.get("CTotalVolume")),
        # 最佳一檔買賣「價」。刻意不取對應的口數（CBidSize1／CAskSize1）與
        # OpenInterest：需求是只要價格與 K 線、不要交易的單量資訊，而
        # getChartData1M 的 OpenInterest 實測本來就一律是空字串。
        # 這支端點主要給的是 CBidPrice1／CAskPrice1，CBestBidPrice 只有
        # getQuoteDetail 才穩定有值，所以兩個都試，取先有值的那個。
        "bid": _number(quote.get("CBidPrice1")) or _number(quote.get("CBestBidPrice")),
        "ask": _number(quote.get("CAskPrice1")) or _number(quote.get("CBestAskPrice")),
        "limit_up": _number(quote.get("CCeilPrice")),
        "limit_down": _number(quote.get("CFloorPrice")),
        "date": quote.get("CDate") or "",
        "bar_time": candles[-1]["date"] if candles else "",
        "sessions": (data.get("Info") or {}).get("Sessions") or [],
        # 前端的蠟燭圖元件用這個字決定圖例要寫「5分均線」還是「5日均線」，
        # 股票的日 K payload 不帶這個欄位、預設就是日。
        "bar_unit": "分",
        "candles": candles,
        "ma": {str(period): values for period, values in ma.items()},
    }


def fetch_chart(symbol: str, name: str = "",
                timeout: float = HTTP_TIMEOUT_SECONDS) -> dict:
    """取一次某合約的即時報價 ＋ 當日 1 分 K。"""
    return parse_chart(_post("getChartData1M",
                             {"MarketType": "0", "SymbolID": symbol}, timeout),
                       symbol, name)


# -- 歷史日 K --------------------------------------------------------------

def _daily_csv(commodity_id: str, start: datetime.date, end: datetime.date,
               timeout: float = HTTP_TIMEOUT_SECONDS) -> list[dict]:
    """抓一段（最多 31 天）的每日行情 CSV，回傳原始欄位的 dict 清單。"""
    body = urllib.parse.urlencode({
        "down_type": "1",
        "commodity_id": commodity_id,
        "queryStartDate": start.strftime("%Y/%m/%d"),
        "queryEndDate": end.strftime("%Y/%m/%d"),
    }).encode("utf-8")
    request = urllib.request.Request(TAIFEX_DAILY_URL, data=body, method="POST", headers={
        "Content-Type": "application/x-www-form-urlencoded",
        "Referer": "https://www.taifex.com.tw/cht/3/futDailyMarketReport",
        "User-Agent": _HEADERS["User-Agent"],
    })
    try:
        with urllib.request.urlopen(request, timeout=timeout,
                                    context=_context()) as response:
            raw = response.read()
    except (urllib.error.URLError, OSError) as error:
        raise TaifexError(f"下載期交所每日行情失敗："
                          f"{type(error).__name__}: {error}") from error

    text = raw.decode("big5", errors="replace")
    # 區間超過上限時伺服器回的是 HTTP 200 + 一頁 HTML，不是 CSV——不先擋掉
    # 的話 csv 模組會安靜地把 HTML 解析成一堆垃圾欄位，變成「沒有資料」而
    # 不是「請求有問題」。
    if not text.lstrip().startswith("交易日期"):
        raise TaifexError(
            f"期交所每日行情回傳的不是 CSV（{start}～{end}，"
            f"開頭 {text.lstrip()[:40]!r}）；單次查詢區間不可超過一個日曆月")
    return list(csv.DictReader(io.StringIO(text)))


def fetch_daily_candles(product: str = DEFAULT_FUTURES_PRODUCT,
                        days: int = DEFAULT_DAILY_DAYS) -> list[dict]:
    """近 `days` 天的日 K，依日期**遞增**排序，欄位與 app/history.py 的股票
    日 K 相同（date/open/high/low/close/volume）。

    每個交易日一份 CSV 會有多列：不同到期月份 × 一般／盤後兩個時段。取法是
    **一般時段中成交量最大的那個到期月份**——那依定義就是近月，因此這條序列
    等同看盤軟體上的「連續近月」日 K。刻意不寫死「取最近的到期月」：每月結算
    日前後近月會換，用成交量判斷不必把結算日曆搬進來，換月當天也自然接上。
    盤後（夜盤）不併進來：日 K 的慣例是日盤，混進夜盤會讓同一天出現兩根。
    """
    commodity_id = PRODUCT_TO_COMMODITY.get(product.upper())
    if commodity_id is None:
        raise TaifexContractError(
            f"不認得的期貨商品代號 {product!r}，"
            f"歷史日 K 目前支援 {'／'.join(sorted(PRODUCT_TO_COMMODITY))}")

    end = datetime.date.today()
    remaining = days
    best: dict[str, tuple[int, dict]] = {}
    while remaining > 0:
        span = min(remaining, DAILY_MAX_SPAN_DAYS)
        start = end - datetime.timedelta(days=span)
        for row in _daily_csv(commodity_id, start, end):
            if (row.get("交易時段") or "").strip() != "一般":
                continue
            date = (row.get("交易日期") or "").strip().replace("/", "-")
            volume = _number(row.get("成交量"))
            close = _number(row.get("收盤價"))
            if not date or volume is None or close is None:
                continue
            # 同一天挑成交量最大的到期月份＝近月。
            if date not in best or volume > best[date][0]:
                best[date] = (int(volume), {
                    "date": date,
                    "open": _number(row.get("開盤價")),
                    "high": _number(row.get("最高價")),
                    "low": _number(row.get("最低價")),
                    "close": close,
                    "volume": volume,
                })
        remaining -= span
        end = start - datetime.timedelta(days=1)     # 下一段往回滑，避免重複整段

    candles = [best[date][1] for date in sorted(best)]
    # 開高低任一缺值的那一根整根丟掉：蠟燭少一個端點就畫不出來，留著只會在
    # 前端變成 NaN 座標，整條路徑消失。
    return [c for c in candles
            if all(c[key] is not None for key in ("open", "high", "low", "close"))]


class DailyCandleCache:
    """``{(product, 今天): payload}`` 行程內快取。

    日 K 一天只變一次（收盤後多一根），不該每次開頁都重抓十幾段 CSV。
    比照 app/history.py 的 HistoryCache 做成類別而非模組級單例，測試之間
    不會共用快取；同樣只快取成功的結果，一次性的失敗不會被釘死一整天。
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._store: dict[tuple[str, str], dict] = {}

    def get(self, product: str, name: str = "",
            days: int = DEFAULT_DAILY_DAYS) -> dict:
        key = (product, datetime.date.today().isoformat())
        with self._lock:
            cached = self._store.get(key)
        if cached is not None:
            return cached

        candles = fetch_daily_candles(product, days=days)
        ma = moving_averages(candles, periods=DAILY_MA_PERIODS)
        payload = {
            "symbol": product,
            "name": name or f"{product} 連續近月",
            "candles": candles,
            "ma": {str(period): values for period, values in ma.items()},
            "bar_unit": "日",
            "source": "期交所每日行情（一般交易時段，取當日成交量最大的到期月份）",
        }
        with self._lock:
            self._store[key] = payload
        return payload


class TaifexFuturesPoller:
    """背景輪詢一檔近月台指期，持有最新一份快照供 web 層讀取。

    執行緒模型：run() 阻塞，請在獨立的背景執行緒呼叫（與 app/feed.py 的
    FugleFeed 相同的用法）。它與股票行情**完全沒有共用狀態**：不碰
    MarketState、不碰 Broadcaster、不佔 Fugle 的連線或訂閱配額，任何一邊
    壞掉都不會影響另一邊。畫面取資料的路徑也是獨立的（GET /api/futures
    輪詢），不走股票那條 WebSocket 推播。

    快照用「整份替換」而非就地修改：讀取端拿到的 dict 之後不會再被寫入端
    改到，因此 snapshot() 只需要在鎖內做一次指派讀取，不必複製整份資料
    （一份含 300 根 K 棒的快照複製起來並不便宜，而且每 5 秒才換一次）。
    """

    def __init__(self, product: str = DEFAULT_FUTURES_PRODUCT,
                 interval: float = DEFAULT_POLL_SECONDS,
                 symbol: str | None = None) -> None:
        # 直接指定合約時（product 給空字串）從代碼前三碼還原商品代號：
        # "TXFI6-F" → "TXF"。歷史日 K 是按商品查的（見 fetch_daily_candles），
        # 沒有這一步就會不知道該抓哪個商品。
        if not product and symbol:
            product = symbol[:3].upper()
        self.product = product
        self.interval = interval
        # 直接指定合約（例如 --futures TXFI6-F 想看次月）時跳過近月查詢。
        self.symbol = symbol
        self.name = ""
        self._explicit_symbol = symbol is not None

        self._lock = threading.Lock()
        self._payload: dict | None = None

        self._state = "reconnecting"
        self._stop_event = threading.Event()
        self._last_ok: float | None = None
        self._last_error: str | None = None
        self._unavailable_reason: str | None = None
        self.polls = 0
        self.failures = 0

    # -- 讀取 --------------------------------------------------------------
    def snapshot(self) -> dict | None:
        with self._lock:
            return self._payload

    def status(self) -> dict:
        """給畫面用的健康度。欄位名沿用股票 feed 的 status()（state /
        last_message_ago / reconnects），前端的狀態列因此不必為兩種來源
        各寫一份分支；reconnects 對輪詢而言就是「連續失敗次數」。"""
        with self._lock:
            payload = self._payload
        last_ago = (time.monotonic() - self._last_ok
                    if self._last_ok is not None else None)
        state = self._state
        if state == "connected" and last_ago is not None and last_ago > STALE_AFTER_SECONDS:
            state = "stale"
        return {
            "state": state,
            "last_message_ago": last_ago,
            "reconnects": self.failures,
            "unavailable_reason": self._unavailable_reason,
            "last_error": self._last_error,
            "symbol": self.symbol,
            "name": (payload or {}).get("name") or self.name,
            "interval": self.interval,
            "source": "期交所 MIS 即時行情",
        }

    # -- 輪詢 --------------------------------------------------------------
    def poll_once(self) -> dict:
        """查（必要時）合約代碼、取一次資料、更新快照。失敗丟 TaifexError。

        單獨抽出來是為了讓測試不必啟動執行緒、也不必等 interval，就能驗證
        一輪的完整行為。
        """
        if self.symbol is None:
            self.symbol, self.name = discover_near_month(self.product)
            print(f"[期貨] 近月合約 {self.symbol} {self.name}", flush=True)
        payload = fetch_chart(self.symbol, self.name)
        with self._lock:
            self._payload = payload
            self._last_ok = time.monotonic()
            self._last_error = None
            self._state = "connected"
        self.polls += 1
        return payload

    def run(self) -> None:
        """阻塞式；請在背景執行緒呼叫。

        失敗時退避重試（interval 起跳、倍增到 MAX_POLL_BACKOFF_SECONDS）而
        不是像舊的 FuturesFeed 那樣數到 5 次就永久放棄：那個上限是為了
        Fugle「一把 key 只有一條連線」的配額而設的，失敗原因不會隨時間改變；
        HTTP 輪詢失敗多半是暫時的網路或來源抖動，放棄了就再也不會自己好。
        唯一的例外是 TaifexContractError（商品代號根本不存在），那個確實
        不會自己好，直接轉 "unavailable" 並結束。
        """
        backoff = self.interval
        while not self._stop_event.is_set():
            try:
                self.poll_once()
                backoff = self.interval
                wait = self.interval
            except TaifexContractError as error:
                self._mark_unavailable(str(error))
                return
            except TaifexError as error:
                self.failures += 1
                with self._lock:
                    self._last_error = str(error)
                    self._state = "reconnecting"
                print(f"[期貨] 取行情失敗（第 {self.failures} 次）：{error}",
                      file=sys.stderr, flush=True)
                wait = backoff
                backoff = min(backoff * 2.0, MAX_POLL_BACKOFF_SECONDS)
            if self._stop_event.wait(wait):
                break
        self._state = "stopped"

    def _mark_unavailable(self, reason: str) -> None:
        with self._lock:
            self._state = "unavailable"
            self._unavailable_reason = reason
            self._last_error = reason
        print(f"[期貨] 行情不可用：{reason}", file=sys.stderr, flush=True)

    def stop(self) -> None:
        self._stop_event.set()
