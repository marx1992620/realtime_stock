"""歷史日 K 棒 + 移動平均線：REST 拉取、計算與行程內快取。

刻意設計成通用元件：只依賴 ``RestClient.stock.historical.candles``，不碰
WebSocket、不碰 MarketState。日後接上期貨合約只需要換 symbol 格式與呼叫端
（``client.futopt.historical.candles`` 之類），這支檔案的邏輯不必重寫。

延遲匯入 ``fugle_marketdata``（在函式內部才 import）是刻意的：測試可以用
``monkeypatch.setitem(sys.modules, "fugle_marketdata", fake)`` 換掉整個 SDK，
不必真的打 API，也不需要在這支檔案的最上層綁死那個套件（跟 app/__main__.py
的 fetch_names／lookup_symbol_name 是同一個手法）。
"""

from __future__ import annotations

import datetime
import threading

# API 硬性限制：單次請求的日期區間必須小於一年，超過回 400
# "Date range must be less than one year"。實測 364 天（含）可以，
# 365 天不行，所以上限抓 364，留一天安全邊界。超過時在 fetch_daily_candles
# 內自動分段、逐段往回拉，再合併。
MAX_SPAN_DAYS = 364

# 端點預設抓的天數：留夠給 120 日均線暖機，也留夠給使用者滾輪縮到「全部」
# 時有東西可看，不是只夠畫出預設的 90 根。
DEFAULT_HISTORY_DAYS = 760

DEFAULT_PERIODS: tuple[int, ...] = (5, 10, 20, 60, 120)


class HistoryError(Exception):
    """取歷史資料失敗。status_code 沿用 REST 回應的狀態碼（代碼不存在時
    通常是 404），讓呼叫端（web.py）不必認識 fugle_marketdata 的例外型別
    也能判斷要不要回 404——web.py 刻意不直接 import SDK，見該檔案開頭的
    執行緒模型說明。"""

    def __init__(self, message: str, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


def fetch_daily_candles(api_key: str, symbol: str, days: int = 365) -> list[dict]:
    """取近 `days` 天的日 K，回傳依日期**遞增**排序的
    ``[{"date","open","high","low","close","volume"}, ...]``。

    API 回傳本身是遞減（新到舊），這裡反轉過來，方便前端與均線逐根對齊。
    `days` 超過 364 時自動分段成多次請求、往回滑動日期區間，再依日期去重
    合併（區間邊界可能重疊一天）。
    """
    from fugle_marketdata import RestClient

    client = RestClient(api_key=api_key)

    end = datetime.date.today()
    remaining = days
    by_date: dict[str, dict] = {}
    while remaining > 0:
        span = min(remaining, MAX_SPAN_DAYS)
        start = end - datetime.timedelta(days=span)
        try:
            raw = client.stock.historical.candles(
                symbol=symbol, **{"from": start.isoformat(), "to": end.isoformat()})
        except Exception as error:                    # noqa: BLE001 - 轉成不依賴 SDK 型別的例外
            raise HistoryError(
                str(error), status_code=getattr(error, "status_code", None)
            ) from error

        for row in raw.get("data", []):
            by_date[row["date"]] = {
                "date": row["date"],
                "open": row["open"],
                "high": row["high"],
                "low": row["low"],
                "close": row["close"],
                "volume": row["volume"],
            }

        remaining -= span
        end = start - datetime.timedelta(days=1)       # 下一段往回滑，避免重複整段

    return [by_date[d] for d in sorted(by_date)]


def moving_averages(candles: list[dict],
                     periods: tuple[int, ...] = DEFAULT_PERIODS
                     ) -> dict[int, list[float | None]]:
    """以收盤價計算簡單移動平均。回傳長度與 candles 一致，逐根對齊。

    前 `period - 1` 根補 ``None``——不足期數的均線用較少樣本硬算會偷偷失真
    （例如第一根「5 日均線」其實只有 1 個樣本），前端也要讓這段線斷開，
    不能連到最左邊。
    """
    closes = [c["close"] for c in candles]
    result: dict[int, list[float | None]] = {}
    for period in periods:
        values: list[float | None] = []
        window_sum = 0.0
        for i, price in enumerate(closes):
            window_sum += price
            if i >= period:
                window_sum -= closes[i - period]
            values.append(window_sum / period if i >= period - 1 else None)
        result[period] = values
    return result


class HistoryCache:
    """``{(symbol, date): payload}`` 行程內快取，當日有效。

    歷史資料一天只變一次（收盤才會多一根新的日 K），不該每次開頁、每次切
    分頁都重打 REST。刻意做成類別而非模組級單例：每個 create_app() 呼叫
    （包含測試裡的每個 build()）各自建立一份，測試之間不會共用快取而互相
    污染彼此的假資料。

    只快取成功的結果：build 失敗（無效代碼、API 錯誤）時不寫入，下一次
    請求會重新嘗試，不會把一次性的錯誤釘死一整天。
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._store: dict[tuple[str, str], dict] = {}

    def get(self, api_key: str, symbol: str, name: str, *,
            days: int = DEFAULT_HISTORY_DAYS,
            periods: tuple[int, ...] = DEFAULT_PERIODS) -> dict:
        today = datetime.date.today().isoformat()
        key = (symbol, today)
        with self._lock:
            cached = self._store.get(key)
        if cached is not None:
            return cached

        candles = fetch_daily_candles(api_key, symbol, days=days)
        ma = moving_averages(candles, periods=periods)
        payload = {
            "symbol": symbol,
            "name": name,
            "candles": candles,
            "ma": {str(period): values for period, values in ma.items()},
        }

        with self._lock:
            self._store[key] = payload
        return payload
