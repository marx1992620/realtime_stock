"""台指期即時行情（期交所 MIS 輪詢）。

全部用假的 _post 取代真正的 HTTP：測試不該依賴期交所開著、也不該依賴
現在是不是交易時段。假回應直接抄 2026-08-13 盤中實際打到的形狀（見
app/futures.py 檔頭），欄位順序與空值慣例（OpenInterest 是空字串、
遠月合約的 CLastPrice 是空字串）都照原樣保留——那些正是解析容易出錯的地方。
"""

import datetime

import pytest

from app import futures as fut
from app.futures import (
    TaifexContractError,
    TaifexError,
    TaifexFuturesPoller,
    discover_near_month,
    parse_chart,
)

# 真實回應的縮小版：欄位名與空值慣例照抄，只把 Ticks 減到幾根。
CHART_RTDATA = {
    "SpotID": "", "SymbolID": "TXFH6-F",
    "DispCName": "臺指期086", "DispEName": "TX086",
    "Info": {"Status": "0", "Sessions": [{"Start": "0845", "End": "1345"}]},
    "Quote": {
        "COpenPrice": "46120.00", "CHighPrice": "46250.00", "CLowPrice": "45840.00",
        "CLastPrice": "46010.00", "CTotalVolume": "24559", "OpenInterest": "",
        "CRefPrice": "45516.00", "CCeilPrice": "50067.00", "CFloorPrice": "40965.00",
        "CDate": "20260813",
        "CBidPrice1": "46010.00", "CAskPrice1": "46014.00",
    },
    "Field": ["T", "O", "H", "L", "C", "V"],
    "Ticks": [
        ["084600", "46120.00", "46154.00", "46106.00", "46128.00", "787"],
        ["084700", "46128.00", "46130.00", "46100.00", "46110.00", "120"],
        ["084800", "46110.00", "46115.00", "46090.00", "46095.00", "64"],
    ],
}

QUOTE_LIST_RTDATA = {
    "QuoteCount": "7",
    "QuoteList": [
        # 現貨指數排在最前面，且不是可交易的合約——近月判斷必須跳過它。
        {"SymbolID": "TXF-S", "DispCName": "臺指現貨", "CLastPrice": "45964.17"},
        {"SymbolID": "TXFH6-F", "DispCName": "臺指期086", "CLastPrice": "46010.00"},
        {"SymbolID": "TXFI6-F", "DispCName": "臺指期096", "CLastPrice": "46150.00"},
    ],
}


@pytest.fixture
def fake_post(monkeypatch):
    """把 app.futures._post 換成假的，並記錄每次呼叫的 (path, payload)。"""
    calls = []
    responses = {"getQuoteList": QUOTE_LIST_RTDATA, "getChartData1M": CHART_RTDATA}

    def _post(path, payload, timeout=None):
        calls.append((path, payload))
        result = responses[path]
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(fut, "_post", _post)
    return type("FakePost", (), {"calls": calls, "responses": responses})


def test_near_month_skips_the_spot_index_and_takes_the_first_contract(fake_post):
    assert discover_near_month("TXF") == ("TXFH6-F", "臺指期086")
    path, payload = fake_post.calls[0]
    assert path == "getQuoteList"
    assert payload["CID"] == "TXF"


def test_near_month_without_any_futures_contract_raises_the_permanent_error(fake_post):
    """商品代號打錯不會隨時間好轉，必須是 TaifexContractError（輪詢迴圈
    靠這個型別決定要停止重試而不是無止盡退避重連）。"""
    fake_post.responses["getQuoteList"] = {"QuoteCount": "1", "QuoteList": [
        {"SymbolID": "TXF-S", "DispCName": "臺指現貨"}]}
    with pytest.raises(TaifexContractError):
        discover_near_month("TXF")


def test_parse_chart_maps_quote_and_one_minute_bars():
    result = parse_chart(CHART_RTDATA, "TXFH6-F", "臺指期086")

    assert result["last_price"] == 46010.0
    assert result["ref_price"] == 45516.0
    assert result["change"] == pytest.approx(494.0)
    assert result["change_pct"] == pytest.approx(494.0 / 45516.0 * 100)
    assert result["open"] == 46120.0 and result["high"] == 46250.0
    assert result["bid"] == 46010.0 and result["ask"] == 46014.0
    assert result["volume"] == 24559.0
    assert result["sessions"] == [{"Start": "0845", "End": "1345"}]

    # K 棒欄位刻意與 app/history.py 的日 K 一致（date/open/high/low/close/
    # volume），前端的蠟燭圖元件才能兩邊共用同一份；date 這裡是 "HH:MM"。
    assert result["candles"][0] == {
        "date": "08:46", "open": 46120.0, "high": 46154.0,
        "low": 46106.0, "close": 46128.0, "volume": 787.0,
    }
    assert result["bar_time"] == "08:48"
    assert result["bar_unit"] == "分"


def test_parse_chart_computes_moving_averages_with_leading_nulls():
    """均線的前 period-1 根必須是 None（不足期數硬算會偷偷失真）。
    只有 3 根 K 棒時，連 5 分均線都還畫不出來。"""
    result = parse_chart(CHART_RTDATA, "TXFH6-F")
    assert set(result["ma"]) == {"5", "10", "20", "60"}
    assert result["ma"]["5"] == [None, None, None]


def test_parse_chart_rejects_a_changed_field_layout():
    """來源沒有官方文件，欄位可能無預警變動。少了欄位要明確失敗（畫面上
    顯示「行情不可用」），不可以靜靜地拿錯位置的數字當價格。"""
    broken = {**CHART_RTDATA, "Field": ["T", "O", "H", "L"]}
    with pytest.raises(TaifexError, match="少了欄位"):
        parse_chart(broken, "TXFH6-F")


def test_parse_chart_skips_a_single_broken_bar_without_failing_the_poll():
    partial = {**CHART_RTDATA, "Ticks": [
        CHART_RTDATA["Ticks"][0],
        ["084700", "", "", "", "", ""],            # 壞掉的一根
        CHART_RTDATA["Ticks"][2],
    ]}
    candles = parse_chart(partial, "TXFH6-F")["candles"]
    assert [c["date"] for c in candles] == ["08:46", "08:48"]


def test_poll_once_discovers_the_contract_only_on_the_first_round(fake_post):
    poller = TaifexFuturesPoller("TXF")
    poller.poll_once()
    poller.poll_once()

    paths = [path for path, _ in fake_post.calls]
    assert paths == ["getQuoteList", "getChartData1M", "getChartData1M"]
    assert poller.symbol == "TXFH6-F"
    assert poller.snapshot()["last_price"] == 46010.0
    assert poller.status()["state"] == "connected"


def test_explicit_contract_skips_the_near_month_lookup(fake_post):
    poller = TaifexFuturesPoller(product="", symbol="TXFI6-F")
    poller.poll_once()
    assert [path for path, _ in fake_post.calls] == ["getChartData1M"]


def test_failed_poll_keeps_the_previous_snapshot_and_reports_the_error(fake_post):
    """一次失敗不可以把畫面清空——上一份快照仍然是最後一次已知的真實
    行情；但狀態要轉成 reconnecting，畫面才會標出「不是最新的」。"""
    poller = TaifexFuturesPoller("TXF")
    poller.poll_once()
    fake_post.responses["getChartData1M"] = TaifexError("連線期交所 MIS 失敗")

    with pytest.raises(TaifexError):
        poller.poll_once()
    assert poller.snapshot()["last_price"] == 46010.0     # 保留上一份

    # 走一次完整的 run() 迴圈確認它有把失敗記下來又不會就此停掉：讓失敗的
    # 那一輪順手叫 stop()，迴圈才會在退避等待處收工，測試不必真的睡。
    original = fut._post

    def stop_after_failing(path, payload, timeout=None):
        poller.stop()
        return original(path, payload, timeout)

    fut._post = stop_after_failing
    try:
        poller.run()
    finally:
        fut._post = original
    status = poller.status()
    assert status["reconnects"] == 1
    assert "連線期交所 MIS 失敗" in status["last_error"]
    # 這一輪是被 stop() 收掉的，所以最終狀態是 stopped 而不是 unavailable
    # ——一般的取行情失敗絕不可以讓輪詢永久放棄（見 run() 的說明）。
    assert status["state"] == "stopped"


def test_run_stops_permanently_when_the_product_does_not_exist(fake_post):
    """與舊的 FuturesFeed「數到 5 次就放棄」不同：只有這一種永久性的錯誤
    才停止，一般的網路失敗會一直退避重試（見 app/futures.py 的 run()）。"""
    fake_post.responses["getQuoteList"] = TaifexContractError("查不到商品 ZZZ")
    poller = TaifexFuturesPoller("ZZZ", interval=0.01)
    poller.run()                       # 不會卡住：碰到這個錯誤直接 return

    status = poller.status()
    assert status["state"] == "unavailable"
    assert "查不到商品 ZZZ" in status["unavailable_reason"]


# -- 歷史日 K（期交所每日行情 CSV）-----------------------------------------

DAILY_CSV_HEADER = ("交易日期,契約,到期月份(週別),開盤價,最高價,最低價,收盤價,漲跌價,漲跌%,"
                    "成交量,結算價,未沖銷契約數,最後最佳買價,最後最佳賣價,歷史最高價,"
                    "歷史最低價,是否因訊息面暫停交易,交易時段,價差對單式委託成交量")

# 欄位與空值慣例照抄真實回應（盤後的結算價／未沖銷是 "-"）。
DAILY_CSV = "\n".join([
    DAILY_CSV_HEADER,
    "2026/08/11,TX,202608  ,44761,45200,44570,45090,329,0.73%,50327,45080,95000,45088,45092,49470,39442,,一般,,",
    "2026/08/11,TX,202609  ,44900,45300,44700,45200,330,0.73%,800,45190,18000,45198,45202,49651,24962,,一般,,",
    "2026/08/11,TX,202608  ,44700,45000,44600,44900,-190,-0.42%,36518,-,-,44898,44902,49470,39442,,盤後,,",
    "2026/08/12,TX,202608  ,45350,45561,45156,45528,443,0.98%,48409,45516,95311,45519,45527,49470,39442,,一般,,",
    "2026/08/12,TX,202609  ,45460,45702,45300,45673,458,1.01%,4721,45673,18300,45668,45678,49651,24962,,一般,,",
])


def test_daily_candles_take_the_highest_volume_contract_of_the_day_session(monkeypatch):
    """每天有多列：不同到期月份 × 一般／盤後。日 K 要的是「一般時段、成交量
    最大的到期月份」＝近月，這樣每月結算換月時序列會自然接上，不必把結算
    日曆搬進程式裡。"""
    import csv, io
    monkeypatch.setattr(fut, "_daily_csv",
                        lambda cid, start, end, timeout=None:
                        list(csv.DictReader(io.StringIO(DAILY_CSV))))

    candles = fut.fetch_daily_candles("TXF", days=10)
    assert [c["date"] for c in candles] == ["2026-08-11", "2026-08-12"]
    # 08/11 取 202608（50327 口）而不是 202609（800 口），也不是盤後那列
    assert candles[0]["close"] == 45090.0 and candles[0]["volume"] == 50327.0
    assert candles[1]["close"] == 45528.0


def test_daily_candles_reject_an_unknown_product():
    with pytest.raises(TaifexContractError, match="不認得的期貨商品代號"):
        fut.fetch_daily_candles("ZZZ")


def test_daily_csv_rejects_the_html_error_page(monkeypatch):
    """區間超過一個日曆月時，伺服器回的是 HTTP 200 + 一頁 HTML。不先擋掉
    的話 csv 模組會安靜地把 HTML 解析成垃圾欄位，症狀變成「查無資料」而
    不是「請求有問題」。"""
    class FakeResponse:
        def read(self): return "<!DOCTYPE HTML PUBLIC ...".encode("big5")
        def __enter__(self): return self
        def __exit__(self, *a): return False

    monkeypatch.setattr(fut.urllib.request, "urlopen", lambda *a, **k: FakeResponse())
    with pytest.raises(TaifexError, match="不是 CSV"):
        fut._daily_csv("TX", datetime.date(2026, 1, 1), datetime.date(2026, 3, 1))


def test_daily_cache_fetches_once_per_day(monkeypatch):
    calls = []
    monkeypatch.setattr(fut, "fetch_daily_candles",
                        lambda product, days=None: calls.append(product) or [
                            {"date": "2026-08-12", "open": 1.0, "high": 2.0,
                             "low": 0.5, "close": 1.5, "volume": 10.0}])
    cache = fut.DailyCandleCache()
    first = cache.get("TXF")
    cache.get("TXF")
    assert calls == ["TXF"], "第二次應該走快取，不再打期交所"
    assert first["bar_unit"] == "日"
    assert set(first["ma"]) == {"5", "10", "20", "60"}


def test_explicit_contract_still_knows_its_product_for_daily_candles():
    """--futures TXFI6-F（直接指定合約）時 product 是空的，但歷史日 K 是按
    商品查的，得從代碼前三碼還原，否則日 K 那張圖會不知道要抓哪個商品。"""
    assert TaifexFuturesPoller(product="", symbol="TXFI6-F").product == "TXF"


def test_status_turns_stale_when_polling_stops_updating(fake_post, monkeypatch):
    poller = TaifexFuturesPoller("TXF")
    poller.poll_once()
    assert poller.status()["state"] == "connected"

    # 時間往前跳過 STALE_AFTER_SECONDS：資料還在、狀態卻必須降級，
    # 不然凍住的價格看起來會跟「盤面很安靜」一模一樣。
    now = fut.time.monotonic() + fut.STALE_AFTER_SECONDS + 1
    monkeypatch.setattr(fut.time, "monotonic", lambda: now)
    assert poller.status()["state"] == "stale"
