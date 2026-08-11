import sys
import types

import pytest

from app.history import HistoryError, fetch_daily_candles, moving_averages


def fake_rest(monkeypatch, candles):
    """比照 tests/test_main.py 的 fake_rest：用假模組換掉整個 SDK，
    不必真的打 API。"""
    sdk = types.ModuleType("fugle_marketdata")

    class RestClient:
        def __init__(self, **kwargs):
            self.stock = types.SimpleNamespace(
                historical=types.SimpleNamespace(candles=candles))

    sdk.RestClient = RestClient
    monkeypatch.setitem(sys.modules, "fugle_marketdata", sdk)


def candle(date, close, open_=None, high=None, low=None, volume=1000):
    return {"date": date, "open": open_ if open_ is not None else close,
            "high": high if high is not None else close,
            "low": low if low is not None else close,
            "close": close, "volume": volume}


# -- fetch_daily_candles ----------------------------------------------------

def test_fetch_daily_candles_sorts_ascending_by_date(monkeypatch):
    # API 本身回傳遞減（新到舊）。
    raw_desc = [candle("2026-01-03", 103), candle("2026-01-02", 102),
                candle("2026-01-01", 101)]

    def candles(symbol, **params):
        assert symbol == "2330"
        return {"data": raw_desc, "sort": "desc"}

    fake_rest(monkeypatch, candles)
    result = fetch_daily_candles("k", "2330", days=10)
    assert [c["date"] for c in result] == ["2026-01-01", "2026-01-02", "2026-01-03"]
    assert result[0]["close"] == 101


def test_fetch_daily_candles_segments_requests_over_364_days(monkeypatch):
    """days > 364 一次請求會被 API 拒絕，必須自動分段成多次請求再合併。"""
    calls = []

    def candles(symbol, **params):
        calls.append(params)
        # 每次只回一筆，日期用呼叫序號區分即可，重點是驗證呼叫次數與合併。
        return {"data": [candle(f"2020-01-0{len(calls)}", len(calls))]}

    fake_rest(monkeypatch, candles)
    result = fetch_daily_candles("k", "2330", days=400)
    assert len(calls) >= 2, "超過 364 天要分段成多次請求"
    assert [c["date"] for c in result] == sorted(c["date"] for c in result)


def test_fetch_daily_candles_wraps_api_error(monkeypatch):
    class FakeAPIError(Exception):
        def __init__(self, message, status_code):
            super().__init__(message)
            self.status_code = status_code

    def candles(symbol, **params):
        raise FakeAPIError("Resource Not Found", 404)

    fake_rest(monkeypatch, candles)
    with pytest.raises(HistoryError) as excinfo:
        fetch_daily_candles("k", "9999", days=10)
    assert excinfo.value.status_code == 404


# -- moving_averages ----------------------------------------------------

def test_moving_averages_pads_leading_none_until_enough_samples():
    candles = [candle(f"2026-01-0{i+1}", close) for i, close in enumerate([10, 11, 12, 13, 14])]
    ma = moving_averages(candles, periods=(3,))
    assert ma[3][:2] == [None, None]
    assert ma[3][2] is not None


def test_moving_averages_period_th_bar_equals_average_of_preceding_bars():
    closes = [10, 20, 30, 40, 50]
    candles = [candle(f"2026-01-0{i+1}", c) for i, c in enumerate(closes)]
    ma = moving_averages(candles, periods=(3,))
    # 第 3 根（index 2）＝前 3 根收盤平均
    assert ma[3][2] == pytest.approx((10 + 20 + 30) / 3)
    # 第 5 根（index 4）＝前 3 根（第 3、4、5 根）收盤平均
    assert ma[3][4] == pytest.approx((30 + 40 + 50) / 3)


def test_moving_averages_length_matches_candles():
    candles = [candle(f"2026-01-0{i+1}", 10 + i) for i in range(7)]
    ma = moving_averages(candles, periods=(5, 10))
    assert len(ma[5]) == len(candles)
    assert len(ma[10]) == len(candles)
    # period 10 但只有 7 根：全部都不足期數
    assert ma[10] == [None] * 7
