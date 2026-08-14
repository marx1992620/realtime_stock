import sys
import types

import pyarrow.parquet as pq
import pytest

from app.__main__ import (MAX_SUBSCRIPTIONS, Pipeline, build_writers, fetch_names,
                          parse_args)
from app.futures import DEFAULT_FUTURES_PRODUCT, DEFAULT_POLL_SECONDS
from app.storage import ParquetTradeWriter
from app.web import Broadcaster, MarketState


AT_ASK = {"symbol": "2330", "price": 2405, "size": 2, "bid": 2400, "ask": 2405,
          "time": 1785902209312276, "serial": 1}


def test_parse_args_accepts_comma_separated_symbols():
    args = parse_args(["--symbols", "2330,2317,2454", "--large-order", "5"])
    assert args.symbols == ["2330", "2317", "2454"]
    # 單一數字 = 全部套用同一個門檻，解析後一律展開成逐檔對照表
    assert args.large_order == {"2330": 5, "2317": 5, "2454": 5}


def test_parse_args_strips_whitespace_around_symbols():
    assert parse_args(["--symbols", " 2330 , 2317 "]).symbols == ["2330", "2317"]


def test_parse_args_rejects_non_positive_threshold():
    with pytest.raises(SystemExit):
        parse_args(["--symbols", "2330", "--large-order", "0"])


def test_parse_args_rejects_fractional_lots(capsys):
    """門檻的單位是張，沒有半張這種東西。"""
    with pytest.raises(SystemExit):
        parse_args(["--symbols", "2330", "--large-order", "2.5"])
    assert "2.5" in capsys.readouterr().err


def test_parse_args_rejects_nan_threshold():
    """float("nan") <= 0 是 False，門檻會被接受；之後每筆比較恆為 False，
    變成永遠沒有大單卻不會有任何錯誤。"""
    with pytest.raises(SystemExit):
        parse_args(["--symbols", "2330", "--large-order", "nan"])


def test_parse_args_rejects_inf_threshold():
    """同樣道理：inf 也會通過 <= 0 檢查，讓大單永遠判不出來。"""
    with pytest.raises(SystemExit):
        parse_args(["--symbols", "2330", "--large-order", "inf"])


# -- 逐檔門檻 -------------------------------------------------------------

def test_parse_args_accepts_per_symbol_thresholds():
    args = parse_args(["--symbols", "2330,2317",
                       "--large-order", "2330=20,2317=3"])
    assert args.large_order == {"2330": 20, "2317": 3}


def test_parse_args_mixes_default_with_overrides():
    args = parse_args(["--symbols", "2330,2317,2454",
                       "--large-order", "5,2330=20"])
    assert args.large_order == {"2330": 20, "2317": 5, "2454": 5}


def test_parse_args_rejects_two_defaults():
    with pytest.raises(SystemExit):
        parse_args(["--symbols", "2330", "--large-order", "5,10"])


def test_parse_args_rejects_non_positive_per_symbol_threshold():
    with pytest.raises(SystemExit):
        parse_args(["--symbols", "2330", "--large-order", "2330=0"])


def test_parse_args_rejects_symbol_without_any_threshold(capsys):
    with pytest.raises(SystemExit):
        parse_args(["--symbols", "2330,2317", "--large-order", "2330=20"])
    assert "2317" in capsys.readouterr().err, "報錯必須指名缺門檻的代碼"


def test_parse_args_rejects_threshold_for_untracked_symbol(capsys):
    with pytest.raises(SystemExit):
        parse_args(["--symbols", "2330", "--large-order", "5,2454=3"])
    assert "2454" in capsys.readouterr().err


def test_default_threshold_applies_to_every_symbol():
    default = parse_args(["--symbols", "2330"]).large_order["2330"]
    assert isinstance(default, int) and default > 0
    assert parse_args(["--symbols", "2330,2317"]).large_order == {
        "2330": default, "2317": default}


# -- 期貨 ------------------------------------------------------------------

def test_futures_flag_defaults_to_the_txf_product_and_empty_string_disables():
    """--futures 收的是**商品代號**（TXF）而不是合約代碼：近月每月會換
    （TXFH6-F → TXFI6-F），要使用者自己跟著改設定沒有道理，改由
    app.futures.discover_near_month 每次啟動時查。傳空字串明確停用。"""
    assert parse_args(["--symbols", "2330"]).futures == "TXF" == DEFAULT_FUTURES_PRODUCT
    assert parse_args(["--symbols", "2330", "--futures", ""]).futures == ""


def test_futures_interval_defaults_to_the_poller_default():
    args = parse_args(["--symbols", "2330"])
    assert args.futures_interval == DEFAULT_POLL_SECONDS == 5.0


# -- 訂閱預算 -------------------------------------------------------------

def test_five_symbols_exactly_fills_the_budget():
    args = parse_args(["--symbols", "2330,2317,2301,4967,2451"])
    assert len(args.symbols) == MAX_SUBSCRIPTIONS


def test_six_symbols_overrun_the_budget(capsys):
    """實測：一把 key 的單一連線最多 5 個訂閱，第 6 個起回
    Subscription limit exceeded，程式卻照樣開分頁 —— 那些股票整天沒有資料。"""
    with pytest.raises(SystemExit):
        parse_args(["--symbols", "2330,2317,2301,4967,2451,2454"])
    err = capsys.readouterr().err
    assert "6" in err, "訊息要說明目前用量"
    assert "5" in err, "訊息要說明上限"


# -- Task 16 C：五檔報價功能已移除 --------------------------------------------

def test_with_book_flag_no_longer_exists():
    """使用者決定：訂閱上限只有 5，不該讓五檔報價跟股票搶配額——整個
    --with-book 參數已移除，傳入就是 argparse 錯誤。"""
    with pytest.raises(SystemExit):
        parse_args(["--symbols", "2330", "--with-book", "2330"])


# -- 股票名稱 -------------------------------------------------------------

def fake_rest(monkeypatch, quote):
    sdk = types.ModuleType("fugle_marketdata")

    class RestClient:
        def __init__(self, **kwargs):
            self.stock = types.SimpleNamespace(
                intraday=types.SimpleNamespace(quote=quote))

    sdk.RestClient = RestClient
    monkeypatch.setitem(sys.modules, "fugle_marketdata", sdk)


def test_fetch_names_reads_the_name_of_every_symbol(monkeypatch):
    names = {"2330": "台積電", "2317": "鴻海"}
    fake_rest(monkeypatch, lambda symbol: {"symbol": symbol, "name": names[symbol]})
    assert fetch_names("k", ["2330", "2317"]) == names


def test_fetch_names_survives_a_failing_lookup(monkeypatch, capsys):
    """REST 取名失敗不可讓程式無法啟動 —— 名稱只是標題的裝飾。"""
    def quote(symbol):
        if symbol == "2317":
            raise RuntimeError("rate limited")
        return {"name": "台積電"}

    fake_rest(monkeypatch, quote)
    assert fetch_names("k", ["2330", "2317"]) == {"2330": "台積電", "2317": ""}
    assert "2317" in capsys.readouterr().err


def test_fetch_names_survives_a_broken_sdk(monkeypatch, capsys):
    sdk = types.ModuleType("fugle_marketdata")   # 沒有 RestClient
    monkeypatch.setitem(sys.modules, "fugle_marketdata", sdk)
    assert fetch_names("k", ["2330"]) == {"2330": ""}
    assert capsys.readouterr().err                # 但要留下痕跡


def test_fetch_names_tolerates_a_quote_without_a_name(monkeypatch):
    fake_rest(monkeypatch, lambda symbol: {"symbol": symbol})
    assert fetch_names("k", ["2330"]) == {"2330": ""}


# -- Pipeline -------------------------------------------------------------

def build_pipeline(tmp_path, symbols=("2330",), threshold=1):
    state = MarketState(list(symbols), large_order_lots=threshold)
    writers = {s: ParquetTradeWriter(tmp_path / f"trades_{s}.parquet", batch_size=1)
               for s in symbols}
    return state, Pipeline(state, Broadcaster(), writers)


def test_pipeline_aggregates_and_persists_trade(tmp_path):
    state, pipeline = build_pipeline(tmp_path)
    pipeline.handle_trade(AT_ASK)
    pipeline.close()

    assert state.snapshot("2330")["ladder"][0]["buy_lots"] == 2
    table = pq.read_table(tmp_path / "trades_2330.parquet")
    row = table.to_pylist()[0]
    assert row["side"] == "buy"
    # 金額欄位保留：門檻改成張數，但落檔仍帶 value_twd，兩種門檻都能事後重算
    assert row["value_twd"] == 4_810_000
    # 記憶體紀錄仍帶 is_large，但落檔的是可重算的欄位而非當下門檻的旗標
    assert "is_large" not in table.column_names
    assert state.aggregator("2330").trades[0]["is_large"] is True


def test_pipeline_does_not_persist_duplicate_serial(tmp_path):
    _, pipeline = build_pipeline(tmp_path)
    pipeline.handle_trade(AT_ASK)
    pipeline.handle_trade(AT_ASK)
    pipeline.close()
    assert pq.read_table(tmp_path / "trades_2330.parquet").num_rows == 1


def test_pipeline_ignores_untracked_symbol(tmp_path):
    _, pipeline = build_pipeline(tmp_path)
    pipeline.handle_trade({**AT_ASK, "symbol": "9999"})   # 不可拋出
    pipeline.close()
    assert not (tmp_path / "trades_9999.parquet").exists()


# -- 每次執行獨立檔案（A）---------------------------------------------------

PARQUET_RECORD = {"symbol": "2330", "serial": 1, "time": 100, "price": 2405.0,
                  "lots": 2, "shares": 2000, "side": "buy", "value_twd": 4_810_000.0}


def test_build_writers_uses_run_id_to_avoid_same_day_collisions(tmp_path):
    """實測缺陷：檔名固定為 data/<日期>/trades_<代碼>.parquet，同一天重跑第二次
    時 pq.ParquetWriter 開檔即截斷，第一次的資料直接消失（實測 5 列變 2 列）。
    main() 啟動時應各自帶一個 run id，讓同一天的兩次執行落在不同檔案，
    彼此不得互相覆寫。"""
    first = build_writers(tmp_path, "2026-08-07", ["2330"], "090000")
    first["2330"].append(PARQUET_RECORD)
    first["2330"].append({**PARQUET_RECORD, "serial": 2})
    first["2330"].close()

    second = build_writers(tmp_path, "2026-08-07", ["2330"], "140000")
    assert second["2330"].path != first["2330"].path, "不同 run id 必須落在不同檔案"
    second["2330"].append({**PARQUET_RECORD, "serial": 100})
    second["2330"].close()

    assert pq.read_table(first["2330"].path).num_rows == 2, "前一次執行的資料不得被更動"
    assert pq.read_table(second["2330"].path).num_rows == 1
