import pyarrow.parquet as pq
import pytest

from app.__main__ import Pipeline, parse_args
from app.storage import ParquetTradeWriter
from app.web import Broadcaster, MarketState


AT_ASK = {"symbol": "2330", "price": 2405, "size": 2, "bid": 2400, "ask": 2405,
          "time": 1785902209312276, "serial": 1}
BOOK = {"symbol": "2330", "bids": [{"price": 2390, "size": 226}],
        "asks": [{"price": 2395, "size": 343}], "time": 2}


def test_parse_args_accepts_comma_separated_symbols():
    args = parse_args(["--symbols", "2330,2317,2454", "--large-order", "1000000"])
    assert args.symbols == ["2330", "2317", "2454"]
    # 單一數字 = 全部套用同一個門檻，解析後一律展開成逐檔對照表
    assert args.large_order == {"2330": 1_000_000, "2317": 1_000_000,
                                "2454": 1_000_000}


def test_parse_args_strips_whitespace_around_symbols():
    assert parse_args(["--symbols", " 2330 , 2317 "]).symbols == ["2330", "2317"]


def test_parse_args_rejects_non_positive_threshold():
    with pytest.raises(SystemExit):
        parse_args(["--symbols", "2330", "--large-order", "0"])


def test_parse_args_rejects_nan_threshold():
    """float("nan") <= 0 是 False，門檻會被接受；之後每筆 value_twd >= nan
    比較恆為 False，變成永遠沒有大單卻不會有任何錯誤。"""
    with pytest.raises(SystemExit):
        parse_args(["--symbols", "2330", "--large-order", "nan"])


def test_parse_args_rejects_inf_threshold():
    """同樣道理：inf 也會通過 <= 0 檢查，讓大單永遠判不出來。"""
    with pytest.raises(SystemExit):
        parse_args(["--symbols", "2330", "--large-order", "inf"])


# -- 逐檔門檻 -------------------------------------------------------------

def test_parse_args_accepts_per_symbol_thresholds():
    """2330 一張 240 萬、2317 一張 25.8 萬 —— 同一個門檻對兩者沒有意義。"""
    args = parse_args(["--symbols", "2330,2317",
                       "--large-order", "2330=5000000,2317=800000"])
    assert args.large_order == {"2330": 5_000_000, "2317": 800_000}


def test_parse_args_mixes_default_with_overrides():
    args = parse_args(["--symbols", "2330,2317,2454",
                       "--large-order", "1000000,2330=5000000"])
    assert args.large_order == {"2330": 5_000_000, "2317": 1_000_000,
                                "2454": 1_000_000}


def test_parse_args_rejects_two_defaults():
    with pytest.raises(SystemExit):
        parse_args(["--symbols", "2330", "--large-order", "1000000,2000000"])


def test_parse_args_rejects_non_positive_per_symbol_threshold():
    with pytest.raises(SystemExit):
        parse_args(["--symbols", "2330", "--large-order", "2330=0"])


def test_parse_args_rejects_symbol_without_any_threshold(capsys):
    with pytest.raises(SystemExit):
        parse_args(["--symbols", "2330,2317", "--large-order", "2330=5000000"])
    assert "2317" in capsys.readouterr().err, "報錯必須指名缺門檻的代碼"


def test_parse_args_rejects_threshold_for_untracked_symbol(capsys):
    with pytest.raises(SystemExit):
        parse_args(["--symbols", "2330", "--large-order", "1000000,2454=800000"])
    assert "2454" in capsys.readouterr().err


def test_default_threshold_applies_to_every_symbol():
    assert parse_args(["--symbols", "2330,2317"]).large_order == {
        "2330": 1_000_000, "2317": 1_000_000}


def build_pipeline(tmp_path, symbols=("2330",), threshold=1_000_000):
    state = MarketState(list(symbols), large_order_twd=threshold)
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
    assert row["value_twd"] == 4_810_000
    # 記憶體紀錄仍帶 is_large，但落檔的是可重算的 value_twd 而非當下門檻的旗標
    assert "is_large" not in table.column_names
    assert state.aggregator("2330").trades[0]["is_large"] is True


def test_pipeline_updates_book(tmp_path):
    state, pipeline = build_pipeline(tmp_path)
    pipeline.handle_book(BOOK)
    assert state.snapshot("2330")["asks"][0]["price"] == 2395


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
