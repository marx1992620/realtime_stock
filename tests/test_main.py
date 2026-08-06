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
    assert args.large_order == 1_000_000


def test_parse_args_strips_whitespace_around_symbols():
    assert parse_args(["--symbols", " 2330 , 2317 "]).symbols == ["2330", "2317"]


def test_parse_args_rejects_non_positive_threshold():
    with pytest.raises(SystemExit):
        parse_args(["--symbols", "2330", "--large-order", "0"])


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
    row = pq.read_table(tmp_path / "trades_2330.parquet").to_pylist()[0]
    assert row["side"] == "buy"
    assert row["value_twd"] == 4_810_000


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
