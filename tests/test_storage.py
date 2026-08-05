import pyarrow.parquet as pq

from app.storage import ParquetTradeWriter, output_path


RECORD = {
    "symbol": "2330", "serial": 13674373, "time": 1785902209312276,
    "price": 2405.0, "lots": 2, "shares": 2000, "side": "buy",
    "value_twd": 4_810_000.0, "is_large": True,
}


def test_writer_persists_records(tmp_path):
    path = tmp_path / "trades_2330.parquet"
    with ParquetTradeWriter(path) as writer:
        writer.append(RECORD)
        writer.append({**RECORD, "serial": 13674374, "side": "sell", "price": 2400.0})

    table = pq.read_table(path)
    assert table.num_rows == 2
    rows = table.to_pylist()
    assert rows[0]["side"] == "buy"
    assert rows[1]["price"] == 2400.0


def test_fractional_price_survives_round_trip(tmp_path):
    path = tmp_path / "trades_2317.parquet"
    with ParquetTradeWriter(path) as writer:
        writer.append({**RECORD, "symbol": "2317", "price": 257.5})
    assert pq.read_table(path).to_pylist()[0]["price"] == 257.5


def test_auction_trade_without_serial_is_written(tmp_path):
    """集合競價那筆在部分情況沒有 serial —— 不可因此丟資料。"""
    path = tmp_path / "trades.parquet"
    with ParquetTradeWriter(path) as writer:
        writer.append({**RECORD, "serial": None, "side": "auction", "lots": 2024})
    row = pq.read_table(path).to_pylist()[0]
    assert row["serial"] is None
    assert row["lots"] == 2024


def test_records_are_buffered_until_batch_size(tmp_path):
    path = tmp_path / "trades.parquet"
    writer = ParquetTradeWriter(path, batch_size=3)
    writer.append(RECORD)
    writer.append(RECORD)
    assert not path.exists(), "未達批次量不應寫檔"
    writer.append(RECORD)
    assert path.exists(), "達到批次量應寫出"
    writer.close()
    assert pq.read_table(path).num_rows == 3


def test_close_flushes_partial_batch(tmp_path):
    path = tmp_path / "trades.parquet"
    writer = ParquetTradeWriter(path, batch_size=100)
    writer.append(RECORD)
    writer.close()
    assert pq.read_table(path).num_rows == 1


def test_close_without_records_creates_no_file(tmp_path):
    path = tmp_path / "trades.parquet"
    ParquetTradeWriter(path).close()
    assert not path.exists()


def test_output_path_layout(tmp_path):
    path = output_path(tmp_path, "2026-08-05", "trades", "2330")
    assert path == tmp_path / "2026-08-05" / "trades_2330.parquet"
