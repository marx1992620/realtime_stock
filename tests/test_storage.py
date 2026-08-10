import pyarrow.parquet as pq
import pytest

from app.storage import TRADE_SCHEMA, ParquetTradeWriter, output_path


# 記憶體紀錄仍帶 is_large（大單階梯靠它算），落檔時必須被濾掉。
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


def test_schema_drops_is_large(tmp_path):
    """門檻可即時調整，已落檔的旗標會停留在寫入當下的門檻上 —— 同一個檔案
    可能混了多個門檻且無處可查。value_twd 已落檔，任何門檻都能事後算出。"""
    assert "is_large" not in TRADE_SCHEMA.names
    assert TRADE_SCHEMA.names == ["symbol", "serial", "time", "price", "lots",
                                  "shares", "side", "value_twd"]

    path = tmp_path / "trades.parquet"
    with ParquetTradeWriter(path) as writer:
        writer.append(RECORD)                 # 多出來的鍵必須被忽略而非拋錯
    table = pq.read_table(path)
    assert "is_large" not in table.column_names
    assert table.to_pylist()[0]["value_twd"] == 4_810_000.0


def test_append_after_close_never_truncates_the_finished_file(tmp_path):
    """行情執行緒是 daemon，close() 之後仍可能送進成交。舊行為會在湊滿批次時
    以同一路徑重開 ParquetWriter，把已經寫完的檔案截斷成讀不出來的殘骸。"""
    path = tmp_path / "trades.parquet"
    writer = ParquetTradeWriter(path, batch_size=2)
    writer.append(RECORD)
    writer.append({**RECORD, "serial": 2})
    writer.close()
    good_size = path.stat().st_size

    for serial in range(10, 15):                      # 關檔後才進來的五筆
        writer.append({**RECORD, "serial": serial})

    # 先驗檔案本身：修正前這裡讀到的是 ArrowInvalid（footer 沒有 magic bytes）
    assert path.stat().st_size == good_size, "關檔後的 append 不得再動檔案"
    assert pq.read_table(path).num_rows == 2
    assert writer.dropped_after_close == 5


def test_records_dropped_after_close_are_announced(tmp_path, capsys):
    """遺失必須是看得見的：正式流程只會 close 一次，所以第一筆丟棄要當下就說，
    再次關檔時報總數。"""
    path = tmp_path / "trades.parquet"
    writer = ParquetTradeWriter(path, batch_size=100)
    writer.append(RECORD)
    writer.close()
    capsys.readouterr()

    writer.append(RECORD)
    assert "after close" in capsys.readouterr().err
    writer.append(RECORD)
    assert capsys.readouterr().err == "", "之後只累加，不重複洗版"

    writer.close()
    assert "2" in capsys.readouterr().err


def test_failed_flush_loses_only_that_batch(tmp_path, capsys):
    """毒化的緩衝區若留著，之後每次 flush 都會再拋一次，整日不再落檔。"""
    path = tmp_path / "trades.parquet"
    writer = ParquetTradeWriter(path, batch_size=1)
    writer.append(RECORD)                              # 開檔，正常寫入

    original = writer._writer.write_table
    writer._writer.write_table = lambda table: (_ for _ in ()).throw(
        RuntimeError("disk full"))
    writer.append({**RECORD, "serial": 2})             # 不得拋出
    assert writer._buffer == [], "寫入失敗的批次必須被丟掉，不可留在緩衝區"
    assert "disk full" in capsys.readouterr().err

    writer._writer.write_table = original
    writer.append({**RECORD, "serial": 3})             # 之後仍能繼續落檔
    writer.close()
    assert [r["serial"] for r in pq.read_table(path).to_pylist()] == [13674373, 3]


def test_closed_flag_is_set_before_flush_can_raise(tmp_path):
    """close() 目前是 flush() -> writer.close() -> self._closed = True。
    flush() 或 writer.close() 任一失敗都會讓 _closed 永遠設不到，daemon
    行情執行緒仍可能通過關檔後的 append 護欄，湊滿批次時重開同路徑的
    ParquetWriter，把已經寫完的檔案截斷成殘骸。_closed 必須是 close() 的
    第一步，不管後面是否拋出。"""
    path = tmp_path / "trades.parquet"
    writer = ParquetTradeWriter(path, batch_size=100)   # 不會自動觸發 flush
    writer.append(RECORD)

    def boom():
        raise RuntimeError("flush blew up")
    writer.flush = boom

    with pytest.raises(RuntimeError):
        writer.close()

    assert writer._closed is True, "即使 flush() 拋出，_closed 也必須已經被設定"
    assert not path.exists(), "flush 失敗、從未真的開檔，不該有新檔案冒出來"

    writer.append(RECORD)                                # 之後仍視為關檔後
    assert writer.dropped_after_close == 1
    assert not path.exists(), "關檔後的 append 不得寫檔"


def test_output_path_layout(tmp_path):
    """A：每次執行、每個代碼各自一個檔案，路徑帶 run id 才不會同一天重跑互相覆寫。"""
    path = output_path(tmp_path, "2026-08-05", "2330", "093000")
    assert path == tmp_path / "2026-08-05" / "2330" / "093000.parquet"


def test_flush_raises_if_target_file_already_exists(tmp_path):
    """A 的最後一道防線：run id 正常不會碰撞，但萬一撞了，flush() 首次建檔前
    必須拒絕覆寫既有檔案，而不是靜默截斷（實測 5 列變 2 列）。"""
    path = tmp_path / "trades.parquet"
    path.write_bytes(b"pretend this is yesterday's finished parquet file")
    writer = ParquetTradeWriter(path, batch_size=1)
    with pytest.raises(FileExistsError):
        writer.append(RECORD)


def test_flush_interval_seconds_zero_flushes_on_first_append(tmp_path):
    """D：即使關閉流程正確，batch_size=500 仍可能損失最多 500 筆。
    flush_interval_seconds=0 時，逾時判斷必須立即成立，第一筆 append 就寫出，
    不必等到湊滿批次。"""
    path = tmp_path / "trades.parquet"
    writer = ParquetTradeWriter(path, batch_size=100, flush_interval_seconds=0)
    writer.append(RECORD)
    assert path.exists(), "flush_interval_seconds=0 應在第一筆 append 就觸發寫出"
    assert writer._buffer == [], "第一筆就該被 flush 出去，緩衝區應已清空"
    writer.close()
    assert pq.read_table(path).num_rows == 1


def test_flush_interval_does_not_trigger_early_with_default(tmp_path):
    """既有的批次量測試不可被逾時判斷破壞：預設 30 秒的視窗內不該提早寫出。"""
    path = tmp_path / "trades.parquet"
    writer = ParquetTradeWriter(path, batch_size=100)
    writer.append(RECORD)
    assert not path.exists(), "未逾時、未達批次量不應寫檔"
    writer.close()
