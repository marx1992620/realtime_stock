import csv

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from app.export import export_csv, main
from app.storage import TRADE_SCHEMA, ParquetTradeWriter


RECORD = {"symbol": "2330", "serial": 1, "time": 1785902209312276, "price": 2405.0,
          "lots": 2, "shares": 2000, "side": "buy", "value_twd": 4_810_000.0,
          "is_large": True}


def test_export_writes_all_rows(tmp_path):
    source = tmp_path / "trades_2330.parquet"
    with ParquetTradeWriter(source) as writer:
        writer.append(RECORD)
        writer.append({**RECORD, "serial": 2, "side": "sell"})

    target = tmp_path / "trades_2330.csv"
    assert export_csv([source], target) == 2

    rows = list(csv.DictReader(target.open(encoding="utf-8")))
    assert [r["side"] for r in rows] == ["buy", "sell"]
    assert rows[0]["price"] == "2405.0"


def test_export_includes_taipei_time_column(tmp_path):
    source = tmp_path / "trades.parquet"
    with ParquetTradeWriter(source) as writer:
        writer.append(RECORD)
    target = tmp_path / "trades.csv"
    export_csv([source], target)
    row = next(iter(csv.DictReader(target.open(encoding="utf-8"))))
    assert row["time_taipei"].startswith("2026-08-05T11:56:49")


def test_export_has_no_is_large_column(tmp_path):
    """CSV_FIELDS 由 TRADE_SCHEMA.names 衍生 —— 欄位移除後必須自動跟隨。"""
    source = tmp_path / "trades.parquet"
    with ParquetTradeWriter(source) as writer:
        writer.append(RECORD)
    target = tmp_path / "trades.csv"
    export_csv([source], target)
    reader = csv.DictReader(target.open(encoding="utf-8"))
    assert "is_large" not in reader.fieldnames
    assert next(iter(reader))["value_twd"] == "4810000.0"


def test_export_missing_source_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        export_csv([tmp_path / "nope.parquet"], tmp_path / "out.csv")


def test_export_csv_rejects_empty_source_list(tmp_path):
    """B：main() 依代碼分組後若某代碼沒有任何檔案就不該呼叫 export_csv，
    但函式本身仍要對空清單這種呼叫方式的誤用有明確反應。"""
    with pytest.raises(ValueError):
        export_csv([], tmp_path / "out.csv")


def test_export_csv_merges_multiple_files_sorted_by_time_and_dedupes_serial(tmp_path):
    """B：改成每次執行一個檔之後，同一天、同一代碼可能有好幾個檔，export_csv
    要能接受多個來源、依 time 遞增合併。重連補資料時同一筆會出現在兩個檔裡
    （這裡以 serial 相同模擬），必須只留先出現（較早執行）的那筆。"""
    first = tmp_path / "090000.parquet"
    with ParquetTradeWriter(first) as writer:
        writer.append({**RECORD, "serial": 1, "time": 100, "price": 100.0})
        writer.append({**RECORD, "serial": 2, "time": 300, "price": 300.0})

    second = tmp_path / "140000.parquet"
    with ParquetTradeWriter(second) as writer:
        # 重連補資料造成的重疊：serial=2 其實是同一筆成交，兩個檔都有
        writer.append({**RECORD, "serial": 2, "time": 300, "price": 999.0})
        writer.append({**RECORD, "serial": 3, "time": 200, "price": 200.0})

    target = tmp_path / "2330.csv"
    assert export_csv([first, second], target) == 3, "重複的 serial 只能算一筆"

    rows = list(csv.DictReader(target.open(encoding="utf-8")))
    assert [r["serial"] for r in rows] == ["1", "3", "2"], "輸出必須依 time 遞增"
    assert rows[-1]["price"] == "300.0", "重複 serial 要保留先出現（第一個檔）的那筆"


def test_export_main_merges_every_run_file_of_a_symbol_into_one_csv(tmp_path, capsys):
    """B：main() 現在要掃 data/<date>/*/*.parquet（A 之後每個代碼是一個子目錄，
    裡面有多個 run id 檔案），依代碼分組後每個代碼輸出單一 CSV，並印出合併了
    幾個檔、共幾列。"""
    day_dir = tmp_path / "2026-08-07"
    sym_dir = day_dir / "2330"
    with ParquetTradeWriter(sym_dir / "090000.parquet") as writer:
        writer.append({**RECORD, "serial": 1, "time": 100})
    with ParquetTradeWriter(sym_dir / "140000.parquet") as writer:
        writer.append({**RECORD, "serial": 2, "time": 200})

    main(["--date", "2026-08-07", "--data-dir", str(tmp_path)])

    out = capsys.readouterr().out
    assert "2330" in out
    assert "2" in out                     # 合併了 2 個檔
    target = day_dir / "2330.csv"
    assert target.exists()
    rows = list(csv.DictReader(target.open(encoding="utf-8")))
    assert len(rows) == 2


def test_export_survives_old_schema_parquet_with_is_large(tmp_path):
    """舊版 Parquet（is_large 移除前寫的檔）落檔的每一列仍帶著 is_large 這個
    鍵，但 CSV_FIELDS 只剩現行 TRADE_SCHEMA 的八欄。csv.DictWriter 預設
    extrasaction="raise"，遇到多出來的鍵會直接 ValueError，讓一個舊檔案
    中斷整個 export 迴圈（main() 逐檔匯出，一檔炸掉就全部沒匯出）。"""
    old_schema = pa.schema(list(TRADE_SCHEMA) + [("is_large", pa.bool_())])
    source = tmp_path / "trades_old.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [RECORD, {**RECORD, "serial": 2, "side": "sell", "is_large": False}],
            schema=old_schema),
        source)

    target = tmp_path / "trades_old.csv"
    assert export_csv([source], target) == 2

    reader = csv.DictReader(target.open(encoding="utf-8"))
    assert "is_large" not in reader.fieldnames
    rows = list(reader)
    assert len(rows) == 2
    assert [r["side"] for r in rows] == ["buy", "sell"]
