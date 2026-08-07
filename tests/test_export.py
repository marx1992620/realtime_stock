import csv

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from app.export import export_csv
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
    assert export_csv(source, target) == 2

    rows = list(csv.DictReader(target.open(encoding="utf-8")))
    assert [r["side"] for r in rows] == ["buy", "sell"]
    assert rows[0]["price"] == "2405.0"


def test_export_includes_taipei_time_column(tmp_path):
    source = tmp_path / "trades.parquet"
    with ParquetTradeWriter(source) as writer:
        writer.append(RECORD)
    target = tmp_path / "trades.csv"
    export_csv(source, target)
    row = next(iter(csv.DictReader(target.open(encoding="utf-8"))))
    assert row["time_taipei"].startswith("2026-08-05T11:56:49")


def test_export_has_no_is_large_column(tmp_path):
    """CSV_FIELDS 由 TRADE_SCHEMA.names 衍生 —— 欄位移除後必須自動跟隨。"""
    source = tmp_path / "trades.parquet"
    with ParquetTradeWriter(source) as writer:
        writer.append(RECORD)
    target = tmp_path / "trades.csv"
    export_csv(source, target)
    reader = csv.DictReader(target.open(encoding="utf-8"))
    assert "is_large" not in reader.fieldnames
    assert next(iter(reader))["value_twd"] == "4810000.0"


def test_export_missing_source_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        export_csv(tmp_path / "nope.parquet", tmp_path / "out.csv")


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
    assert export_csv(source, target) == 2

    reader = csv.DictReader(target.open(encoding="utf-8"))
    assert "is_large" not in reader.fieldnames
    rows = list(reader)
    assert len(rows) == 2
    assert [r["side"] for r in rows] == ["buy", "sell"]
