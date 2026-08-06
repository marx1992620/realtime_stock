import csv

import pytest

from app.export import export_csv
from app.storage import ParquetTradeWriter


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


def test_export_missing_source_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        export_csv(tmp_path / "nope.parquet", tmp_path / "out.csv")
