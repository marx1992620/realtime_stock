"""Parquet -> CSV 匯出。

平時以 Parquet 落地（體積小、事後分析快），需要用 Excel 或 tail 直接看時
再匯出成 CSV，並附上台北時間欄位方便人眼閱讀。
"""

from __future__ import annotations

import argparse
import csv
import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq

from app.storage import TRADE_SCHEMA

_TAIPEI = ZoneInfo("Asia/Taipei")
CSV_FIELDS = ["time_taipei", *TRADE_SCHEMA.names]


def _taipei(microseconds: int | None) -> str:
    if microseconds is None:
        return ""
    moment = datetime.datetime.fromtimestamp(microseconds / 1_000_000,
                                             tz=datetime.timezone.utc)
    return moment.astimezone(_TAIPEI).isoformat()


def export_csv(parquet_path: Path, csv_path: Path) -> int:
    parquet_path = Path(parquet_path)
    if not parquet_path.exists():
        raise FileNotFoundError(parquet_path)
    rows = pq.read_table(parquet_path).to_pylist()
    csv_path = Path(csv_path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({"time_taipei": _taipei(row.get("time")), **row})
    return len(rows)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python3 -m app.export",
                                     description="把當日 Parquet 匯出成 CSV")
    parser.add_argument("--date", required=True, help="資料日期，例如 2026-08-05")
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    args = parser.parse_args(argv)

    day_dir = args.data_dir / args.date
    sources = sorted(day_dir.glob("*.parquet"))
    if not sources:
        raise SystemExit(f"{day_dir} 下沒有 parquet 檔")
    for source in sources:
        target = source.with_suffix(".csv")
        print(f"{source.name} -> {target.name}: {export_csv(source, target)} 列")


if __name__ == "__main__":
    main()
