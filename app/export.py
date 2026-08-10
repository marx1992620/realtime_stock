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


def export_csv(parquet_paths: list[Path], csv_path: Path) -> int:
    """把多個 Parquet 檔合併成一份 CSV，依 time 遞增排序。

    A 改成每次執行、每個代碼各自一個檔案之後，同一天、同一代碼可能有好幾個
    run id 的檔案，匯出必須把它們全部讀進來合併，不能只認一個檔案而漏資料。
    重連補資料時同一筆成交可能出現在兩個檔裡（同一個 serial），依 time
    排序後只保留先出現（較早執行）的那筆。
    """
    paths = [Path(p) for p in parquet_paths]
    if not paths:
        raise ValueError("parquet_paths must not be empty")
    for path in paths:
        if not path.exists():
            raise FileNotFoundError(path)

    all_rows: list[dict] = []
    for path in paths:
        all_rows.extend(pq.read_table(path).to_pylist())
    all_rows.sort(key=lambda row: row.get("time") or 0)   # 穩定排序，同時間保留原順序

    seen_serials: set[tuple] = set()
    rows: list[dict] = []
    for row in all_rows:
        serial = row.get("serial")
        if serial is not None:                             # 集合競價可能沒有 serial，
            key = (row.get("symbol"), serial)                # 不能拿 None 互相去重
            if key in seen_serials:
                continue
            seen_serials.add(key)
        rows.append(row)

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
    # A 之後的佈局是 data/<date>/<symbol>/<run_id>.parquet，一個代碼一個子目錄。
    sources = sorted(day_dir.glob("*/*.parquet"))
    if not sources:
        raise SystemExit(f"{day_dir} 下沒有 parquet 檔")

    groups: dict[str, list[Path]] = {}
    for path in sources:
        groups.setdefault(path.parent.name, []).append(path)

    for symbol in sorted(groups):
        paths = groups[symbol]
        target = day_dir / f"{symbol}.csv"
        rows = export_csv(paths, target)
        print(f"{symbol}: 合併 {len(paths)} 個檔案，共 {rows} 列 -> {target.name}")


if __name__ == "__main__":
    main()
