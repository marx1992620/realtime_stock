"""Parquet 緩衝寫入。

逐筆成交量大（單日單檔實測逾 1 萬筆），逐筆開檔寫入成本過高，因此累積到
batch_size 才寫一個 row group；close() 會把未滿的批次補寫出去。
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

TRADE_SCHEMA = pa.schema([
    ("symbol", pa.string()),
    ("serial", pa.int64()),          # 集合競價事件可能沒有 -> 允許 null
    ("time", pa.int64()),            # epoch microseconds
    ("price", pa.float64()),         # 可能為小數，例如 2317 的 257.5
    ("lots", pa.int64()),            # 張
    ("shares", pa.int64()),          # 股 = 張 × 1000
    ("side", pa.string()),           # buy / sell / auction / unknown
    ("value_twd", pa.float64()),
    ("is_large", pa.bool_()),
])


def output_path(base_dir: Path, date_str: str, kind: str, symbol: str) -> Path:
    return Path(base_dir) / date_str / f"{kind}_{symbol}.parquet"


class ParquetTradeWriter:
    def __init__(self, path: Path, batch_size: int = 500) -> None:
        self.path = Path(path)
        self.batch_size = batch_size
        self._buffer: list[dict] = []
        self._writer: pq.ParquetWriter | None = None

    def __enter__(self) -> "ParquetTradeWriter":
        return self

    def __exit__(self, *_exc) -> None:
        self.close()

    def append(self, record: dict) -> None:
        self._buffer.append({name: record.get(name) for name in TRADE_SCHEMA.names})
        if len(self._buffer) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        if not self._buffer:
            return
        if self._writer is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._writer = pq.ParquetWriter(self.path, TRADE_SCHEMA)
        self._writer.write_table(pa.Table.from_pylist(self._buffer, schema=TRADE_SCHEMA))
        self._buffer.clear()

    def close(self) -> None:
        self.flush()
        if self._writer is not None:
            self._writer.close()
            self._writer = None
