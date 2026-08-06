"""Parquet 緩衝寫入。

逐筆成交量大（單日單檔實測逾 1 萬筆），逐筆開檔寫入成本過高，因此累積到
batch_size 才寫一個 row group；close() 會把未滿的批次補寫出去。
"""

from __future__ import annotations

import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

# 不落 is_large：那是門檻的函數，而門檻可在盤中即時調整，落檔的旗標會永遠
# 停在寫入當下的門檻上 —— 同一個檔案可能混了好幾個門檻且事後無處可查。
# value_twd 已經落檔，任何門檻都能事後算出來。
TRADE_SCHEMA = pa.schema([
    ("symbol", pa.string()),
    ("serial", pa.int64()),          # 集合競價事件可能沒有 -> 允許 null
    ("time", pa.int64()),            # epoch microseconds
    ("price", pa.float64()),         # 可能為小數，例如 2317 的 257.5
    ("lots", pa.int64()),            # 張
    ("shares", pa.int64()),          # 股 = 張 × 1000
    ("side", pa.string()),           # buy / sell / auction / unknown
    ("value_twd", pa.float64()),
])


def output_path(base_dir: Path, date_str: str, kind: str, symbol: str) -> Path:
    return Path(base_dir) / date_str / f"{kind}_{symbol}.parquet"


class ParquetTradeWriter:
    def __init__(self, path: Path, batch_size: int = 500) -> None:
        self.path = Path(path)
        self.batch_size = batch_size
        self.dropped_after_close = 0
        self._buffer: list[dict] = []
        self._writer: pq.ParquetWriter | None = None
        self._closed = False

    def __enter__(self) -> "ParquetTradeWriter":
        return self

    def __exit__(self, *_exc) -> None:
        self.close()

    def append(self, record: dict) -> None:
        """關檔後只計數不寫入。

        行情執行緒是 daemon，關檔（在 finally 裡）之後它仍可能送進成交。若
        照收，湊滿一個批次就會以同一路徑重開 ParquetWriter，把已經寫完的檔案
        截斷成讀不出來的殘骸（實測 2,605 bytes 的有效檔變成 773 bytes，讀取
        時 ArrowInvalid: Parquet magic bytes not found in footer，整日資料報廢）。
        也不能改成拋例外 —— 那會從行情執行緒的回呼逸出。
        """
        if self._closed:
            self.dropped_after_close += 1
            if self.dropped_after_close == 1:
                # 正式流程只會 close 一次（uvicorn 收工後的 finally），關檔後的
                # 丟棄若只在下一次 close 才報告就永遠不會被印出來。第一筆當下
                # 就說一次，之後只累加，不洗版。
                print(f"{self.path}: trade arrived after close, dropping it "
                      "(feed thread is still running)", file=sys.stderr, flush=True)
            return
        self._buffer.append({name: record.get(name) for name in TRADE_SCHEMA.names})
        if len(self._buffer) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        if not self._buffer:
            return
        if self._writer is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._writer = pq.ParquetWriter(self.path, TRADE_SCHEMA)
        try:
            self._writer.write_table(
                pa.Table.from_pylist(self._buffer, schema=TRADE_SCHEMA))
        except Exception as error:
            # 一批寫失敗只損失該批。緩衝區若留著，之後每次 flush 都會再拋一次
            # 同樣的錯，整日不再落檔。
            print(f"Parquet write failed for {self.path}, dropping "
                  f"{len(self._buffer)} record(s): {type(error).__name__}: {error}",
                  file=sys.stderr, flush=True)
        finally:
            self._buffer.clear()

    def close(self) -> None:
        self.flush()
        if self._writer is not None:
            self._writer.close()
            self._writer = None
        self._closed = True
        if self.dropped_after_close:
            print(f"{self.path}: dropped {self.dropped_after_close} record(s) "
                  "appended after close", file=sys.stderr, flush=True)
