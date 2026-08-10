"""Parquet 緩衝寫入。

逐筆成交量大（單日單檔實測逾 1 萬筆），逐筆開檔寫入成本過高，因此累積到
row_group_size 才寫一個 row group；close() 會把未滿的批次補寫出去。
"""

from __future__ import annotations

import os
import sys
import time
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


def output_path(base_dir: Path, date_str: str, symbol: str, run_id: str) -> Path:
    """每次執行、每個代碼各自一個檔案：data/<日期>/<代碼>/<run_id>.parquet。

    run_id 是程式啟動時間（同一次執行的所有代碼共用），這樣同一天重跑第二次
    不會撞上第一次的檔名——舊版檔名固定，pq.ParquetWriter 開檔即截斷，
    實測重跑一次就讓 5 列資料變 2 列，且沒有任何警告。
    """
    return Path(base_dir) / date_str / symbol / f"{run_id}.parquet"


class ParquetTradeWriter:
    def __init__(self, path: Path, batch_size: int = 500,
                 flush_interval_seconds: float = 300.0,
                 row_group_size: int = 2000) -> None:
        self.path = Path(path)
        # 實際寫入的是這個暫存檔；close() 成功寫完 footer 之後才 os.replace
        # 成 self.path。os.replace 在同一檔案系統上是原子操作，所以
        # self.path 要嘛不存在、要嘛是完整可讀的檔案 —— 讀取端不會看到
        # 「開了頭但沒有 footer」的殘檔。
        self.partial_path = self.path.with_name(self.path.name + ".partial")
        self.batch_size = batch_size
        # 意外終止最多損失多少緩衝資料的上限。batch_size 曾經同時扮演這個
        # 角色，但把持久化頻率跟 row_group_size 解耦之後，buffer 可能遠遠
        # 超過 batch_size 都還沒真的落地，所以損失上限改由這個逾時獨立把關
        # ——不管 buffer 多大，逾時就強制寫出一個 row group（寧可碎片化，
        # 也不要讓資料無界地留在記憶體）。用單調時鐘，不受重開機、時區影響。
        self.flush_interval_seconds = flush_interval_seconds
        # 一個 row group 至少要湊到這麼多列才真正呼叫 write_table，避免
        # 「30 秒逾時 flush」把 190KB 的檔案切成 102 個、每組 5～11 列的
        # row group（讀取效率很差）。逾時強制寫出時可能達不到這個數字，
        # 那是刻意的取捨：資料安全優先於檔案結構。
        self.row_group_size = row_group_size
        self.dropped_after_close = 0
        self._buffer: list[dict] = []
        self._writer: pq.ParquetWriter | None = None
        self._closed = False
        self._last_flush = time.monotonic()

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
        overdue = (time.monotonic() - self._last_flush) >= self.flush_interval_seconds
        if overdue:
            # 逾時且緩衝有資料：不管有沒有到 row_group_size，寧可寫出一個
            # 較碎的 row group，也不要讓資料的損失上限失去意義。
            self._write_row_group()
        elif len(self._buffer) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        """批次量觸發的一般 flush：只有累積到 row_group_size 才真正落地成一個
        row group；未達門檻就先留在記憶體，讓下一次 flush 或逾時再檢查——
        這是持久化頻率跟 row group 大小解耦的地方。"""
        if len(self._buffer) >= self.row_group_size:
            self._write_row_group()

    def _write_row_group(self) -> None:
        # 只有真的嘗試寫出（或確認沒東西可寫）才重置逾時起算點。flush() 因為
        # 未達 row_group_size 而沒寫的那些呼叫不算數 —— 否則批次量觸發的
        # flush() 會不斷把逾時的時鐘往後推，讓「最多損失
        # flush_interval_seconds 秒」的承諾失去意義。
        self._last_flush = time.monotonic()
        if not self._buffer:
            return
        if self._writer is None:
            if self.path.exists() or self.partial_path.exists():
                # 最後一道防線：正常情況下 run id 不會碰撞，但萬一撞上，
                # 寧可整批拋出讓上層看見，也不能靜默截斷已經寫完的檔案。
                raise FileExistsError(
                    f"{self.path} (or {self.partial_path.name}) already exists; "
                    "refusing to overwrite it")
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._writer = pq.ParquetWriter(self.partial_path, TRADE_SCHEMA)
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
        self._closed = True
        self._write_row_group()          # close 一律寫出剩餘資料
        if self._writer is not None:
            writer, self._writer = self._writer, None
            try:
                writer.close()            # 寫 footer；中途被中斷會拋例外
            except Exception as error:
                # footer 沒寫完，.partial 沒有索引無法讀取。留著它（不改名成
                # self.path），讓讀取端天然忽略——但必須讓人知道發生過這件事。
                print(f"{self.partial_path}: writer.close() failed while "
                      f"writing the footer, file left as {self.partial_path.name} "
                      f"and is likely unreadable: {type(error).__name__}: {error}",
                      file=sys.stderr, flush=True)
                raise
            # footer 寫完才代表 .partial 是完整可讀的檔案；os.replace 在同一
            # 檔案系統上是原子操作，self.path 不會出現「半寫」的中間狀態。
            os.replace(self.partial_path, self.path)
        if self.dropped_after_close:
            print(f"{self.path}: dropped {self.dropped_after_close} record(s) "
                  "appended after close", file=sys.stderr, flush=True)
