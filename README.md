# 台股 2330 即時逐筆成交收集器

使用 Fugle 官方 Market Data WebSocket 訂閱台積電（2330）的 `trades` 頻道，將每筆**實際成交**追加到 CSV。

## 準備

1. 在 [Fugle Developer](https://developer.fugle.tw/) 申請 Market Data API Key。
2. 在本目錄建立 `.env`：`cp .env.example .env`，再填入 API Key。
3. 安裝套件：`python3 -m pip install -r requirements.txt`

## 執行

```bash
python3 collector.py
```

資料預設寫入 `data/2330_trades.csv`。每一筆會輸出成交時間、價、單筆張數（並附換算股數）、買一、賣一與交易所流水號。程式會以股票代碼與 `serial` 去重，因此重連時收到的重複行情不會重複寫入。

> 若既有 CSV 是舊版（欄位為 `size_shares,size_lots,cumulative_volume`）寫出的，其張數被誤除以 1000。程式會拒絕續寫並提示，請先把該檔移走或刪除。

連線後會依序出現 `Connected`、`API Key authenticated`、`Trade subscription confirmed`。最後一行出現代表訂閱已成功；正常整股交易時段外不會再有逐筆成交輸出。

預設忽略開盤前的試撮合訊息（`isTrial=true`）；若也要保存，使用：

```bash
python3 collector.py --include-trials
```

可改訂閱其他個股與輸出位置：

```bash
python3 collector.py --symbol 2317 --output data/2317_trades.csv
```

## 查看資料

```bash
# 最新 20 筆（第一行是欄位名稱）
tail -n 20 data/2330_trades.csv
```

### 單位

Fugle `trades` 頻道在**整股市場**（`market: TSE/OTC`、`type: EQUITY`）回報的 `size` 單位是**張**，`volume` 是當日累計成交張數。因此：

| 欄位 | 單位 | 來源 |
|------|------|------|
| `size_lots` | 張 | 直接取自 API 的 `size` |
| `size_shares` | 股 | `size_lots × 1000`（衍生欄位） |
| `cumulative_volume_lots` | 張 | 直接取自 API 的 `volume`（當日累計） |

可自行複驗：`volume` 每筆恰好增加該筆的 `size`（同單位），且 2330 在 11:56 的累計 `volume` 為 21,910 —— 以「張」解讀是正常的一個上午，以「股」解讀則等於兩個半小時只成交約 22 張，不合理。

> 若日後要訂閱**盤中零股**，其 `size` 單位是股而非張，需另行分辨市場別再換算。

資料只在市場與 API 方案提供行情時抵達；程式不會對網頁做爬取或輪詢。

## 測試

```bash
python3 -m pytest test_collector.py -q
```

## macOS SSL 憑證錯誤

若出現 `CERTIFICATE_VERIFY_FAILED`，先重新安裝所需套件：

```bash
python3 -m pip install -r requirements.txt
```

收集器會自動使用 `certifi` 的受信任 CA 憑證包，仍會驗證伺服器憑證。請勿使用關閉 SSL 驗證的做法。
