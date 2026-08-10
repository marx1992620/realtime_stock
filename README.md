# 台股即時大單追蹤

本目錄有兩支程式：

| 程式 | 用途 |
|------|------|
| **`app`（主要）** | 多檔股票即時追蹤：每價位成交量（買賣分計）、大單篩選、五分鐘大單流向圖、Web 看盤畫面、Parquet 落檔 |
| `collector.py`（舊版） | 單檔逐筆成交寫 CSV，最早的雛形，保留可用 |

---

## 準備（兩支共用）

1. 在 [Fugle Developer](https://developer.fugle.tw/) 申請 Market Data API Key。
2. 在本目錄建立 `.env`：`cp .env.example .env`，再填入 API Key。
3. 安裝套件：`python3 -m pip install -r requirements.txt`

---

# app：多檔即時大單追蹤

## 啟動

```bash
python3 -m app --symbols 2330,2317,2301,4967,2451 --large-order 5,2330=3
```

啟動後開瀏覽器到 **http://127.0.0.1:8000**。終端會依序出現：

```
追蹤 2330 台積電, 2317 鴻海, …｜大單門檻（張）2330 台積電 3、2317 鴻海 5、…｜訂閱數 5/5
看盤畫面 http://127.0.0.1:8000
Connected. Authenticating...
API Key authenticated.
Subscribed: trades 2330
…
```

看到 `Subscribed:` 逐檔出現代表訂閱成功。按 **Ctrl-C** 停止，資料會在停止時完整寫出。

## 參數

| 參數 | 預設 | 說明 |
|------|------|------|
| `--symbols` | `2330` | 逗號分隔的股票代碼 |
| `--large-order` | `10` | 大單門檻（**張數**）。單一數字全部套用；`2330=20,2317=5` 逐檔指定；`10,2330=20` 混用 |
| `--output-dir` | `data` | Parquet 輸出目錄 |
| `--host` / `--port` | `127.0.0.1` / `8000` | 看盤畫面位址 |
| `--include-trials` | 關閉 | 一併記錄開盤前試撮 |

大單門檻也可在畫面上逐檔即時調整，會就地重算、不需重連。

## ⚠️ 兩個 API 硬限制

這兩項都是實測撞出來的，會直接決定你能追幾檔：

**一把 API key 只能開一條 WebSocket 連線。** 執行 `python3 -m app` 期間不可同時跑
`collector.py` 或另一個 `app`，否則後啟動的會被伺服器以
`Maximum number of connections reached` 斷線。

**一條連線最多 5 個訂閱。** 每檔股票的成交資料（`trades`）佔 1 個，所以**最多同時追蹤 5 檔股票**。

超過上限時程式**啟動前就會擋下**並告訴你怎麼調整，不會靜默留下空白分頁。

（Task 16 起已移除五檔報價功能：訂閱預算只有 5 個，不該讓五檔掛單跟股票搶配額。）

## 畫面看什麼

- **代號與名稱**、目前價格、當日買賣總張數
- **每價位成交量**：買（外盤）／賣（內盤）／集合競價分開計算
- **大單每價位成交量**：只含達門檻者，同樣買賣分開
- **大單流向圖**：五分鐘一格，買紅色向上、賣綠色向下的柱狀圖，股價線疊在上面
- 上方可切換股票、逐檔調整大單門檻
- 新增的代碼若訂閱被伺服器拒絕（例如超過上限），分頁上會顯示 ⚠ 提示，該檔不會有任何資料

## 資料落檔與匯出

逐筆成交寫成 `data/<日期>/trades_<代碼>.parquet`，欄位：

`symbol, serial, time, price, lots, shares, side, value_twd`

（`lots` 單位張、`shares` 單位股、`time` 為 epoch 微秒）。刻意**不存**「是否大單」——那取決於門檻，而門檻可隨時調整，存下來的旗標會與後來的設定不一致。用 `lots` 或 `value_twd` 自行篩選即可。

需要 CSV 時：

```bash
python3 -m app.export --date 2026-08-07
```

會在同一個日期目錄下產生對應的 `.csv`，並多一個人眼可讀的 `time_taipei` 欄位。

## 買賣方向如何判定

Fugle 的成交事件沒有買賣旗標，方向由成交價與當下最佳一檔推得：

| 條件 | 分類 |
|------|------|
| 無 `bid` 且無 `ask` | 集合競價（開盤／收盤，無主動方） |
| `price >= ask` | 買（外盤，買方主動） |
| `price <= bid` | 賣（內盤，賣方主動） |
| 恰缺一邊（漲跌停單邊無量） | 未分類 |

此規則以 2026-08-05 當日 2330 全 10,055 筆逐筆成交驗證過：買 15,113 張、賣 10,042 張，與交易所
`total.tradeVolumeAtAsk` / `tradeVolumeAtBid` **零差異**；2026-08-07 盤中再以 2330 44 筆、2317 60 筆
逐筆比對，方向與張數同樣零誤差。

## 測試

```bash
python3 -m pytest -q
```

## Docker 部署

想把 `app` 搬到另一台 server 長駐執行，不想手動裝 Python 環境，可以用容器跑。

### 建置映像檔

```bash
docker build -t tw-stock-tracker .
```

`Dockerfile` 只複製 `app/`、`collector.py`（`app/__main__.py` 會 import 它取用
`load_dotenv`）與 `requirements.txt` 進映像檔；`.env`、`data/`、`tests/` 等一律不進去
（見 `.dockerignore`）。容器內以非 root 使用者執行，且固定用 `--host 0.0.0.0`
（CLI 本身 `--host` 預設仍是 `127.0.0.1`，本機直跑沒有改，容器要能對外連才在
`docker run`／`docker-compose.yml` 的參數上覆寫）。

### 用環境變數帶 API key

映像檔裡不含任何金鑰，`FUGLE_API_KEY` 一律在啟動容器時才注入：

```bash
docker run --rm -p 8000:8000 -v $(pwd)/data:/data \
  -e FUGLE_API_KEY=your_real_key \
  tw-stock-tracker --symbols 2330,2317 --host 0.0.0.0 --output-dir /data
```

或用 `docker compose`（見下方），從主機的 `.env` 帶入，`.env` 本身留在主機、不進映像。

### 用 docker compose 跑起來

```bash
cp .env.example .env   # 填入真實的 FUGLE_API_KEY，這個檔案不會進映像檔
docker compose up -d --build
docker compose logs -f
```

`docker-compose.yml` 內已經帶好 `--host 0.0.0.0` 與 `--output-dir /data`，並把
`./data` 掛進容器的 `/data`。

**首次啟動前**，請確認主機上的 `./data` 目錄容器內的執行使用者（uid 1000）可以
寫入，例如：

```bash
mkdir -p data && chmod 777 data
# 或者，若主機是 Linux 且想收緊權限：sudo chown -R 1000:1000 data
```

（容器內建置時已經 `chown` 過 `/data`，但 bind mount 掛載當下，掛進去的目錄一律
沿用**主機端**的擁有者/權限，Dockerfile 裡的 `chown` 對「掛載進來之後」的路徑不
生效，所以主機端也要有寫入權限。）

### 資料在哪

資料一樣落在 `data/<日期>/<代碼>/<run>.parquet`，只是「容器內看到的路徑」是
`/data/...`，因為掛了 `./data:/data`——實際檔案還是在主機的 `./data`。容器被砍掉、
重建都不影響既有檔案；沿用 Task 12 的規則，每次啟動是新的 run id、新的檔案，重啟
不會覆寫前一次執行留下的資料。

### 怎麼改追蹤清單

兩種方式：

1. **不重啟**：直接在網頁上用「新增／移除股票」的功能即時調整（見上方「畫面看什麼」），
   不需要改 compose 檔或重啟容器。
2. **改預設值**：改 `docker-compose.yml` 裡 `command:` 的 `--symbols`／`--large-order`
   等參數，`docker compose up -d` 重建套用（這樣做會斷線重連一次，當下這幾秒的行情
   會漏接）。

### ⚠️ 單一實例限制

跟本機直跑一樣，**一把 API key 只能開一條 WebSocket 連線**，這是 Fugle 伺服器端的
硬限制。因此：

- **不要**對這個 service 用 `docker compose up --scale`（或任何形式的多副本）。
- **不要**在一台以上的機器同時用同一把 key 跑這個容器（也不可以和本機直跑的
  `python3 -m app` 或 `collector.py` 同時開）。

兩條以上的連線用同一把 key 連上去，會被伺服器互踢
（`Maximum number of connections reached`），造成雙方都不穩定地斷線重連。
`docker-compose.yml` 裡也有同樣的註解提醒。

---

# collector.py（舊版單檔收集器）

使用 Fugle Market Data WebSocket 訂閱單一個股的 `trades` 頻道，將每筆**實際成交**追加到 CSV。

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
