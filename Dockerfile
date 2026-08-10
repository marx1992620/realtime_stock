# 台股即時大單追蹤 —— 執行期映像檔
#
# 基底版本需與 Pipfile 的 python_version = "3.10" 一致。
FROM python:3.10-slim

WORKDIR /app

# 先只複製 requirements.txt 再裝套件：程式碼變動不會讓相依層失效，重build時
# 只要 requirements.txt 沒變就會直接吃快取，不必重新 pip install。
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 只複製執行需要的東西。app/__main__.py 會 import 根目錄的 collector.py 來
# 取用它的 load_dotenv()，所以 collector.py 也要一併複製進來——即使它本身是
# 舊版的獨立小工具。不複製 data/、.env、tests/、.superpowers/、docs/、
# training_set/（.dockerignore 已排除）。
COPY app/ app/
COPY collector.py .

# 不以 root 執行：建一個一般使用者，並讓它擁有 /app（含之後掛進來的 /data
# 掛載點的父目錄）。/data 由 docker-compose 的 volume 掛進來，掛載當下會沿用
# 主機端目錄的擁有者/權限，所以同時也要保證這個使用者對主機掛進來的目錄有
# 寫入權限——在 docker-compose.yml 的說明裡有提到如何配合。
RUN useradd --create-home --uid 1000 appuser \
    && mkdir -p /data \
    && chown -R appuser:appuser /app /data
USER appuser

# 不加緩衝直接印出 log，容器化執行時才看得到 print() 的即時輸出。
ENV PYTHONUNBUFFERED=1

EXPOSE 8000

# ENTRYPOINT 固定進入點，CMD 只給預設參數方便被 docker run 後面的參數整段
# 覆寫（例如 --help、或换一組 --symbols）。
# 注意：CLI 本身 --host 預設仍是 127.0.0.1（本機直跑比較安全，不在這裡改
# 程式碼），容器要對外開放必須在這裡或 docker-compose.yml 明確帶
# --host 0.0.0.0——見 docker-compose.yml。
ENTRYPOINT ["python", "-m", "app"]
CMD ["--symbols", "2330", "--host", "0.0.0.0", "--output-dir", "/data"]
