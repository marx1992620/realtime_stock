#!/usr/bin/env python3
"""Store Fugle WebSocket trade events for a Taiwanese stock in SQLite."""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


# Fugle reports round-lot (market=TSE/OTC, type=EQUITY) trade sizes in **張**,
# and `volume` is the day's cumulative size in the same unit — verified on
# captured data: `volume` rises by exactly `size` on every event, and 2330's
# cumulative `volume` only makes sense as lots. Shares are derived, never assumed.
SHARES_PER_LOT = 1000

CSV_FIELDS = (
    "symbol", "serial", "event_time_us", "event_time_taipei", "received_at_utc",
    "price", "size_lots", "size_shares", "cumulative_volume_lots", "bid", "ask",
    "is_trial", "is_continuous", "is_open", "is_close", "raw_json",
)


def load_dotenv(path: Path) -> None:
    """Load simple KEY=VALUE entries without adding a runtime dependency."""
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        # A shell may export FUGLE_API_KEY as an empty string.  Treat that as
        # unset, while preserving an explicitly supplied non-empty value.
        key = key.strip()
        if not os.environ.get(key):
            os.environ[key] = value.strip().strip('"').strip("'")


class TradeStore:
    def __init__(self, output_file: Path) -> None:
        output_file.parent.mkdir(parents=True, exist_ok=True)
        self.output_file = output_file
        self.lock = threading.Lock()
        self.serials = self._load_serials()

    def _load_serials(self) -> set[tuple[str, str]]:
        if not self.output_file.exists() or self.output_file.stat().st_size == 0:
            return set()
        with self.output_file.open("r", encoding="utf-8", newline="") as file:
            reader = csv.DictReader(file)
            # Rows are appended without re-writing the header, so a file whose
            # header no longer matches CSV_FIELDS would take new rows in the
            # wrong columns. Refuse rather than corrupt it silently.
            if tuple(reader.fieldnames or ()) != CSV_FIELDS:
                raise SystemExit(
                    f"{self.output_file} was written by an older version with a different "
                    f"schema (its trade sizes were mis-scaled: size_lots held 張/1000).\n"
                    f"  expected columns: {', '.join(CSV_FIELDS)}\n"
                    f"  found instead   : {', '.join(reader.fieldnames or ['<empty>'])}\n"
                    "Move or delete that file, then re-run to start a clean one."
                )
            return {
                (row["symbol"], row["serial"])
                for row in reader
                if row.get("symbol") and row.get("serial")
            }

    def save(self, trade: dict[str, Any]) -> bool:
        serial = trade.get("serial")
        event_time = trade.get("time")
        if serial is None or event_time is None:
            raise ValueError("trade event is missing serial or time")
        key = (str(trade["symbol"]), str(serial))
        with self.lock:
            if key in self.serials:
                return False
            is_new_file = not self.output_file.exists() or self.output_file.stat().st_size == 0
            event_datetime = datetime.fromtimestamp(event_time / 1_000_000, tz=timezone.utc)
            with self.output_file.open("a", encoding="utf-8", newline="") as file:
                writer = csv.DictWriter(file, fieldnames=CSV_FIELDS)
                if is_new_file:
                    writer.writeheader()
                lots = trade.get("size")
                writer.writerow({
                    "symbol": trade["symbol"],
                    "serial": serial,
                    "event_time_us": event_time,
                    "event_time_taipei": event_datetime.astimezone(ZoneInfo("Asia/Taipei")).isoformat(),
                    "received_at_utc": datetime.now(timezone.utc).isoformat(),
                    "price": trade.get("price"),
                    "size_lots": lots,
                    "size_shares": lots * SHARES_PER_LOT if lots is not None else None,
                    "cumulative_volume_lots": trade.get("volume"),
                    "bid": trade.get("bid"),
                    "ask": trade.get("ask"),
                    "is_trial": int(bool(trade.get("isTrial", False))),
                    "is_continuous": _optional_bool(trade, "isContinuous"),
                    "is_open": _optional_bool(trade, "isOpen"),
                    "is_close": _optional_bool(trade, "isClose"),
                    "raw_json": json.dumps(trade, ensure_ascii=False, separators=(",", ":")),
                })
            self.serials.add(key)
            return True

    def close(self) -> None:
        pass


def _optional_bool(data: dict[str, Any], key: str) -> int | None:
    return int(bool(data[key])) if key in data else None


def format_trade(trade: dict[str, Any]) -> str:
    timestamp = datetime.fromtimestamp(trade["time"] / 1_000_000, tz=timezone.utc)
    taipei_time = timestamp.astimezone(ZoneInfo("Asia/Taipei")).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    size = trade.get("size")
    lots = "-" if size is None else f"{size:,} 張 ({size * SHARES_PER_LOT:,} 股)"
    return (
        f"{taipei_time} | {trade['symbol']} | {trade.get('price')} | {lots} | "
        f"bid/ask {trade.get('bid')}/{trade.get('ask')} | serial {trade.get('serial')}"
    )


def run(symbol: str, output_file: Path, include_trials: bool) -> None:
    try:
        import certifi
        from fugle_marketdata import WebSocketClient
    except ImportError as error:
        raise SystemExit("Missing dependency. Run: python3 -m pip install -r requirements.txt") from error

    # Some macOS Python installations have no usable system CA bundle.  Point
    # the SSL stack at certifi's maintained bundle while retaining validation.
    os.environ.setdefault("SSL_CERT_FILE", certifi.where())
    os.environ.setdefault("REQUESTS_CA_BUNDLE", certifi.where())

    api_key = os.getenv("FUGLE_API_KEY")
    if not api_key or api_key == "replace_with_your_api_key":
        raise SystemExit("FUGLE_API_KEY is not set. Copy .env.example to .env and add your API key.")

    store = TradeStore(output_file)
    client = WebSocketClient(api_key=api_key)
    stock = client.stock

    def on_message(message: str) -> None:
        try:
            event = json.loads(message)
            event_name = event.get("event")
            if event_name == "authenticated":
                print("API Key authenticated.", flush=True)
                return
            if event_name == "subscribed":
                print(f"Trade subscription confirmed for {symbol}.", flush=True)
                return
            if event_name == "error":
                print(f"Fugle API error: {event.get('data')}", file=sys.stderr, flush=True)
                return
            if event_name != "data" or event.get("channel") != "trades":
                return
            trade = event["data"]
            if trade.get("symbol") != symbol:
                return
            if trade.get("isTrial", False) and not include_trials:
                return
            if store.save(trade):
                print(format_trade(trade), flush=True)
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as error:
            print(f"Unable to process message: {error}", file=sys.stderr, flush=True)

    def on_connect() -> None:
        print("Connected. Authenticating...", flush=True)

    def on_authenticated(_message: Any) -> None:
        # Subscribe only once the server has confirmed authentication. The SDK
        # sends the auth frame from its own "connect" listener and does not wait
        # for the reply, so subscribing on "connect" puts the subscribe frame on
        # the wire while the session is still unauthenticated — the server then
        # rejects it with {"message": "Forbidden resource"}.
        print(f"Subscribing to {symbol} trades...", flush=True)
        stock.subscribe({"channel": "trades", "symbol": symbol})

    def on_error(error: Any) -> None:
        print(f"WebSocket error: {error}", file=sys.stderr, flush=True)

    stock.on("message", on_message)
    stock.on("connect", on_connect)
    stock.on("authenticated", on_authenticated)
    stock.on("error", on_error)
    print(f"Writing {symbol} trade events to {output_file}. Press Ctrl+C to stop.")
    try:
        stock.connect()
    except KeyboardInterrupt:
        print("\nStopping collector.")
    finally:
        store.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect real-time Taiwan stock trades from Fugle.")
    parser.add_argument("--symbol", default="2330", help="Stock code to subscribe to (default: 2330)")
    parser.add_argument("--output", type=Path, default=Path("data/2330_trades.csv"), help="CSV output file")
    parser.add_argument("--include-trials", action="store_true", help="Also store pre-open trial matches")
    args = parser.parse_args()
    load_dotenv(Path(__file__).with_name(".env"))
    run(args.symbol, args.output, args.include_trials)


if __name__ == "__main__":
    main()
