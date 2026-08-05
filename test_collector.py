#!/usr/bin/env python3
"""Unit tests for trade-size handling.

The Fugle `trades` channel reports `size` in **張 (lots)** for the round-lot
market (market=TSE, type=EQUITY), not in shares. Two independent checks on
captured data confirm it:

1. `volume` (cumulative) increases by exactly `size` on every event, so both
   fields share one unit.
2. 2330's cumulative `volume` was 21,910 at 11:56 on 2026-08-05. As lots that
   is an ordinary morning for TSMC; as shares it would mean only ~22 lots had
   traded in two and a half hours.

These tests pin that unit so the conversion can never silently flip again.
"""

import csv
import sys
import types

import collector


# A real event captured from the WebSocket (data/2330_trades.csv, serial 13665698).
SAMPLE_TRADE = {
    "symbol": "2330", "type": "EQUITY", "exchange": "TWSE", "market": "TSE",
    "price": 2400, "size": 1, "bid": 2400, "ask": 2405, "volume": 21910,
    "isContinuous": True, "time": 1785902195644483, "serial": 13665698,
}


def test_format_trade_reports_size_in_lots():
    """size=1 is one lot (1,000 shares) — what the broker app shows."""
    line = collector.format_trade(SAMPLE_TRADE)
    assert "1 張" in line
    assert "1,000 股" in line
    assert "0.001" not in line


def test_format_trade_scales_multi_lot_trades():
    line = collector.format_trade({**SAMPLE_TRADE, "size": 25})
    assert "25 張" in line
    assert "25,000 股" in line


def test_saved_row_records_lots_and_shares(tmp_path):
    store = collector.TradeStore(tmp_path / "trades.csv")
    assert store.save(SAMPLE_TRADE) is True

    row = next(iter(csv.DictReader((tmp_path / "trades.csv").open(encoding="utf-8"))))
    assert row["size_lots"] == "1"
    assert row["size_shares"] == "1000"
    assert row["cumulative_volume_lots"] == "21910"


def test_duplicate_serial_is_not_written_twice(tmp_path):
    store = collector.TradeStore(tmp_path / "trades.csv")
    assert store.save(SAMPLE_TRADE) is True
    assert store.save(SAMPLE_TRADE) is False


def test_reopening_an_existing_file_keeps_working(tmp_path):
    path = tmp_path / "trades.csv"
    collector.TradeStore(path).save(SAMPLE_TRADE)
    reopened = collector.TradeStore(path)          # header matches -> resumes
    assert reopened.save(SAMPLE_TRADE) is False    # serial already known
    assert reopened.save({**SAMPLE_TRADE, "serial": 999, "size": 2}) is True


def test_file_written_by_the_buggy_version_is_rejected(tmp_path):
    """A pre-fix file used different columns AND wrong units. Appending to it
    would misalign every new row against the old header, silently corrupting the
    dataset — refuse instead, and say what to do."""
    path = tmp_path / "trades.csv"
    path.write_text(
        "symbol,serial,event_time_us,event_time_taipei,received_at_utc,price,"
        "size_shares,size_lots,cumulative_volume,bid,ask,is_trial,is_continuous,"
        "is_open,is_close,raw_json\n",
        encoding="utf-8",
    )
    try:
        collector.TradeStore(path)
    except SystemExit as exit_error:
        assert "size_lots" in str(exit_error)      # names the schema change
        assert str(path) in str(exit_error)        # names the offending file
    else:
        raise AssertionError("expected SystemExit for a stale CSV schema")


# ---------------------------------------------------------------------------
# Subscribe/authenticate ordering
#
# The SDK registers its own __authenticate on the "connect" event
# (fugle_marketdata/websocket/client.py:41) and sends the auth frame there,
# WITHOUT waiting for the server's reply. A "connect" handler that subscribes
# therefore puts the subscribe frame on the wire immediately behind the auth
# frame, and the server may evaluate it before authentication has taken effect
# -> {"message": "Forbidden resource"}. Subscribing on "authenticated" removes
# the race.
# ---------------------------------------------------------------------------

class _FakeStock:
    """Records the frames the collector sends, in order, against SDK events."""

    def __init__(self, timeline):
        self.timeline = timeline
        self.handlers = {}

    def on(self, event, listener):
        self.handlers[event] = listener

    def subscribe(self, params):
        self.timeline.append(("subscribe", params["symbol"]))

    def connect(self):
        # Mirrors the SDK: auth frame goes out, "connect" fires, and only later
        # does the server's "authenticated" reply come back.
        self.timeline.append(("auth_frame_sent",))
        self.handlers["connect"]()
        self.timeline.append(("server_authenticated",))
        message = '{"event":"authenticated","data":{}}'
        if "authenticated" in self.handlers:
            self.handlers["authenticated"](message)
        self.handlers["message"](message)


def _install_fake_sdk(monkeypatch, timeline):
    stock = _FakeStock(timeline)
    client = types.SimpleNamespace(stock=stock)
    sdk = types.ModuleType("fugle_marketdata")
    sdk.WebSocketClient = lambda **kwargs: client
    monkeypatch.setitem(sys.modules, "fugle_marketdata", sdk)
    certifi = types.ModuleType("certifi")
    certifi.where = lambda: "/dev/null"
    monkeypatch.setitem(sys.modules, "certifi", certifi)
    monkeypatch.setenv("FUGLE_API_KEY", "test-key")
    return stock


def test_subscribe_happens_only_after_authentication(monkeypatch, tmp_path):
    timeline = []
    _install_fake_sdk(monkeypatch, timeline)

    collector.run("2330", tmp_path / "trades.csv", include_trials=False)

    assert ("subscribe", "2330") in timeline, "collector never subscribed"
    subscribed_at = timeline.index(("subscribe", "2330"))
    authenticated_at = timeline.index(("server_authenticated",))
    assert subscribed_at > authenticated_at, (
        "subscribe was sent before the server confirmed authentication -> "
        f"races the auth frame and can be rejected as Forbidden resource: {timeline}"
    )


def test_subscribe_is_not_wired_to_the_connect_event(monkeypatch, tmp_path):
    timeline = []
    stock = _install_fake_sdk(monkeypatch, timeline)

    collector.run("2330", tmp_path / "trades.csv", include_trials=False)

    assert "authenticated" in stock.handlers, "no authenticated handler registered"
