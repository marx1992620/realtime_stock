"""成交方向判定與大單判定 —— 純函式，無狀態、無 IO。

方向規則以當日 2330 全 10,055 筆逐筆成交驗證過：買(外盤) 15,113 張、
賣(內盤) 10,042 張，與交易所 total.tradeVolumeAtAsk / tradeVolumeAtBid
零差異。連續交易時段沒有任何一筆落在五檔中間。
"""

from __future__ import annotations

SHARES_PER_LOT = 1000

BUY = "buy"          # 外盤，買方主動
SELL = "sell"        # 內盤，賣方主動
AUCTION = "auction"  # 集合競價（開盤/收盤/瞬間價格穩定措施），無主動方
UNKNOWN = "unknown"  # 五檔中間；實測未發生，保留以免靜默歸錯類


def classify_side(trade: dict) -> str:
    """判定一筆成交的方向。

    集合競價的成交事件不帶 bid/ask —— 那筆撮合本來就沒有最佳一檔可比，
    買賣雙方同時成交，因此獨立成一類，不併入買或賣。
    """
    if "bid" not in trade or "ask" not in trade:
        return AUCTION
    price = trade["price"]
    if price >= trade["ask"]:
        return BUY
    if price <= trade["bid"]:
        return SELL
    return UNKNOWN


def trade_value_twd(price: float, lots: int) -> float:
    """成交金額（台幣）。Fugle 的 size 單位是張，一張 1,000 股。"""
    return price * lots * SHARES_PER_LOT


def is_large_order(trade: dict, threshold_twd: float) -> bool:
    """該筆成交金額是否達到大單門檻。"""
    return trade_value_twd(trade["price"], trade["size"]) >= threshold_twd
