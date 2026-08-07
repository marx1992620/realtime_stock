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

    集合競價的成交事件「兩邊都不帶」bid/ask —— 那筆撮合本來就沒有最佳一檔
    可比，買賣雙方同時成交，因此獨立成一類，不併入買或賣。

    只缺一邊是另一回事：漲停時賣方佇列為空、跌停時買方佇列為空，若來源省略
    該欄位而非送 0，把它算成集合競價會讓漲跌停日的成交全部灌進 auction_lots，
    在最需要準確的那幾天靜默失真。缺一邊就無從判定主動方，歸為 UNKNOWN。
    """
    has_bid = "bid" in trade
    has_ask = "ask" in trade
    if not has_bid and not has_ask:
        return AUCTION
    if not has_bid or not has_ask:
        return UNKNOWN
    price = trade["price"]
    if price >= trade["ask"]:
        return BUY
    if price <= trade["bid"]:
        return SELL
    return UNKNOWN


def trade_value_twd(price: float, lots: int) -> float:
    """成交金額（台幣）。Fugle 的 size 單位是張，一張 1,000 股。

    門檻雖已改用張數，金額仍要算：Parquet 落 value_twd，日後想用金額門檻
    回頭重算才有依據。
    """
    return price * lots * SHARES_PER_LOT


def is_large_order(trade: dict, threshold_lots: int) -> bool:
    """該筆成交張數是否達到大單門檻。

    門檻的單位是張，與價格無關 —— 使用者盯的是「有人一次丟幾張」，
    金額門檻會讓同一個數字對高低價股代表完全不同的規模。
    """
    return trade["size"] >= threshold_lots
