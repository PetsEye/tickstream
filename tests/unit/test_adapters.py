from common.schemas import Side
from producer.adapters.binance import BinanceAdapter
from producer.adapters.coinbase import CoinbaseAdapter
from producer.adapters.kraken import KrakenAdapter


def test_coinbase_inverts_maker_side_to_taker_side():
    adapter = CoinbaseAdapter(["BTC-USD"])
    trade = adapter._parse(
        {
            "trade_id": "123",
            "product_id": "BTC-USD",
            "price": "60000.5",
            "size": "0.01",
            "side": "BUY",  # maker bought => taker sold
            "time": "2023-11-14T22:13:20.130Z",
        }
    )
    assert trade.side is Side.SELL
    assert trade.symbol == "BTC-USD"
    assert trade.price == 60000.5
    assert trade.trade_id == "coinbase:123"
    assert trade.trade_ts == 1_700_000_000_130


def test_binance_market_maker_flag_maps_to_taker_side():
    adapter = BinanceAdapter(["BTC-USD"])
    sell = adapter._parse(
        {"a": 7, "s": "BTCUSDT", "p": "60001.0", "q": "0.02", "T": 1700000000123, "m": True}
    )
    buy = adapter._parse(
        {"a": 8, "s": "BTCUSDT", "p": "60001.0", "q": "0.02", "T": 1700000000124, "m": False}
    )
    assert sell.side is Side.SELL
    assert buy.side is Side.BUY
    assert sell.symbol == "BTC-USDT"


def test_kraken_maps_side_directly():
    adapter = KrakenAdapter(["BTC-USD"])
    trade = adapter._parse(
        {
            "trade_id": 9,
            "symbol": "BTC/USD",
            "price": 60002.0,
            "qty": 0.03,
            "side": "buy",
            "timestamp": "2023-11-14T22:13:20.130Z",
        }
    )
    assert trade.side is Side.BUY
    assert trade.symbol == "BTC-USD"
    assert trade.trade_id == "kraken:9"
