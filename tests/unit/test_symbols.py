from producer.adapters.base import normalize_symbol, parse_rfc3339_ms


def test_normalize_concatenated_symbol():
    assert normalize_symbol("BTCUSDT") == "BTC-USDT"
    assert normalize_symbol("ethusdc") == "ETH-USDC"


def test_normalize_slash_and_dash_symbol():
    assert normalize_symbol("btc/usd") == "BTC-USD"
    assert normalize_symbol("BTC-USD") == "BTC-USD"


def test_normalize_leaves_unknown_symbol():
    assert normalize_symbol("weird") == "WEIRD"


def test_parse_rfc3339_to_epoch_ms():
    assert parse_rfc3339_ms("2023-11-14T22:13:20.130Z") == 1_700_000_000_130
    assert parse_rfc3339_ms("2023-11-14T22:13:20+00:00") == 1_700_000_000_000
