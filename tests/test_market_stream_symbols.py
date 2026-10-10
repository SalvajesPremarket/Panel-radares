from TradeScanner.data_engine.market_stream import AlpacaMarketStream


def test_normalizar_simbolos_acepta_tickers_de_acciones_y_descarta_formatos_invalidos():
    stream = AlpacaMarketStream("key", "secret")

    symbols = stream._normalizar_simbolos([
        " aapl ",
        "BRK.B",
        "msft",
        "",
        "BAD/TICKER",
        "https://example.com",
        "TOO-LONG-SYMBOL-123",
    ])

    assert symbols == ["AAPL", "BRK.B", "MSFT"]
    assert stream.health_snapshot()["invalid_symbols"] == [
        "BAD/TICKER",
        "HTTPS://EXAMPLE.COM",
        "TOO-LONG-SYMBOL-123",
    ]


def test_normalizar_simbolos_elimina_duplicados_y_respeta_limite():
    stream = AlpacaMarketStream("key", "secret", max_symbols=2)

    assert stream._normalizar_simbolos(["aapl", "AAPL", "MSFT", "NVDA"]) == [
        "AAPL",
        "MSFT",
    ]
