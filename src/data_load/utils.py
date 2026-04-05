import yfinance as yf


def isin_to_ticker(isins: list[str]) -> dict[str, str]:
    """Resolve ISINs to Yahoo Finance tickers."""
    mapping = {}
    for isin in isins:
        try:
            ticker = yf.Ticker(isin)
            symbol = ticker.info.get("symbol")
            if symbol:
                mapping[isin] = symbol
        except ValueError:
            print(f"Could not resolve ISIN: {isin}")
    return mapping
