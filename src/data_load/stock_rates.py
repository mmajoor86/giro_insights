import pandas as pd
import yfinance as yf

from src.constants.config import path_proc_trans, path_proc_rates


def collect_prices(df_port: pd.DataFrame) -> pd.DataFrame:
    """Fetch daily close prices per ticker, starting from first portfolio date."""
    print("Collecting Stockrates")
    all_prices = []

    for ticker, group in df_port.groupby("Ticker"):
        start = group["Datum"].min().strftime("%Y-%m-%d")
        try:
            prices = yf.download(ticker, start=start, auto_adjust=True)["Close"]
            df_prices = (
                prices.reset_index()
                .set_axis(["Date", "Close"], axis=1)
                .assign(Ticker=ticker)
            )
            all_prices.append(df_prices)
        except Exception:
            print(f"Could not fetch prices for {ticker}")

    return pd.concat(all_prices, ignore_index=True)


def collect_currencies(tickers: list[str]) -> dict[str, str]:
    """Get the trading currency for each ticker."""
    print("Collecting Currencies")
    currencies = {}
    for t in tickers:
        try:
            currencies[t] = yf.Ticker(t).fast_info["currency"]
        except Exception:
            print(f"Could not resolve currency for {t}")
    return currencies


def load_stockrates() -> pd.DataFrame:
    """Load stock rates and currency denominations from yfinance.

    Reads processed transactions from parquet, fetches current prices and
    currency information via yfinance, and persists the enriched result.

    Returns
    -------
    pd.DataFrame
        DataFrame containing stock prices enriched with currency information,
        with columns: Ticker, Datum, Price, Currency.
    """
    transactions = pd.read_parquet(path_proc_trans)

    tickers = transactions["Ticker"].unique().tolist()
    prices = collect_prices(transactions)
    currency_map = collect_currencies(tickers)

    prices["Currency"] = prices["Ticker"].map(currency_map)
    prices = prices.rename(columns={"Date": "Datum"})

    prices.to_parquet(path_proc_rates)

    return prices
