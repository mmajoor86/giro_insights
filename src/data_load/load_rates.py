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


def load():
    """Load Stockrates and Currrency denotations from yfinance"""
    df = pd.read_parquet(path_proc_trans)
    df_price = collect_prices(df)
    currency_map = collect_currencies(df["Ticker"].unique().tolist())
    df_price["Currency"] = df_price["Ticker"].map(currency_map)
    df_price.to_parquet(path_proc_rates)
    return df_price
