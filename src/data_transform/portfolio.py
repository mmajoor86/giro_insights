import numpy as np
import pandas as pd
from src.constants.tickers import isin_to_ticker
from src.constants.config import path_portfolio
from src.data_load.load_rates import collect_stock_rates, collect_currency_data
from src.constants.tickers import currencies


def compute_portfolio():
    df_portfolio, df_overview = generate_overview(path_portfolio=path_portfolio)
    df_portfolio["Date"] = pd.to_datetime(df_portfolio["Date"]).dt.date
    df_stock_rates = collect_stock_rates(df_overview)
    df_stock_rates["Date"] = pd.to_datetime(df_stock_rates["Date"]).dt.date

    df_currency = collect_currency_data(currencies)

    df = df_portfolio.merge(
        df_stock_rates[["Date", "Ticker", "Close", "currency"]],
        on=["Date", "Ticker"],
        how="inner",
    ).merge(
        df_currency[["Date", "currency", "Fx_rate"]],
        on=["Date", "currency"],
        how="left",
    )

    df["Fx_rate"] = np.where(df["currency"] == "EUR", 1, df["Fx_rate"])
    df["eur"] = df["Close"] / df["Fx_rate"] * df["Aantal"]
    df.to_csv("data/processed/portfolio.csv", index=False)
    return df


def generate_overview(path_portfolio=path_portfolio):
    """ """
    df = pd.read_csv(path_portfolio)

    df["Ticker"] = df["ISIN"].replace(isin_to_ticker)
    df = df.rename({"SnapshotDate": "Date"}, axis=1)
    df["Date"] = pd.to_datetime(df["Date"].str.slice(0, 10))

    # Find the min-max date for each Product
    df_agg = df.groupby(["Product", "ISIN", "Ticker"]).agg({"Date": ["min", "max"]})
    df_agg.columns = ["StartDate", "EndDate"]
    df_agg = df_agg.reset_index()
    return df, df_agg
