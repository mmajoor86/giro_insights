import pandas as pd
import numpy as np

from src.constants.config import (
    path_proc_trans,
    path_trans_portfolio,
    path_proc_fx,
    path_proc_rates,
)



def generate_portfolio() -> pd.DataFrame:
    df_trans = pd.read_parquet(path_proc_trans)
    """Generate daily (business-day) portfolio snapshots based on transaction history."""
    days = pd.date_range(
        start=df_trans["Datum"].min(), end=pd.Timestamp.today(), freq="B"
    )

    # Sum transactions per product per day, reindex to all business days, then cumsum
    df = (
        df_trans.groupby(
            [pd.Grouper(key="Datum", freq="B"), "Product", "ISIN", "Ticker"]
        )["Aantal"]
        .sum()
        .unstack(["Product", "ISIN", "Ticker"], fill_value=0)
        .reindex(days, fill_value=0)
        .cumsum()
        .stack(["Product", "ISIN", "Ticker"])
        .reset_index()
        .rename(columns={"level_0": "Datum", 0: "Aantal"})
    )

    # Filter sold transactions (n=0), sort, reorder columns
    df = (
        df.query("Aantal != 0")
        .sort_values(["Datum", "Product"])[
            ["Datum", "Product", "ISIN", "Ticker", "Aantal"]
        ]
        .reset_index(drop=True)
    )

    return df


def enrich_portfolio() -> pd.DataFrame:
    """Enrich the daily portfolio with stock prices and convert all values to EUR.

    Merges the generated portfolio with stock rates and FX rates, computes
    EUR-denominated prices, and calculates the total position value per holding.

    Non-EUR positions are converted using the FX rate for that currency on that
    date. EUR positions receive a rate of 1. Rows where the EUR rate could not
    be determined are dropped.

    Returns
    -------
    pd.DataFrame
        Enriched portfolio with columns: Datum, Ticker, Aantal, Rate_EUR,
        Total_Value.
    """
    portfolio = generate_portfolio()
    fx_rates = pd.read_parquet(path_proc_fx)
    stock_rates = pd.read_parquet(path_proc_rates)

    portfolio = (
        portfolio.merge(stock_rates, on=["Datum", "Ticker"], how="inner")
        .merge(fx_rates, on=["Datum", "Currency"], how="left")
        .sort_values(by=["Datum", "Ticker"], ascending=True)
    )

    portfolio["Rate"] = np.where(portfolio["Currency"] == "EUR", 1, portfolio["Rate"])
    portfolio["Rate_EUR"] = portfolio["Close"] / portfolio["Rate"]
    portfolio["Total_Value"] = portfolio["Aantal"] * portfolio["Rate_EUR"]

    portfolio = portfolio.drop(columns=["Close", "Currency"]).dropna(
        subset=["Rate_EUR"]
    )

    portfolio.to_parquet(path_trans_portfolio)

    return portfolio
