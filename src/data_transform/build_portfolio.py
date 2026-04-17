import logging

import numpy as np
import pandas as pd

from src.constants.config import (
    path_proc_fx,
    path_proc_rates,
    path_proc_saldo,
    path_proc_trans,
    path_trans_portfolio,
    path_proc_deposit,
)

logger = logging.getLogger(__name__)

_CASH_LABEL = "Cash Balance"


def generate_portfolio() -> pd.DataFrame:
    """Generate daily (business-day) portfolio snapshots from transaction history.

    Aggregates transactions per product per business day, forward-fills holdings
    via cumulative sum, and removes positions that have been fully sold.

    Returns
    -------
    pd.DataFrame
        Daily portfolio with columns: Datum, Product, ISIN, Ticker, Aantal.
    """
    logger.info("Generating daily portfolio snapshots")
    df_trans = pd.read_parquet(path_proc_trans)

    days = pd.date_range(
        start=df_trans["Datum"].min(),
        end=pd.Timestamp.today(),
        freq="B",
    )

    df = (
        df_trans.groupby(
            [pd.Grouper(key="Datum", freq="B"), "Product", "ISIN", "Ticker"]
        )["Aantal"]
        .sum()
        .unstack(["Product", "ISIN", "Ticker"], fill_value=0)
        .reindex(days, fill_value=0)
        .cumsum()
        .stack(["Product", "ISIN", "Ticker"], future_stack=True)
        .reset_index()
        .rename(columns={"level_0": "Datum", 0: "Aantal"})
    )

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
        Enriched portfolio with columns: Datum, Product, ISIN, Ticker,
        Aantal, Rate_EUR, Total_Value.

    Raises
    ------
    FileNotFoundError
        If processed FX rates or stock rates parquet files do not exist.
    """
    logger.info("Enriching portfolio with stock prices and FX rates")
    portfolio = generate_portfolio()
    fx_rates = pd.read_parquet(path_proc_fx)
    stock_rates = pd.read_parquet(path_proc_rates)

    portfolio = (
        portfolio.merge(stock_rates, on=["Datum", "Ticker"], how="left")
        .sort_values(by=["Datum", "Ticker"], ascending=True)
    )
    # Forward-fill missing prices per ticker (weekends, holidays, yfinance lag)
    portfolio[["Close", "Currency"]] = portfolio.groupby("Ticker")[
        ["Close", "Currency"]
    ].ffill()

    portfolio = portfolio.merge(fx_rates, on=["Datum", "Currency"], how="left")
    portfolio["Rate"] = portfolio.groupby("Ticker")["Rate"].ffill()

    portfolio["Rate"] = np.where(portfolio["Currency"] == "EUR", 1, portfolio["Rate"])
    portfolio["Rate_EUR"] = portfolio["Close"] / portfolio["Rate"]
    portfolio["Total_Value"] = portfolio["Aantal"] * portfolio["Rate_EUR"]

    portfolio = portfolio.drop(columns=["Close", "Currency"]).dropna(
        subset=["Rate_EUR"]
    )

    return portfolio


def compute_cash_balance() -> pd.DataFrame:
    """Build a daily cash balance time series from the end-of-day saldo.

    Reindexes the saldo to a contiguous daily range and forward-fills missing
    dates, giving a cash balance value for every calendar day.

    Returns
    -------
    pd.DataFrame
        Daily cash balance with columns: Datum, Total_Value, Product,
        Ticker, ISIN.
    """
    logger.info("Computing daily cash balance")
    df_saldo = pd.read_parquet(path_proc_saldo)

    days = pd.date_range(
        start=df_saldo["Datum"].min(),
        end=pd.Timestamp.today(),
        freq="D",
    )

    df_saldo = (
        df_saldo.set_index("Datum")
        .reindex(days)
        .ffill()
        .reset_index()
        .rename(columns={"index": "Datum", "Saldo": "Total_Value"})
        .assign(Product=_CASH_LABEL, Ticker=_CASH_LABEL, ISIN=_CASH_LABEL)
    )

    return df_saldo


def build_daily_portfolio() -> pd.DataFrame:
    """Combine the enriched stock portfolio and cash balance into one daily overview.

    Concatenates holdings and cash, sorts chronologically, and persists the
    result to parquet.

    Returns
    -------
    pd.DataFrame
        Combined daily portfolio with columns: Datum, Product, ISIN, Ticker,
        Aantal, Rate_EUR, Total_Value.
    """
    logger.info("Building daily portfolio")
    portfolio = enrich_portfolio()
    cash_balance = compute_cash_balance()

    df = (
        pd.concat([portfolio, cash_balance])
        .sort_values(by="Datum", ascending=True)
        .reset_index(drop=True)
    )

    df_cash_deposit = pd.read_parquet(path_proc_deposit)

    df_cash_deposit = df_cash_deposit.rename(
        {"Bedrag": "Cumulatieve Inleg"}, axis=1
    ).drop(columns="Valuta")

    df_cash_deposit["Cumulatieve Inleg"] = df_cash_deposit["Cumulatieve Inleg"].cumsum()

    df = df.merge(df_cash_deposit, on=["Datum"], how="left")

    df["Cumulatieve Inleg"] = df["Cumulatieve Inleg"].ffill()
    df.to_parquet(path_trans_portfolio)
    logger.info("Saved portfolio to %s", path_trans_portfolio)
    return df
