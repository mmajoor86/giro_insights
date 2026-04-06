import pandas as pd
from pandas import DataFrame
from typing import Tuple
import numpy as np


from src.constants.config import (
    path_raw_trans,
    path_raw_acc,
    path_proc_acc,
    path_proc_trans,
)

from src.data_load.utils import isin_to_ticker


def load_degiro() -> Tuple[DataFrame, DataFrame, DataFrame]:
    """Load DEGIRO account & transactions data and derive daily portfolio data."""
    df_acc = load_account()
    df_trans = load_transactions()
    return df_acc, df_trans


def load_account() -> pd.DataFrame:
    """Load and clean DeGiro account data."""
    df = pd.read_excel(path_raw_acc)

    # Remove and rename columns
    df = df.drop(
        [
            "Tijd",
            "Valutadatum",
            "Product",
            "ISIN",
            "Saldo",
            "FX",
            "Unnamed: 10",
            "Order Id",
        ],
        axis=1,
    )

    df = df.rename({"Mutatie": "Valuta", "Unnamed: 8": "Bedrag"}, axis=1)

    # Keep only deposits and dividend payments
    df = df.loc[
        df["Omschrijving"]
        .str.lower()
        .isin(["dividend", "ideal deposit", "ideal storting"])
    ]

    # Store Datum as a date type
    df["Datum"] = pd.to_datetime(df["Datum"], format="%d-%m-%Y")

    df.to_parquet(path_proc_acc)

    return df


def load_transactions() -> DataFrame:
    """Load and clean DeGiro transactions data."""

    # look up tickers in yahoo finance
    df = pd.read_excel(path_raw_trans)

    df["Datum"] = pd.to_datetime(df["Datum"], format="%d-%m-%Y")

    df["Totale Kosten"] = np.round(
        df["AutoFX Kosten"] + df["Transactiekosten en/of kosten van derden EUR"], 2
    )
    df["Prijs"] = np.round(df["Waarde EUR"] / df["Aantal"], 2)
    df["Waarde EUR"] = np.round(df["Waarde EUR"], 2)

    # Convert the ISINs code to the yahoo finance ticker to collect the rates later
    # Yfinance does not track ISINs that have stopped trading, these are removed from the dataset.
    isins = df["ISIN"].unique()
    isin_ticker = isin_to_ticker(isins)
    df["Ticker"] = df["ISIN"].replace(isin_ticker)
    df = df.loc[df["Ticker"] != df["ISIN"]]

    df = df.filter(
        [
            "Datum",
            "Product",
            "ISIN",
            "Ticker",
            "Uitvoeringsplaats",
            "Aantal",
            "Waarde EUR",
            "Prijs",
            "Totale Kosten",
        ]
    )

    df.to_parquet(path_proc_trans)
    return df
