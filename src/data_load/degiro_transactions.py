import logging

import pandas as pd
from pandas import DataFrame

from src.constants.config import path_proc_trans, path_raw_trans
from src.data_load.utils import isin_to_ticker

logger = logging.getLogger(__name__)


_COST_COLUMN = "Transactiekosten en/of kosten van derden EUR"

_OUTPUT_COLUMNS = [
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


def load_transactions() -> DataFrame:
    """Load, clean, and enrich DeGiro transaction data with Yahoo Finance tickers.

    Reads the raw transaction export, derives total costs and per-unit prices,
    and resolves ISIN codes to Yahoo Finance tickers. Transactions whose ISIN
    could not be resolved (e.g. delisted instruments) are excluded.

    Notes
    -----
    Yahoo Finance does not track ISINs for instruments that have stopped
    trading. Rows where no ticker could be resolved are silently dropped.

    Returns
    -------
    DataFrame
        Cleaned transaction records with columns:
        Datum, Product, ISIN, Ticker, Uitvoeringsplaats,
        Aantal, Waarde EUR, Prijs, Totale Kosten.

    Raises
    ------
    FileNotFoundError
        If the raw transactions file at `path_raw_trans` does not exist.
    """
    df = pd.read_excel(path_raw_trans)

    df["Datum"] = pd.to_datetime(df["Datum"], format="%d-%m-%Y")
    df["Waarde EUR"] = df["Waarde EUR"].round(2)
    df["Totale Kosten"] = (df["AutoFX Kosten"] + df[_COST_COLUMN]).round(2)
    df["Prijs"] = (df["Waarde EUR"] / df["Aantal"]).round(2)

    # Resolve ISINs to Yahoo Finance tickers; drop unresolvable rows
    isin_ticker_map = isin_to_ticker(df["ISIN"].unique())
    df["Ticker"] = df["ISIN"].replace(isin_ticker_map)
    df = df.loc[df["Ticker"] != df["ISIN"]]

    df = df.filter(_OUTPUT_COLUMNS)
    logger.info("Saving transactions to %s", path_proc_trans)
    df.to_parquet(path_proc_trans)
    return df
