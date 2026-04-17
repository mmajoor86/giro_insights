import pandas as pd

from src.constants.config import (
    path_raw_acc,
    path_proc_saldo,
    path_proc_dividend,
    path_proc_deposit,
)

_COLUMNS_TO_DROP = [
    "Valutadatum",
    "Product",
    "ISIN",
    "Saldo",
    "FX",
    "Order Id",
]

_COLUMN_RENAMES = {
    "Mutatie": "Valuta",
    "Unnamed: 8": "Bedrag",
    "Unnamed: 10": "Saldo",
}

_DIVIDEND_LABELS = {"dividend"}
_DEPOSIT_LABELS = {"ideal deposit", "ideal storting"}


def load_account_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load and parse the raw account Excel export into daily financial summaries.

    Reads the raw account file, cleans column names, and derives three
    aggregated outputs:

    - Daily balance (saldo): the account balance at end of day.
    - Daily dividends: total dividend income per currency per day.
    - Daily deposits: total iDEAL deposits per currency per day.

    All three outputs are persisted to parquet.

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]
        A (saldo, dividends, deposits) tuple, each indexed by Datum.

    Raises
    ------
    FileNotFoundError
        If the raw account file at `path_raw_acc` does not exist.
    """
    df = pd.read_excel(path_raw_acc)

    df = df.drop(columns=_COLUMNS_TO_DROP, errors="ignore").rename(
        columns=_COLUMN_RENAMES
    )

    df["Datetime"] = pd.to_datetime(
        df["Datum"] + " " + df["Tijd"], format="%d-%m-%Y %H:%M"
    )
    df["Datum"] = pd.to_datetime(df["Datum"], format="%d-%m-%Y")

    # End-of-day balance: keep only the last record per day
    saldo = (
        df.loc[df.groupby("Datum")["Datetime"].idxmax()]
        .reset_index(drop=True)
        .filter(["Datum", "Saldo"])
    )

    # Dividend income per currency per day
    dividends = (
        df.loc[df["Omschrijving"].str.lower().isin(_DIVIDEND_LABELS)]
        .groupby(["Datum", "Valuta"], as_index=False)["Bedrag"]
        .sum()
    )

    # iDEAL cash deposits per currency per day
    deposits = (
        df.loc[df["Omschrijving"].str.lower().isin(_DEPOSIT_LABELS)]
        .groupby(["Datum", "Valuta"], as_index=False)["Bedrag"]
        .sum()
    )

    saldo.to_parquet(path_proc_saldo)
    dividends.to_parquet(path_proc_dividend)
    deposits.to_parquet(path_proc_deposit)

    return saldo, dividends, deposits
