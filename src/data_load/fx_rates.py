import logging
from datetime import datetime

import pandas as pd
import yfinance as yf

from src.constants.config import path_proc_rates, path_proc_fx

logger = logging.getLogger(__name__)


def collect_fx_rates(df_cur) -> pd.DataFrame:
    """Fetch daily EUR exchange rates per currency over each currency's date range."""
    max_date = datetime.today().strftime("%Y-%m-%d")
    all_rates = []

    for _, row in df_cur.iterrows():
        currency = row["Currency"]
        ticker = f"EUR{currency}=X"
        try:
            rates = yf.download(
                ticker,
                start=row["min_date"].strftime("%Y-%m-%d"),
                end=max_date,
                auto_adjust=True,
                progress=False,
            )["Close"]
            df_rates = (
                rates.reset_index()
                .set_axis(["Date", "Rate"], axis=1)
                .assign(Currency=currency)
            )
            all_rates.append(df_rates)
        except Exception:
            logger.warning("Could not fetch FX for %s", ticker)

    df = pd.concat(all_rates, ignore_index=True)
    return df


def load_fx() -> pd.DataFrame:
    """Load foreign exchange rates for all non-EUR currencies in the portfolio.

    Reads processed stock rates to determine which currencies are held and
    their relevant date ranges, fetches the corresponding FX rates against EUR,
    and persists the result.

    Returns
    -------
    pd.DataFrame
        DataFrame containing FX rates per currency with columns:
        Currency, Datum, Rate.
    """
    rates = pd.read_parquet(path_proc_rates)
    currency_date_ranges = (
        rates.groupby("Currency")
        .agg(min_date=("Datum", "min"))
        .reset_index()
        .query('Currency != "EUR"')
    )

    fx_rates = collect_fx_rates(currency_date_ranges)
    fx_rates = fx_rates.rename(columns={"Date": "Datum"})

    fx_rates.to_parquet(path_proc_fx)

    return fx_rates
