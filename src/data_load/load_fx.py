import yfinance as yf
import pandas as pd

from src.constants.config import path_proc_rates, path_proc_fx


def collect_fx_rates() -> pd.DataFrame:
    """Fetch daily EUR exchange rates per currency over each currency's date range."""

    df_cur = pd.read_parquet(path_proc_rates)

    df_cur = (
        df_cur.groupby("Currency")
        .agg(min_date=("Date", "min"), max_date=("Date", "max"))
        .reset_index()
        .query('Currency != "EUR"')
    )

    all_rates = []

    for _, row in df_cur.iterrows():
        currency = row["Currency"]
        ticker = f"EUR{currency}=X"
        try:
            rates = yf.download(
                ticker,
                start=row["min_date"].strftime("%Y-%m-%d"),
                end=row["max_date"].strftime("%Y-%m-%d"),
                auto_adjust=True,
            )["Close"]
            df_rates = (
                rates.reset_index()
                .set_axis(["Date", "Rate"], axis=1)
                .assign(Currency=currency)
            )
            all_rates.append(df_rates)
        except Exception:
            print(f"Could not fetch FX for {ticker}")

    df = pd.concat(all_rates, ignore_index=True)

    df.to_parquet(path_proc_fx)
    return df
