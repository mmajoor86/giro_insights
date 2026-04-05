import pandas as pd


from src.constants.config import path_proc_trans


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
