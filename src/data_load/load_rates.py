import pandas as pd
import os
import yfinance as yf
from src.constants.config import path_db


def search_database(path_db=path_db):
    """Returns Delisted stock from manual history db"""
    csv_files = [
        pd.read_csv(os.path.join(path_db, file), sep=";")
        for file in os.listdir(path_db)
        if file.endswith(".csv")
    ]
    df = pd.concat(csv_files, ignore_index=True)
    df["Date"] = pd.to_datetime(df["Date"], format="%d-%m-%Y").dt.date

    return df


def collect_stock_rates(df):
    """Fetch historical stock data for a grouped DataFrame of tickers and return combined results."""
    all_data = []
    empty_data = []

    for _, row in df.iterrows():
        start_date = row["StartDate"]
        end_date = row["EndDate"]
        symbol = row["Ticker"]
        product = row["Product"]
        isin_code = row["ISIN"]

        try:
            ticker = yf.Ticker(symbol)
            currency = ticker.info["currency"]
            history_df = (
                ticker.history(start=start_date, end=end_date)
                .reset_index()[["Date", "Close"]]
                .assign(
                    Product=product, ISIN=isin_code, Ticker=symbol, currency=currency
                )
            )
            print(f"Retrieved {history_df.shape[0]} rows for {symbol}")
            all_data.append(history_df)

        except Exception as error:
            print(f"Error fetching data for {symbol}: {error}")
            empty_data.append(
                {
                    "Ticker": symbol,
                    "Product": product,
                    "ISIN": isin_code,
                    "StartDate": start_date,
                    "EndDate": end_date,
                    "Error": str(error),
                }
            )

    combined_df = pd.concat(all_data, ignore_index=True)
    empty_data = pd.DataFrame(empty_data)

    # read db
    print(f"loading delisted stocks from db:{empty_data['Ticker'].unique()}")
    df_db = search_database()
    df_db = df_db.loc[df_db["Ticker"].isin(empty_data["Ticker"].unique())]

    df = pd.concat([combined_df, df_db])
    df["Date"] = pd.to_datetime(df["Date"].astype(str).str.slice(0, 10)).dt.date

    return df


def collect_currency_data(currencies):
    """
    Fetch historical currency data using yfinance and return a combined DataFrame.

    Parameters:
        currencies (list): List of currency tickers.

    Returns:
        pd.DataFrame: Combined DataFrame with historical FX rates.
    """
    currency_data = []

    for currency in currencies:
        print(f"Fetching currency data for: {currency}")

        try:
            ticker = yf.Ticker(currency)
            cur_df = (
                ticker.history(period="max")
                .reset_index()[["Date", "Close"]]
                .assign(Product=currency)
                .assign(currency=currency.replace("=X", "").replace("EUR", ""))
                .rename(columns={"Close": "Fx_rate"})
            )

            print(f"Retrieved {cur_df.shape[0]} rows for {currency}")
            currency_data.append(cur_df)

        except Exception as error:
            print(f"Error fetching data for {currency}: {error}")

    combined_df = pd.concat(currency_data, ignore_index=True)
    combined_df["Date"] = combined_df["Date"].dt.date
    return combined_df
