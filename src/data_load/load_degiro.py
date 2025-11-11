import pandas as pd
import numpy as np

from src.constants.config import path_account, path_transactions


def load_degiro_data():
    df_acc = load_account()
    df_trans = load_transactions()
    df_dailyportfolio = generate_daily_totals(df_trans)
    return df_acc, df_trans, df_dailyportfolio


def load_account():
    # data cleaning
    df = pd.read_csv(path_account)

    df.columns = [
        "Datum",
        "Tijd",
        "Valutadatum",
        "Product",
        "ISIN",
        "Omschrijving",
        "FX",
        "Valuta Mutatie",
        "Waarde Mutatie",
        "Valuta Saldo",
        "Waarde Saldo",
        "Order Id",
    ]

    df["Timestamp"] = pd.to_datetime(
        df["Datum"] + " " + df["Tijd"], format="%d-%m-%Y %H:%M"
    )
    df = df.drop(["Datum", "Tijd", "Valuta Saldo", "Waarde Saldo", "Order Id"], axis=1)

    # convert string to floats
    df["Waarde Mutatie"] = df["Waarde Mutatie"].str.replace(",", ".").astype(float)
    df["FX"] = df["FX"].str.replace(",", ".").astype(float)

    # convert values to eur
    df["Waarde_EUR"] = np.where(
        df["FX"].notna(), df["Waarde Mutatie"] / df["FX"], df["Waarde Mutatie"]
    )
    df.to_csv("data/processed/account.csv", index=False)
    return df


def load_transactions():
    # data cleaning
    df = pd.read_csv(path_transactions)

    df.columns = [
        "Datum",
        "Tijd",
        "Product",
        "ISIN",
        "Beurs",
        "Uitvoeringsplaats",
        "Aantal",
        "Koers",
        "FX koers",
        "Lokale waarde",
        "FX lokale Waarde",
        "Waarde",
        "FX ",
        "Wisselkoers",
        "Transactiekosten",
        "FX Transactiekosten",
        "Totaal",
        "FX Totaal",
        "Order ID",
    ]

    df["Timestamp"] = pd.to_datetime(
        df["Datum"] + " " + df["Tijd"], format="%d-%m-%Y %H:%M"
    )

    # convert string to floats
    df["Koers"] = df["Koers"].str.replace(",", ".").astype(float)
    df["Wisselkoers"] = df["Wisselkoers"].str.replace(",", ".").astype(float)

    # convert values to eur
    df["Koers_EUR"] = np.where(
        df["FX koers"] != "EUR", df["Koers"] / df["Wisselkoers"], df["Koers"]
    )
    df["Waarde_EUR"] = df["Aantal"] * df["Koers_EUR"]

    df = df[
        [
            "Timestamp",
            "Order ID",
            "Product",
            "ISIN",
            "Aantal",
            "Koers_EUR",
            "Wisselkoers",
            "Waarde_EUR",
            "Transactiekosten",
        ]
    ]
    df.to_csv("data/processed/transactions.csv", index=False)
    return df


def generate_daily_totals(df):
    # create daily totals:
    start_date = df["Timestamp"].min()
    end_date = pd.Timestamp.today()
    days = pd.date_range(start=start_date, end=end_date, freq="B")

    # Generate portfolio snapshots for each first-of-month date
    portfolio_snapshots = [
        df[df["Timestamp"] <= date]
        .groupby(["Product", "ISIN"], as_index=False)["Aantal"]
        .sum()
        .query("Aantal != 0")
        .assign(SnapshotDate=date)
        for date in days
    ]

    # Combine and export the overview
    df_portfolio_overview = pd.concat(
        portfolio_snapshots, ignore_index=True
    ).sort_values(["SnapshotDate", "Product"])

    df_portfolio_overview.to_csv("data/processed/daily_portfolio.csv", index=False)
    return df
