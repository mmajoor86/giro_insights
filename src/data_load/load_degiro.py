import pandas as pd
import numpy as np

from src.constants.config import path_account, path_transactions


def load_degiro_data():
    df_acc = load_account()
    df_trans = load_transactions()
    df_trans.head()
    return df_acc, df_trans


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


load_degiro_data()
