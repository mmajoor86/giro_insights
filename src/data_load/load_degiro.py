import pandas as pd
import numpy as np

from src.constants.config import path_account, path_transactions


def load_degiro_data():

    df_acc = load_account()
    df_trans = load_transactions()
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
    df = df.drop(["Valuta Saldo", "Waarde Saldo", "Order Id"], axis=1)

    # convert string to floats
    df["Waarde Mutatie"] = df["Waarde Mutatie"].str.replace(",", ".").astype(float)
    df["FX"] = df["FX"].str.replace(",", ".").astype(float)

    # convert values to eur
    df["Waarde_EUR"] = np.where(
        df["FX"].notna(), df["Waarde Mutatie"] / df["FX"], df["Waarde Mutatie"]
    )
    return df


def load_transactions():
    # data cleaning
    df = pd.read_csv(path_transactions)

    return df


load_degiro_data()
