import pandas as pd

from src.constants.config import path_account, path_transactions


def load_degiro_data():
    print("loading")
    print(path_transactions)
    pd_trans = pd.read_csv(path_transactions)

    pd_acc = pd.read_csv(path_account)

    return pd_trans, pd_acc