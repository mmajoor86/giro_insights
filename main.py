from src.data_load.load_degiro import load_degiro_data
from src.data_transform.portfolio import compute_portfolio


def main():
    load_degiro_data()
    df = compute_portfolio()
    print(df.head())


if __name__ == "__main__":
    main()
