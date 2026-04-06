from src.data_load.degiro import load_degiro
from src.data_load.stock_rates import load_stockrates
from src.data_load.fx_rates import load_fx
from src.data_transform.build_portfolio import enrich_portfolio


def main():
    load_degiro()
    load_stockrates()
    load_fx()
    enrich_portfolio()


if __name__ == "__main__":
    main()
