import logging
import warnings

from src.data_load.degiro_transactions import load_transactions
from src.data_load.degiro_account import load_account_data
from src.data_load.stock_rates import load_stockrates
from src.data_load.fx_rates import load_fx
from src.data_transform.build_portfolio import build_daily_portfolio

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
warnings.filterwarnings("ignore", message="Workbook contains no default style")


def main():
    load_transactions()
    load_account_data()
    load_stockrates()
    load_fx()
    build_daily_portfolio()


if __name__ == "__main__":
    main()
