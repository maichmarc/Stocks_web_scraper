from src.stock_to_db import DatatoDB
from src.trading_days import Days_of_trading
from src.loggers import logging
import calendar
import sys
import warnings

warnings.filterwarnings('ignore')
trade_days = Days_of_trading()
data_to_db = DatatoDB()

YEAR = 2024
MONTH = 11
END_MONTH = 12



company_names = data_to_db.company_names()
data_to_db.company_names_to_db(company_names)

while MONTH < 13:
    try:
        trading_days = trade_days.tradingDays(YEAR, MONTH)
        company_stock_details = data_to_db.company_details(trading_days)
        # print(company_stock_details)
        data_to_db.company_details_to_db(company_stock_details)
        logging.info(f'Uploading {calendar.month_name[MONTH]}/{YEAR}')
    except Exception as e:
        print(f'Error:{str(e)}')
    MONTH += 1
    if MONTH == END_MONTH+1:
        break
