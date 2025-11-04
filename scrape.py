from src.trading_days import Days_of_trading
from src.scraper import Scraper
from src.loggers import logging
import calendar
import sys


YEAR = 2025
MONTH = 8
END_MONTH = 12

trading_days = Days_of_trading()
scraper = Scraper()

while MONTH < 13:    
    trade_days = trading_days.tradingDays(YEAR, MONTH)
    logging.info(f'Getting prices for {calendar.month_name[MONTH]}/{YEAR}')
    try:
        df_price_list = scraper.stock_scraper(trade_days)
        scraper.save_csv(df_price_list)
    except Exception as e:
        print(f'{str(e)}')
        sys.exit()
    logging.info(f'{calendar.month_name[MONTH]} completed ')
    MONTH += 1
    if MONTH == END_MONTH+1:
        break






