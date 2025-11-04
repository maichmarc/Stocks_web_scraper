import pandas as pd
from bs4 import BeautifulSoup
from src.trading_days import Days_of_trading
import configparser
import datetime
import requests
from playwright.sync_api import sync_playwright
import sys
import re
import os

class Scraper:
    def __init__(self):
        self.config = configparser.ConfigParser()
        
        # read settings from config file
        # get data from config file
        try:
            self.config.read('my_ini.ini')
        except Exception as e:
            print(f'Failed to read config file: {str(e)}')
            sys.exit()

    def stock_scraper(self, trading_days):
        '''
        This function takes in a list of trading days in a particular month. It the logs into the stock price website 
        and scrapes stock prices for each trading day in the list and returns a list of dataframes for each trading day
        '''
        
        self.trading_days = trading_days
        self.url = self.config['CONFIG']['url']
        self.user_mystock = self.config['CONFIG']['user_mystock']
        self.mystocks_pass = self.config['CONFIG']['mystocks_pass']
        self.chrome_path = self.config['CONFIG']['chrome_path']
        for i in range(len(self.trading_days)):
            self.date = self.trading_days[i]
            self.day = self.trading_days[i][6:8]
            self.month = self.trading_days[i][4:6]
            self.year = self.trading_days[i][0:4]
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False, slow_mo=50)
            page = browser.new_page()
            page.goto('https://live.mystocks.co.ke/')
            page.fill('input#username', self.user_mystock)
            page.fill('input#password', self.mystocks_pass)
            page.click('#loginBtn')
            df_price_list = []
            for a_day in self.trading_days:
                page.goto(f'{self.url}/{a_day}')
                page.is_visible('table.tblHoverHi')
                html = page.inner_html('#pricelist')
                soup = BeautifulSoup(html, 'html.parser')
                df = self.parser(soup)
                # self.save_csv(df)
                df_price_list.append(df)

        return df_price_list
    
    def parser (self, soup):
        '''This function takes in html data i.e. soup and extracts ['Code', 'Name', 'Day_low', 'Day_high', 'Price', 'Previous_price', 'Volume']
        and returns a pandas dataframe with all the captured data
        '''
        # self.price_list = price_list
        # for i in range
        title = soup.find_all('th', {'class':'b2'})  
        
        header = []
        for i in range(1, len(title)):
            header.append(title[i].text)

        # codes for listed companies
        the_codes = soup.find_all('td', {'class':'nm'})
        codes = []
        for i in range(len(the_codes)):
            if i % 2 == 0:
                codes.append(the_codes[i].text)

        # names of listed companies
        names = []
        for i in range(len(the_codes)):
            if i % 2 != 0:
                names.append(the_codes[i].text)

        # highest and lowest prices of the day
        h_l_prices = soup.find_all('td', {'class':'n bl'})
        hi_lo_prices = []
        for i in range(len(h_l_prices)):
            hi_lo_prices.append(h_l_prices[i].text)

        day_low = []
        n = 2
        while n < len(hi_lo_prices):
            day_low.append(hi_lo_prices[n])
            n += 4

        day_high = []
        n = 3
        while n < len(hi_lo_prices):
            day_high.append(hi_lo_prices[n])
            n += 4

        # closing prices of the day
        curr_price = soup.find_all('td', {'class':'n bl b'})
        current_price = []
        for i in range(len(curr_price)):
            current_price.append(curr_price[i].text)

        # closing price of the previous day
        prev_price = soup.find_all('td', {'class':'n bl br'})
        previous_price = []
        for i in range(len(prev_price)):
            previous_price.append(prev_price[i].text)

        # volumes traded for the day
        the_vols = soup.select('td.n:not(.bl):not(.br):not(.x-1):not(.x0):not(.x1)')
        volumes = []
        for i in range(len(the_vols)):
            if i % 2 == 0:
                volumes.append(the_vols[i].text)

        header = ['Code', 'Name', 'Day_low', 'Day_high', 'Price', 'Previous_price', 'Volume']
        data =  {'Code':codes, 'Name':names, 'Day_low':day_low, 'Day_high':day_high, 'Price':current_price, 'Previous_price':previous_price, 'Volume':volumes}
        df = pd.DataFrame(data)

        return df

    def save_csv(self, df_price_list):
        '''This function takes a list of dataframes containing stock price data and saves each dataframe as a .csv file corresponding to
        the trading day
        '''
        data_path = os.path.join(os.getcwd(), 'Data', f'{self.year}',f'{self.month}')
        os.makedirs(f'{data_path}', exist_ok=True)
        for i in range(len(df_price_list)):
            df_price_list[i].to_csv(f'{data_path}\{self.trading_days[i]}.csv', index=False)




   
