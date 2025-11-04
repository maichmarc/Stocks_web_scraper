import psycopg2
import pandas as pd
import sys
import os
import re
import datetime
import configparser
from sqlalchemy import create_engine, text
import petl
from src.trading_days import Days_of_trading
from src.loggers import logging

class DatatoDB:
    def __init__(self):      

        self.config = configparser.ConfigParser()

        # read settings from config file
        # get data from config file
        try:
            self.config.read('my_ini.ini')
        except Exception as e:
            print(f'Failed to read config file: {str(e)}')
            sys.exit()

        # read settings from config file
        self.destDB = self.config['CONFIG']['database']
        self.user = self.config['CONFIG']['user_db']
        self.password = self.config['CONFIG']['password_db']
        self.host = self.config['CONFIG']['host']
        self.port = self.config['CONFIG']['port']

        self.codes = ['EGAD', 'KUKZ', 'KAPC', 'LIMT', 'SASN', 'WTK', 'CGEN', 'ABSA', 'BKG', 'COOP', 'DTK', 'EQTY', 'HFCK', 'IMH', 'KCB', 
            'NBK', 'NCBA', 'SBIC', 'SCBK', 'DCON', 'EVRD', 'XPRS', 'HBE', 'KQ', 'LKL', 'NBV', 'NMG', 'SMER', 'SGL', 'TPSE', 
            'UCHM', 'SCAN', 'ARM', 'BAMB', 'CRWN', 'CABL', 'PORT', 'KEGN', 'KPLC-P4', 'KPLC-P7', 'KPLC', 'TOTL', 'UMME', 'BRIT', 
            'CIC', 'JUB', 'KNRE', 'LBTY', 'SLAM', 'CTUM', 'HAFR', 'KURV', 'OCH', 'TCL', 'NSE', 'AMAC', 'BOC', 'BAT', 'CARB', 'EABL', 
            'FTGH', 'MSC', 'UNGA', 'SCOM', 'LAPR', 'GLD', '^N10I', '^N20I', '^N25I', '^NASI', '^NBDI', '^ZKEQTK', '^ZKEQTU']

        self.names = ['Eaagads Ltd', 'Kakuzi Plc', 'Kapchorua Tea Kenya Plc', 'Limuru Tea Plc', 'Sasini Plc', 'Williamson Tea Kenya Plc', 
                'Car and General (K) Ltd', 'ABSA Bank Kenya Plc', 'BK Group Plc', 'Co-operative Bank of Kenya Ltd', 
                'Diamond Trust Bank Kenya Ltd', 'Equity Group Holdings Plc', 'HF Group Ltd', 'I & M Holdings Plc', 'KCB Group Plc', 
                'National Bank of Kenya Ltd', 'NCBA Group Plc', 'Stanbic Holdings Plc', 'Standard Chartered Bank Kenya Ltd', 
                'Deacons (East Africa) Plc', 'Eveready East Africa Ltd', 'Express Kenya Plc', 'Homeboyz Entertainment Plc', 
                'Kenya Airways Plc', 'Longhorn Publishers Plc', 'Nairobi Business Ventures Ltd', 'Nation Media Group Plc', 'Sameer Africa Plc', 
                'Standard Group Plc', 'TPS Eastern Africa (Serena) Ltd', 'Uchumi Supermarket Plc', 'WPP ScanGroup Plc', 'ARM Cement Plc', 
                'Bamburi Cement Ltd', 'Crown Paints Kenya Plc', 'East African Cables Plc', 'East African Portland Cement Ltd', 
                'Kenya Electricity Generating Company Plc', 'KPLC-P4', 'KPLC-P7', 'Kenya Power and Lighting Company Plc', 
                'TotalEnergies Marketing Kenya Plc', 'Umeme Ltd', 'Britam Holdings Plc', 'CIC Insurance Group Ltd', 'Jubilee Holdings Ltd', 
                'Kenya Re-Insurance Corporation Ltd', 'Liberty Kenya Holdings Ltd', 'Sanlam Kenya Plc', 'Centum Investment Company Plc', 
                'Home Afrika Ltd', 'Kurwitu Ventures Ltd', 'Olympia Capital Holdings Ltd', 'Trans-Century Plc', 'Nairobi Securities Exchange Plc', 
                'Africa Mega Agricorp Plc', 'BOC Kenya Plc', 'British American Tobacco Kenya Plc', 'Carbacid Investments Plc', 
                'East African Breweries Ltd', 'Flame Tree Group Holdings Ltd', 'Mumias Sugar Company Ltd', 'Unga Group Ltd', 
                'Safaricom Plc', 'Laptrust Imara Income-REIT', 'ABSA NewGold ETF', 'NSE 10-Share Index', 'NSE 20-Share Index', 
                'NSE 25-Share Index', 'NSE All-Share Index', 'NSE Bonds Index', 'Zamara Kenya Equity Index (KES)', 'Zamara Kenya Equity Index (USD)'
                ]

    def company_names(self):
        logging.info('Creating a list of Companies and their Ticker codes')
        my_company_names = pd.DataFrame(data={'code':self.codes, 'names': self.names})
        company_names = petl.fromcolumns([self.codes, self.names], header=['code','name'])
        return company_names
    
    def company_details(self, trading_days):
        self.trading_days = trading_days
        df_list = []
        for i in range(len(trading_days)):
            date = trading_days[i]
            day = trading_days[i][6:8]
            month = trading_days[i][4:6]
            year = trading_days[i][0:4]
            data_path = os.path.join(os.getcwd(), 'Data', f'{year}',f'{month}')
            # load expenses document
            # try:
            df = pd.read_csv(f'{data_path}\{date}.csv')#, sheet='Github')
            df_dict = {f'{trading_days[i]}': df}
            df_list.append(df_dict)
    
        company_id = {}
        for i in range(len(self.codes)):
            company_id[self.codes[i]] = i+1
          
        dates = []
        company_ticker_id = []
        day_low = []
        day_high = []
        day_price = []
        prev_price = []
        volume = []
        for i in range(len(df_list)): 
            for date in df_list[i]:
                logging.info(f'Converting {date}')                           
                df_list[i][date]['date'] = datetime.datetime.strptime(date, '%Y%m%d')
                df_list[i][date]['Day_low'] = df_list[i][date]['Day_low'].replace('-', '.0')
                df_list[i][date]['Day_low'] = df_list[i][date]['Day_low'].str.replace(',', '').astype(float)
                df_list[i][date]['Day_high'] = df_list[i][date]['Day_high'].replace('-', '.0')
                df_list[i][date]['Day_high'] = df_list[i][date]['Day_high'].str.replace(',', '').astype(float)
                df_list[i][date]['Price'] = df_list[i][date]['Price'].str.replace(',', '').astype(float)
                df_list[i][date]['Previous_price'] = df_list[i][date]['Previous_price'].str.replace(',', '').astype(float)
                df_list[i][date]['Volume'] = df_list[i][date]['Volume'].str.replace('-', '0')
                df_list[i][date]['Volume'] = df_list[i][date]['Volume'].str.replace(',', '').astype(int)
                df_list[i][date]['Name'] = df_list[i][date]['Name'].str.replace('\n','')
                df_list[i][date]['company_id'] = df_list[i][date]['Code'].map(company_id)

                for row in df_list[i][date]['date']:
                    dates.append(row)
                for row in df_list[i][date]['company_id']:
                    company_ticker_id.append(row)
                for row in df_list[i][date]['Day_low']:
                    day_low.append(row)
                for row in df_list[i][date]['Day_high']:
                    day_high.append(row)
                for row in df_list[i][date]['Price']:
                    day_price.append(row)
                for row in df_list[i][date]['Previous_price']:
                    prev_price.append(row)
                for row in df_list[i][date]['Volume']:
                    volume.append(row)
        
        data = {'company_id':company_ticker_id, 'date': dates, 'day_high':day_high, 'day_low':day_low, 'day_price':day_price, 'previous_price':prev_price,
                'volume':volume}
        my_company_dets = pd.DataFrame(data=data)

        
        up = str('▲')
        down = str('▼')
        same = '-'
        my_company_dets['change'] = my_company_dets['day_price'] - my_company_dets['previous_price']
        my_company_dets['movement'] = ''
        for i in range(len(my_company_dets['change'])):
            if my_company_dets['change'][i] < 0:
                my_company_dets['movement'][i] = down
            elif my_company_dets['change'][i] > 0:
                my_company_dets['movement'][i] = up
            else:
                my_company_dets['movement'][i] = same

        # print(my_company_dets)#.info())
        my_company_dets.to_csv('compan.csv', encoding='utf-8')
        movement = my_company_dets['movement']
        change = my_company_dets['change']
        company_stock_details = petl.fromcolumns([company_ticker_id, dates, day_low, day_high, day_price, change, movement, volume], 
                                                header=['company_id', 'date', 'day_low', 'day_high', 'day_price', 'change', 'movement', 'volume'])
        
        return(company_stock_details)

  
    def company_names_to_db(self, company_names):
        try:
            conn = psycopg2.connect(
                dbname = self.destDB,
                user = self.user,
                password = self.password,
                host = self.host,
                port = self.port
            )
        except Exception as e:
            print(f'Could not connect to Database: {str(e)}')
            sys.exit()


        # create companies table
        try:
            # engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{destDB}')
            table_name = 'companies'
            cur = conn.cursor()
            cur.execute(f'DROP TABLE IF EXISTS {table_name} CASCADE')
            conn.commit()

            cur = conn.cursor()
            cur.execute('''
                    CREATE TABLE  "companies"(
                        id SERIAL PRIMARY KEY,
                        code VARCHAR(10) UNIQUE NOT NULL,
                        name VARCHAR(255) NOT NULL
                        )
        ''')
            conn.commit()
                
        except Exception as e:
            print(f'Could not create table: {str(e)}')
            sys.exit()

        # populate companies database table

        try:
            
            petl.io.todb(company_names, conn, 'companies')#, engine, if_exists='replace', index=False)
        except Exception as e:
            print(f'Could not write to Database: {str(e)}')
            sys.exit()

    def company_details_to_db(self, company_stock_details):
        try:
            conn = psycopg2.connect(
                dbname = self.destDB,
                user = self.user,
                password = self.password,
                host = self.host,
                port = self.port
            )
        except Exception as e:
            print(f'Could not connect to Database: {str(e)}')
            sys.exit()

        # create daily_prices table
        try:
            
            cur = conn.cursor()
            cur.execute('''
                    CREATE TABLE IF NOT EXISTS "daily_prices"(
                        id BIGSERIAL  PRIMARY KEY,
                        company_id INTEGER REFERENCES companies(id) ON DELETE CASCADE,
                        date DATE NOT NULL,
                        day_low NUMERIC(10, 4),
                        day_high NUMERIC(10, 4),
                        day_price NUMERIC(10, 4),
                        change NUMERIC(10,4),
                        movement VARCHAR(5),
                        volume BIGINT,
            
                        UNIQUE (company_id, date) 
                        )
        ''')
            conn.commit()
        except Exception as e:
            print(f'Could not create table: {str(e)}')
            sys.exit()

        # populate daily_prices database table

        try:
            
            petl.io.appenddb(company_stock_details, cur, 'daily_prices')#, engine, if_exists='replace', index=False)

            conn.commit()
        except Exception as e:
            print(f'Could not write to Database: {str(e)}')
            sys.exit()

