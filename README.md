## Stocks_ETL

This is a web scraper for NSE (Nairobi Securities Exchange). 
The data scraped can be found in the data folder.

**Setup**

This was created using python 3.10. To install required dependencies run:

<pre>pip install -r requirements.txt</pre>

The src folder contains four modules:
* trading_days.py: This module takes in month and year as input and returns a list of days in which trading would typically happen i.e. weekdays and non-holidays.
* scraper.py : This module takes the list of trading days from above and scrapes stock data on those days. Daily data is then saved in .csv format in the data folder.
* stock_to_db.py : This module takes the data and stores it in a Postgresql Database.
* loggers.py: This module creates logs.

Data Format in the csv

Each csv contains daily prices for all the companies for that day.

Code | Name | Lowest Price of the Day | Highest Price of the Day | Closing Price | Previous Day Closing Price | Volume Traded

To run the scraper
<pre> python scraper.py </pre>
Change input year and month accordingly
To transfer the csv data to Postrgresql run:
<pre> python to_db.py </pre>

Set the following input

* Month
* Year
* End month

NB: The data in this app has been gotten from 'https://live.mystocks.co.ke/'. 
It is provided here **strictly for educational and research purposes only**.

All rights to the original data belong to their respective owners.
If you are a rights holder and have concerns about the use of this data,
please contact me.

This project does not intend any commercial use or redistribution
of the scraped data.
