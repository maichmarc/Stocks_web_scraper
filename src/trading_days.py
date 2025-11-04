import datetime
import calendar
from test import Calender
from dateutil.easter import *


class Days_of_trading:
    def __init__(self):
        self.calender_obj = calendar.Calendar()
                        
    def tradingDays(self, year, month):
        '''
        Takes the year and month as input and returns all weekdays and non holiday days when trading can happen
        '''
        self.year = year
        self.month = month
        monthdays = self.monthDays(year, month)
        holidays = self.holidayDays(year, monthdays)
        not_holidays = []
        for day in monthdays:
            if day not in holidays:
                not_holidays.append(day)
        tradingdays = self.areWeekdays(not_holidays)
        return tradingdays
    
    def monthDays(self, year, month):
        '''
        Takes month and date as input and returns all the  dates in the month that year
        '''
        self.year = year
        self.month = month
        
        days = [datetime.datetime.strftime(day,'%Y%m%d') for day in self.calender_obj.itermonthdates(self.year, self.month) if day.month == month]
        return days

    def areWeekdays(self, monthdays):
        '''
        Takes output of mothDays() and removes the dates that were weekends and returns weekdays only 
        '''
        self.monthdays = monthdays
        weekdays = []
        for date in monthdays:
            # days.append(datetime.datetime.strptime(i, '%Y%M%d').strftime('%Y%M%d'))
            day = calendar.weekday(int(date[0:4]),int(date[4:6]), int(date[6:8]))
            if day < 5:
                weekdays.append(f'{date[0:4]}{date[4:6]}{date[6:8]}')
        return weekdays

    def holidayDays(self, year, monthdays):
        '''
        Takes the year and month as input. There is a list of Kenyan Public holidays provided. It checks:
          1. Easter dates in the given year and adds them to the list of public holidays.
          2. Checks if a given public holiday lands on a Sunday and pushes it to Monday and adds the date to the list of public holidys 
          '''
        self.year = year
        self.monthdays = monthdays
        self.PUBLIC_HOLIDAYS = [f'{year}0101', f'{year}0501', f'{year}0601', f'{year}1010', f'{year}1020', f'{year}1212', f'{year}1225', f'{year}1226']
        holiday_days = []
        add_1_day = datetime.timedelta(days=1)
        add_2_day = datetime.timedelta(days=2)
        easterSun = easter(year)
        goodFri = easterSun - add_2_day
        easterMon = easterSun + add_1_day
        # easterSun_str = datetime.datetime.strftime(easterSun, '%Y%m%d')
        easterMon_str = datetime.datetime.strftime(easterMon, '%Y%m%d')
        goodFri_str = datetime.datetime.strftime(goodFri, '%Y%m%d')
        self.PUBLIC_HOLIDAYS.extend([goodFri_str,easterMon_str])
        for day in monthdays:
            whichDay = calendar.weekday(int(day[0:4]),int(day[4:6]), int(day[6:8]))
            if day in self.PUBLIC_HOLIDAYS and whichDay == 6:
                newdate = datetime.datetime.strptime(day, '%Y%m%d').date()
                add_1_day = datetime.timedelta(days=1)
                monHoliday = newdate + add_1_day
                monHoliday_str = datetime.datetime.strftime(monHoliday, '%Y%m%d')
                self.PUBLIC_HOLIDAYS.append(monHoliday_str)
        return self.PUBLIC_HOLIDAYS