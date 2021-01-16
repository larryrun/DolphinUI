import pandas as pd
import time


class Datetime(object):

    def __init__(self, date_time_str=None):
        if date_time_str:
            self.date_time = pd.to_datetime(date_time_str);
        else:
            self.date_time = pd.datetime.today()

    def get(self):
        return self.date_time

    def format(self, fmt='%Y%m%d'):
        try:
            return self.date_time.strftime(fmt)
        except ValueError:
            return None

    def offset(self, offset):
        return Datetime(self.date_time + pd.Timedelta(days=offset))

    def before(self, date_time):
        return self.get() < date_time.get()

    def after(self, date_time):
        return self.get() > date_time.get()

    def month_end_day(self):
        if self.date_time.month in [1, 3, 5, 7, 8, 10, 12]:
            return 31
        elif self.date_time.month in [4, 6, 9, 11]:
            return 30
        else:
            return 29 if self.date_time.year % 4 == 0 else 28

    def is_weekday(self):
        return self.date_time.weekday() <= 4

    def is_market_closed(self):
        today = pd.datetime.today()
        today = today + pd.Timedelta(hours=-today.hour);
        return self.date_time < today or self.date_time.hour >= 16