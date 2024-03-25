from collections import namedtuple
from datetime import datetime, timedelta
from typing import List, Union

import numpy as np
import pandas as pd
from twstock import Stock

from create_downloaded_stock_price_db import conn


def year_month(year, month):
    return ''.join([str(year), str(month).zfill(2)])


DATATUPLE2 = namedtuple('Data',
                        ['sid', 'month', 'date', 'capacity', 'turnover', 'last_close', 'open', 'high', 'low', 'close',
                         'change', 'transaction'])


class MyStock(Stock):
    """
    MyStock繼承twstock.Stock，並加入一些自己的方法。

    任何塞選資料，建議都把資料塞到self.data 然後return self.data

    資料說明
    date: datetime.datetime格式之時間，例如datetime.datetime(2017, 6, 12, 0, 0)。
    capacity: 總成交股數(單位: 股)。
    turnover: 總成交金額(單位: 新台幣 / 元)。
    last_close: 昨日收盤價。
    open: 開盤價。
    high: 盤中最高價
    low:盤中最低價。
    close: 收盤價。
    change: 漲跌價差。
    transaction: 成交筆數。
    """

    def __int__(self, sid: str, initial_fetch: bool = True):
        super().__init__(sid, initial_fetch)

    def fetch_from_to(self, from_year: int, from_month: int, to_year: int, to_month: int):
        """
        抓取指定月份區間的股價資料
        """
        self.raw_data = []
        self.data = []
        for year, month in self._month_year_iter(from_month, from_year, to_month, to_year):
            is_this_month = year == to_year and month == to_month
            year_month_str = year_month(year, month)
            not_in_db = not self.check_stock_data_in_db(year_month_str)
            # 先從DB確認有沒有抓過該股票該月的資料，若沒有抓過或是當前月份，則抓取資料塞入DB
            if is_this_month or not_in_db:
                new_fetch_data = self.fetcher.fetch(year, month, self.sid)
                new_fetch_data = new_fetch_data['data']
                # 存入DB header，若key存在要更新日期
                conn.execute(
                    f"INSERT OR REPLACE INTO stock_header VALUES ('{self.sid}', '{year_month_str}', CURRENT_TIMESTAMP)")
                conn.commit()
                # 存入DB stock_daily_price
                for data in new_fetch_data:
                    conn.execute(
                        f"INSERT OR REPLACE INTO stock_daily_price VALUES ('{self.sid}', '{year_month_str}', "
                        f"'{data.date}', {data.capacity}, {data.turnover}, {data.close - data.change}, {data.open}, "
                        f"{data.high}, {data.low}, {data.close}, {data.change}, {data.transaction}, CURRENT_TIMESTAMP)")
                    conn.commit()
            # 從DB取出資料
            res = conn.execute(
                f"SELECT * FROM stock_daily_price WHERE sid = '{self.sid}' AND month = '{year_month_str}'")
            self.purify_data(res.fetchall())

        return self.data

    def purify_data(self, db_data: List[tuple]):
        # 轉成datatuple
        self.data.extend([self.to_datatuple(data) for data in db_data])

    def to_datatuple(self, sub_db_data: tuple):
        tmp_dict = {
            'sid': sub_db_data[0],
            'month': sub_db_data[1],
            'date': datetime.strptime(sub_db_data[2], '%Y-%m-%d %H:%M:%S'),
            'capacity': sub_db_data[3],
            'turnover': sub_db_data[4],
            'last_close': sub_db_data[5],
            'open': sub_db_data[6],
            'high': sub_db_data[7],
            'low': sub_db_data[8],
            'close': sub_db_data[9],
            'change': sub_db_data[10],
            'transaction': sub_db_data[11]
        }
        return DATATUPLE2(**tmp_dict)

    def get_target_date_n_daily_average_price(self, target_date: datetime, n_daily_average: int, soft=True):
        """
        取得指定日期的N日均價。若當日無資料，則往前找到有資料的日期。
        :param target_date: 指定日期
        :param n_daily_average: N日均價
        :param soft: 是否使用軟性搜尋，若為True，則會往前找到有資料的日期，若為False，則會直接回傳None
        stock = MyStock('2330')
        stock.get_target_date_n_daily_average_price(datetime(year=2024, month=2, day=25), 60)
        """

        # 確保可以抓到N日均價
        pre_month = target_date - timedelta(days=n_daily_average * 3)
        self.fetch_from_to(pre_month.year, pre_month.month, target_date.year, target_date.month)
        # 去掉晚於target_date的資料
        self.data = [tmp_data for tmp_data in self.data if tmp_data.date <= target_date]

        # 若最後一筆資料日期不等於目標日期，代表當日無資料，若soft為False，則回傳None
        if self.data[-1].date != target_date and not soft:
            return None, None

        return self.moving_average(self.price, n_daily_average)[-1], self.data[-1].date

    def back_test(self, start_backtest_date: datetime, n_daily_average=5,
                  test_day_list: List = [30, 60, 120, 180, 360], silent=False) -> List[Union[float, None]]:
        """
        回測股價
        :param start_backtest_date: 開始回測日期
        :param n_daily_average: 使用幾日均價當作當天價格(預設5日均價)
        :param test_day_list: 回測測試天數列表
        :param silent: 是否print回測結果
        :return:
        """
        # sid = '2330'
        # start_backtest_date = datetime(year=2023, month=9, day=18)
        # test_day_list: List = [30, 60, 120, 180, 360]
        # n_daily_average = 5
        # 不可早於今天
        assert start_backtest_date <= datetime.today(), f'start_backtest_date不可早於今天'
        # 確認時間格式
        assert isinstance(start_backtest_date, datetime)

        start_stock_price, real_start_backtest_date = self.get_target_date_n_daily_average_price(
            start_backtest_date,
            n_daily_average)
        print(f'起始日期: {real_start_backtest_date}, 起始股價: {start_stock_price}, N日均價: {n_daily_average}日')
        result = []
        for i in test_day_list:
            test_date = start_backtest_date + timedelta(days=i)
            if test_date > datetime.today():
                if not silent:
                    print(f'測試日期: {test_date}超過今天，無法進行測試')
                continue
            test_stock_price, real_test_start_backtest_date = self.get_target_date_n_daily_average_price(test_date,
                                                                                                         n_daily_average)

            day_range = (real_test_start_backtest_date - real_start_backtest_date).days
            IRR = ((test_stock_price / start_stock_price) ** (365 / day_range) - 1) * 100
            IRR = round(IRR, 2)
            result.append(IRR)
            if not silent:
                print(
                    f"測試日期: {real_test_start_backtest_date}(經過{day_range}天), "
                    f"測試股價: {test_stock_price}, "
                    f"起始股價: {start_stock_price}, "
                    f"漲跌幅: {(test_stock_price / start_stock_price - 1) * 100:.2f}%,"
                    f"年均報酬率: {IRR:.2f}%")
        return result if result else [None]

    def to_df(self):
        # 轉成dataframe
        return pd.DataFrame(self.data)

    def check_stock_data_in_db(self, year_month_str: str) -> bool:  # sid = '2330'; year_month_str = '202301'
        # 確認該股票該月份是否已經抓取過
        res = conn.execute(f"SELECT * FROM stock_header WHERE sid = '{self.sid}' AND month = '{year_month_str}'")
        return res.fetchone() is not None


if __name__ == '__main__':
    stock = MyStock('2330')
    stock.back_test(datetime(year=2023, month=3, day=5), 5)

# stock.a = fetch_from1
#
# stock = Stock('2330')
# len(stock.price)
# ma_p = stock.moving_average(stock.price, 1)
# stock.__class__.
# stock.fetch_from(2020, 1)
# stock.fetcher.fetch(2020, 1, stock.sid)


# # 處理該月份資料
# def to_dict(date_data):
#     return {key: val for key, val in zip(date_data._fields, date_data)}
#
# start_backtest_month_data_dict = {tmp_date_data.date: to_dict(tmp_date_data) for tmp_date_data in
#                                   start_backtest_month_data}
