import time
from collections import namedtuple
from datetime import datetime, timedelta
from typing import List, Union, Dict

import numpy as np
import pandas as pd
from twstock import Stock
from twstock.codes import codes

from create_downloaded_stock_price_db import conn
from get_TWII_price import get_TWII_data


def year_month(year, month):
    return ''.join([str(year), str(month).zfill(2)])


DATATUPLE2 = namedtuple('Data',
                        ['sid', 'month', 'date', 'capacity', 'turnover', 'previous_close', 'open', 'high', 'low',
                         'close', 'change', 'transaction'])
cur_month = datetime.today().month
cur_year = datetime.today().year


class MyStock(Stock):
    """
    MyStock繼承twstock.Stock，並加入一些自己的方法。

    任何塞選資料，建議都把資料塞到self.data 然後return self.data

    資料說明
    date: datetime.datetime格式之時間，例如datetime.datetime(2017, 6, 12, 0, 0)。
    capacity: 總成交股數(單位: 股)。
    turnover: 總成交金額(單位: 新台幣 / 元)。
    previous_close: 昨日收盤價。
    open: 開盤價。
    high: 盤中最高價
    low:盤中最低價。
    close: 收盤價。
    change: 漲跌價差。
    transaction: 成交筆數。
    """

    def __init__(self, sid: str, initial_fetch: bool = True, silent=False):
        start_time = datetime.now()
        try:
            super().__init__(sid, initial_fetch)
        except Exception as e:
            raise Exception(f'股票代碼{sid}初始化失敗')
        if not silent:
            print(f'股票代碼{sid}初始化成功，耗時{round((datetime.now() - start_time).total_seconds(), 3)}秒')

    def fetch_31(self):
        """Fetch 31 days data"""
        today = datetime.today()
        before = today - timedelta(days=60)
        self.fetch_from_to(before.year, before.month, today.year, today.month)

    def fetch_from_to(self, from_year: int, from_month: int, to_year: int, to_month: int):
        """
        抓取指定月份區間的股價資料
        """
        self.raw_data = []
        self.data = []
        for year, month in self._month_year_iter(from_month, from_year, to_month, to_year):

            # 是否為當前月份
            is_this_month = year == cur_year and month == cur_month
            year_month_str = year_month(year, month)
            # 是否沒抓過該月資料
            not_in_db = not self.check_stock_data_in_db(year_month_str)
            # 沒抓過該閱資料，或是當前月份就要去線上抓取，否則從DB取出資料
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
            'previous_close': sub_db_data[5],
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
        取得指定日期的N日收盤價的均價。若當日無資料，則往前找到有資料的日期。
        :param target_date: 指定日期
        :param n_daily_average: N日均價
        :param soft: 是否使用軟性搜尋，若為True，則會往前找到有資料的日期，若為False，則會直接回傳None
        stock = MyStock('2330')
        stock.get_target_date_n_daily_average_price(datetime(year=2024, month=2, day=25), 60)
        """

        # 往前抓3倍N天前的日期的月份，確保可以抓到N日均價
        pre_month = target_date - timedelta(days=n_daily_average * 3)
        # 抓取 3倍N天前的日期的月份 到 當月
        self.fetch_from_to(pre_month.year, pre_month.month, target_date.year, target_date.month)
        # 去掉晚於target_date的資料
        self.data = [tmp_data for tmp_data in self.data if tmp_data.date <= target_date]

        # 若最後一筆資料日期不等於目標日期，代表當日無資料，若soft為False，則回傳None
        if self.data[-1].date != target_date and not soft:
            return None, None

        return self.moving_average(self.price, n_daily_average)[-1], self.data[-1].date

    def get_taiex_performance(self, target_date: datetime, n_daily_average: int, soft=True):
        pass

    def cal_return(self, start_cal_return_date: datetime, n_daily_average=5,
                   test_day_list: List[int] = [10, 30, 60, 120, 180, 360], evaluation_metric='ROI', silent=False,
                   adjust_by_taiex=False) -> Dict[str, Union[float, None]]:
        """
        回測股價
        :param adjust_by_taiex: 計算報酬是否要用大盤進行校正
        :param start_cal_return_date: 開始計算報酬的日期
        :param n_daily_average: 使用幾日均價當作當天價格(預設5日均價)
        :param test_day_list: 績效測試天數列表
        :param evaluation_metric:
            評估指標。
            ROI: 投報率
            IRR: 年度報酬率
        :param silent: 是否print回測結果
        :return:
        """
        # sid = '2330'
        # start_cal_return_date = datetime(year=2023, month=9, day=18)
        # test_day_list: List = [30, 60, 120, 180, 360]
        # n_daily_average = 5
        # 不可早於今天
        assert start_cal_return_date <= datetime.today(), f'start_cal_return_date不可晚於今天'
        # 確認時間格式
        assert isinstance(start_cal_return_date, datetime)

        start_stock_price, real_start_cal_return_date = self.get_target_date_n_daily_average_price(
            start_cal_return_date,
            n_daily_average)
        if not silent:
            print(f'開始回測股票SID : {self.sid}')
            print(
                f'    起始日期: {real_start_cal_return_date}, 起始股價: {start_stock_price}, N日均價: {n_daily_average}日')
        result_dict = {}
        for i in test_day_list:
            test_date = start_cal_return_date + timedelta(days=i)
            if test_date > datetime.today():
                if not silent:
                    print(f'    測試日期: {test_date}超過今天，無法進行測試')
                result_dict[str(i)] = None
                if adjust_by_taiex:
                    result_dict[str(i) + '_adj'] = None
                continue
            test_stock_price, real_end_cal_return_date = self.get_target_date_n_daily_average_price(test_date,
                                                                                                    n_daily_average)

            day_range = (real_end_cal_return_date - real_start_cal_return_date).days
            if evaluation_metric == 'IRR':
                metric = ((test_stock_price / start_stock_price) ** (365 / day_range) - 1) * 100
                metric = round(metric, 2)
            elif evaluation_metric == 'ROI':
                metric = ((test_stock_price / start_stock_price) - 1) * 100
                metric = round(metric, 2)
            else:
                raise ValueError('evaluation_metric只能為"ROI"或"IRR"')

            result_dict[str(i)] = metric
            # 是否用大盤進行校正
            if adjust_by_taiex:
                # 計算大盤報酬率
                # 開始日期的大盤價格，這個日期是股票有資料的日期，理論上大盤也會有。4是收盤價
                taiex_start_price = get_TWII_data(real_start_cal_return_date.strftime('%Y-%m-%d'))[4]
                taiex_end_price = get_TWII_data(real_end_cal_return_date.strftime('%Y-%m-%d'))[4]
                # 計算大盤報酬率
                if evaluation_metric == 'IRR':
                    taiex_metric = ((taiex_end_price / taiex_start_price) ** (365 / day_range) - 1) * 100
                    taiex_metric = round(taiex_metric, 2)
                elif evaluation_metric == 'ROI':
                    taiex_metric = ((taiex_end_price / taiex_start_price) - 1) * 100
                    taiex_metric = round(taiex_metric, 2)
                # 扣去大盤報酬率
                adjust_metric = metric - taiex_metric
                result_dict[str(i) + '_adj'] = round(adjust_metric, 2)

            if not silent:
                print(
                    f"    測試日期: {real_end_cal_return_date}(經過{day_range}天), "
                    f"測試股價: {test_stock_price}, "
                    f"起始股價: {start_stock_price}, "
                    f"漲跌幅: {(test_stock_price / start_stock_price - 1) * 100:.2f}%,"
                    f"年均報酬率: {metric:.2f}%")

        return result_dict

    def to_df(self):
        # 轉成dataframe
        return pd.DataFrame(self.data)

    def check_stock_data_in_db(self, year_month_str: str) -> bool:  # sid = '2330'; year_month_str = '202301'
        # 確認該股票該月份是否已經抓取過
        res = conn.execute(f"SELECT * FROM stock_header WHERE sid = '{self.sid}' AND month = '{year_month_str}'")
        return res.fetchone() is not None

    def recent_fluctuation(self, days_list: List[int] = [5, 10, 30, 60, 120]):
        """
        往回看，最近n日的漲跌幅
        """
        res_dict = {}
        # 抓取當日股價
        today_price = self.get_target_date_n_daily_average_price(datetime.today(), 1)[0]
        for tmp_day in days_list:
            tmp_day_ago_price = \
                self.get_target_date_n_daily_average_price(datetime.today() - timedelta(days=tmp_day), 1)[0]
            # 計算漲跌幅並加入dict
            res_dict[str(tmp_day)] = round((today_price / tmp_day_ago_price - 1) * 100, 2)

        return res_dict


if __name__ == '__main__':
    start = time.time()
    stock = MyStock('00631L', initial_fetch=False)
    print(f'Init {time.time() - start} seconds')

    start = time.time()
    stock = MyStock('00631L', initial_fetch=True)
    print(f'Init {time.time() - start} seconds')

    # 跑第一次
    start = time.time()
    print(stock.cal_return(datetime(year=2020, month=3, day=5)))
    print(f'All took {time.time() - start} seconds')

    # 跑第二次會比較快
    start = time.time()
    print(stock.cal_return(datetime(year=2020, month=3, day=5)))
    print(f'All took {time.time() - start} seconds')

    # 校正by大盤
    print(stock.cal_return(datetime(year=2024, month=3, day=5), adjust_by_taiex=True))

    # 計算近期漲跌幅
    print(stock.recent_fluctuation([5, 10, 20, 30]))
