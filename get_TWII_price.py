import pandas as pd
import yfinance as yf

from create_downloaded_stock_price_db import conn, create_TWII_table


# 確認是否有TWII的table，若沒有則建立一個
def update_TWII_data(start_date, all_data=False):
    # start_date = '2024-05-22'
    # 確認start_date是否為str and yyyy-mm-dd格式
    if not all_data:
        if not isinstance(start_date, str):
            raise ValueError('start_date必須為str的yyyy-mm-dd格式, 例如: "2021-01-01"')
        if len(start_date) != 10:
            raise ValueError('start_date格式必須為yyyy-mm-dd')
        if start_date[4] != '-' or start_date[7] != '-':
            raise ValueError('start_date格式必須為yyyy-mm-dd')

    data = yf.Ticker('^TWII')
    if all_data:
        df = data.history(period='max')
    else:
        # 多抓幾天的資料，才可以算出change
        start_date = pd.to_datetime(start_date)
        start_date = start_date - pd.DateOffset(days=10)
        start_date = start_date.strftime('%Y-%m-%d')
        df = data.history(start=start_date, end=None)

    df = df.reset_index(drop=False)
    df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
    # 用前一天的收盤價來當作基準價
    df['previous_close'] = df['Close'].shift(1)
    df['change'] = df['Close'] - df['previous_close']

    # 沒有change的資料踢掉，因為沒抓前一天的資料
    df = df.dropna(subset=['change'])

    # 比db內最晚的時間還要早的替除掉，避免重複
    if not all_data:
        # 剔除DB內這次更新的資料
        min_df_date = min(df['Date'])
        conn.execute(f"DELETE FROM TWII_daily_price WHERE date >= '{min_df_date}'")

    df.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'dividends', 'stock_splits', 'previous_close',
                  'change']
    df.to_sql('TWII_daily_price', conn, if_exists='append', index=False)


def get_TWII_data(date):  # date = '2024-05-26'
    """
    取得指定日期的TWII資料，若單天沒有開盤(沒資料)，則往前找到有資料的日期。
    資料依據順序分別代表資料如下
    0 date: 指定日期
    1 open: 開盤價
    2 high: 最高價
    3 low: 最低價
    4 close: 收盤價
    5 volume: 成交量
    6 dividends: 股息
    7 stock_splits: 股票分割
    8 previous_close: 前一天的收盤價
    9 change: 收盤價變動

    datetime.strftime('%Y-%m-%d')
    :param date:
    :return:
    """
    # 先確認資料是不是最新的，若不是則更新
    latest_updated_date = conn.execute("SELECT MAX(updated_date) FROM TWII_daily_price").fetchone()[0]
    # 轉為時間格式
    latest_updated_date = pd.to_datetime(latest_updated_date)
    # 用年月日比較，若更新日期是今天以前，則更新
    if latest_updated_date.strftime('%Y-%m-%d') < pd.Timestamp.now().strftime('%Y-%m-%d'):
        update_TWII_data(latest_updated_date.strftime('%Y-%m-%d'))

    # 檢查date是否為str and yyyy-mm-dd格式
    if not isinstance(date, str):
        raise ValueError('date必須為str的yyyy-mm-dd格式, 例如: "2021-01-01"')
    # 檢查時間格式須為 yyyy-mm-dd
    if len(date) != 10:
        raise ValueError('date格式必須為yyyy-mm-dd')
    if date[4] != '-' or date[7] != '-':
        raise ValueError('date格式必須為yyyy-mm-dd')
    # 該天可能沒有資料應該是沒有開市，若發生該情況就往前找
    while 1:
        res = conn.execute(f"SELECT * FROM TWII_daily_price WHERE date = '{date}'").fetchall()
        if res:
            break
        date = pd.to_datetime(date)
        date = date - pd.DateOffset(days=1)
        date = date.strftime('%Y-%m-%d')
    return res[0]


# 測試用: 刪除TWII_daily_price
# conn.execute("DROP TABLE TWII_daily_price")

if __name__ == '__main__':
    # 確認是否有TWII的table，若沒有則建立一個
    if 'TWII_daily_price' not in [table[0] for table in
                                  conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]:
        print('TWII_daily_price不存在')
        # 若TWII_daily_price不存在，則建立一個
        create_TWII_table()
        print('TWII_daily_price已建立')
        # 初次更新TWII資料，更新所有資料
        update_TWII_data(None, all_data=True)

    else:
        print('TWII_daily_price存在')
        # 確認TWII_daily_price內最新的日期
        latest_date = conn.execute("SELECT MAX(date) FROM TWII_daily_price").fetchone()[0]
        print('TWII_daily_price最新日期:', latest_date)

        # 加一天
        latest_date = pd.to_datetime(latest_date)
        latest_date = latest_date + pd.DateOffset(days=1)
        latest_date = latest_date.strftime('%Y-%m-%d')

        # 確認是否晚於今天
        if latest_date > pd.Timestamp.now().strftime('%Y-%m-%d'):
            print('TWII資料已經是最新的了')
        else:
            # 更新TWII資料
            print('更新TWII資料:', latest_date)
            update_TWII_data(latest_date)
    print('TWII資料更新完成')

    get_TWII_data('2024-06-02')
