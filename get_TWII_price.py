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
        df = data.history(start=start_date, end=None)

    df = df.reset_index(drop=False)
    df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
    # 比db內最晚的時間還要早的替除掉，避免重複
    if not all_data:
        latest_date = conn.execute("SELECT MAX(date) FROM TWII_daily_price").fetchone()[0]
        df = df[df['Date'] > latest_date]
    df.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'dividends', 'stock_splits']
    df.to_sql('TWII_daily_price', conn, if_exists='append', index=False)


# 測試用: 刪除TWII_daily_price
# conn.execute("DROP TABLE TWII_daily_price")


if 'TWII_daily_price' not in [table[0] for table in
                              conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]:
    # 若TWII_daily_price不存在，則建立一個
    create_TWII_table()
    # 初次更新TWII資料，更新所有資料
    update_TWII_data(None, all_data=True)
else:
    # 確認TWII_daily_price內最新的日期
    latest_date = conn.execute("SELECT MAX(date) FROM TWII_daily_price").fetchone()[0]

    # 加一天
    latest_date = pd.to_datetime(latest_date)
    latest_date = latest_date + pd.DateOffset(days=1)
    latest_date = latest_date.strftime('%Y-%m-%d')

    # 確認是否晚於今天
    if latest_date > pd.Timestamp.now().strftime('%Y-%m-%d'):
        print('TWII資料已經是最新的了')
    else:
        # 更新TWII資料
        update_TWII_data(latest_date)
