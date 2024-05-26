import sqlite3

from config import db_path

db_file_name = db_path + "stock_data.db"
conn = sqlite3.connect(db_file_name)


def create_stock_price_table():
    # 建立股價資料表
    conn.execute("""
    CREATE TABLE IF NOT EXISTS stock_daily_price (
        sid TEXT,
        month TEXT,
        date TIMESTAMP,
        capacity INTEGER,
        turnover INTEGER,
        last_close REAL,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        change REAL,
        "transaction" INTEGER,
        updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (sid, date)
    )
    """)

    conn.commit()


def create_stock_header_table():
    # 建立股票標頭資料表，確認每個月的股票資料是否已經抓取
    conn.execute("""
    CREATE TABLE IF NOT EXISTS stock_header (
        sid TEXT ,
        month TEXT,
        updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (sid, month)
    )
    """)
    conn.commit()


def create_TWII_table():
    conn.execute("""
    CREATE TABLE IF NOT EXISTS TWII_daily_price (
        date DATE,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume INTEGER,
        dividends REAL,
        stock_splits REAL,
        updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (date)
    )
    """)
    conn.commit()


if __name__ == '__main__':
    create_stock_price_table()
    create_stock_header_table()
    create_TWII_table()
