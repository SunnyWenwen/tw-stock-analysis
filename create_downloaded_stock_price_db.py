import sqlite3

db_file_name = "D:\data\stock_data\stock_data.db"
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


if __name__ == '__main__':
    create_stock_price_table()
    create_stock_header_table()
