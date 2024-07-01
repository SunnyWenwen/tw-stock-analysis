import pandas as pd

from get_stock_price_data import MyStock

etf00940_constituent_stocks = {'2603': (9.2, '長榮'), '2303': (3.3, '聯電'), '5483': (3.2, '中美晶'),
                               '3005': (3.1, '神基'), '2404': (3.0, '漢唐'), '2385': (2.8, '群光'),
                               '6176': (2.7, '瑞儀'), '2454': (2.6, '聯發科'), '3293': (2.5, '鈊象'),
                               '6121': (2.5, '新普')}

etf00939_constituent_stocks = {'2454': (6.21, '聯發科'), '3231': (5.85, '緯創'), '3702': (5.33, '大聯大'),
                               '3034': (5.07, '聯詠'),
                               '3711': (5.07, '日月光投控'), '2385': (5.06, '群光'), '6669': (4.84, '緯穎'),
                               '3037': (3.93, '欣興'),
                               '2379': (3.75, '瑞昱'), '2603': (3.66, '長榮')}

n_day = 5


def get_etf_constituent_stocks_percentage(constituent_stocks, cost):
    print('ETF成分股購入金額，成交金額占比')
    for stock_code, (weight, stock_name) in constituent_stocks.items():
        print(stock_code, weight)
        my_stock = MyStock(stock_code)
        stock_df = my_stock.to_df()
        n_day_average_turnover = sum(stock_df.tail(n_day)['turnover']) / n_day
        turnover_percentage = cost * weight / n_day_average_turnover
        print(
            f"{stock_code}/{stock_name} 近{n_day}天平均成交金額: {n_day_average_turnover}, ETF購買金額於成交金額占比: {turnover_percentage:.2f}%")


# get_etf_constituent_stocks_percentage(etf00939_constituent_stocks, 5 * 10 ** 10)
# get_etf_constituent_stocks_percentage(etf00940_constituent_stocks, 1.7 * 10 ** 11)

# 上市日期前兩個月開始算1個月的漲幅
# 939
listing_date = '2024-02-01'
# listing_date = '2024-03-01'
res_list = []
for stock_code, (weight, stock_name) in etf00939_constituent_stocks.items():
    # stock_code, (weight, stock_name) = ('2454', (6.21, '聯發科'))
    my_stock = MyStock(stock_code)
    tmp_res = my_stock.cal_return(pd.to_datetime(listing_date), test_day_list=[30], silent=True, adjust_by_taiex=True,
                                  n_daily_average=1)
    tmp_res['stock_name'] = stock_name
    res_list.append(tmp_res)

res_df = pd.DataFrame(res_list)
mean_return_939 = res_df['30'].mean()
mean_return_939_adjust = res_df['30_adj'].mean()
print(f'ETF 939成分股2/1~3/1平均漲幅: {mean_return_939:.2f}%, 用大盤校正後: {mean_return_939_adjust:.2f}%')

# 940
listing_date = '2024-02-01'
# listing_date = '2024-03-01'
res_list = []
for stock_code, (weight, stock_name) in etf00940_constituent_stocks.items():
    # stock_code, (weight, stock_name) = ('2603', (9.2, '長榮'))
    my_stock = MyStock(stock_code)
    tmp_res = my_stock.cal_return(pd.to_datetime(listing_date), test_day_list=[30], silent=True, adjust_by_taiex=True,
                                  n_daily_average=1)
    tmp_res['stock_name'] = stock_name
    res_list.append(tmp_res)
res_df = pd.DataFrame(res_list)
mean_return_940 = res_df['30'].mean()
mean_return_940_adjust = res_df['30_adj'].mean()

print(f'ETF 940成分股2/1~3/1平均漲幅: {mean_return_940:.2f}%, 用大盤校正後: {mean_return_940_adjust:.2f}%')
