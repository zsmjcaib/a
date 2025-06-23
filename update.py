from typing import Dict
import efinance as ef
import pandas as pd
from datetime import datetime,timedelta
import tushare as ts
import talib
import yaml
import os
import time
from  utils.getMacd import stock_macd
ts.set_token("2876ea85cb005fb5fa17c809a98174f2d5aae8b1f830110a5ead6211")


def update(stock_codes,content,date = 'all'):
    if date == 'all':
        now = datetime.now()
        delta32 = timedelta(days=15)
        beg = (now - delta32).date().strftime('%Y%m%d')
    else:
        beg = datetime.now().date().strftime('%Y%m%d')

    # beg = '20230701'
    # end_date='20241111'
    # beg = '20110101'
    # end_time = '20160401'
    total = len(stock_codes)
    each = int(total / 6)
    # each = 1

    each = each if each > 0 else total + 1
    start = 0
    end = each
    # while end < total + each:
    #     df_5: Dict[str, pd.DataFrame] = ef.stock.get_quote_history(stock_codes[start:end], klt=101, beg=beg)
    #     for stock_code, df in df_5.items():
    #         if len(df) == 0:
    #             continue
    #         deal(content, df, 'day')
    #     df_5 = ef.stock.get_quote_history(stock_codes[start:end], klt=102, beg=beg)
    #     for stock_code, df in df_5.items():
    #         if len(df) == 0:
    #             continue
    #         deal(content, df, 'week')
    #     start += each
    #     end += each

    i = 0
    for code in stock_codes:
        # time.sleep(0.3)
        try:
            df = ts.pro_bar(ts_code=code, adj='qfq', start_date=beg)
            if len(df) == 0:
                continue
            deal_ts(content, df, 'day')
            df = pro.stk_week_month_adj(ts_code=code, freq='week',start_date=beg)
            if len(df) == 0:
                continue
            deal_ts_wk(content, df, 'week')
        except:print(code)
        i = i + 1
        if i % 100 == 0:
            print(str(i))

def deal_ts_wk(content,df,freq):
    day_path = content['week']
    code = df.iat[0, 0][:6] + '.csv'
    if not os.path.exists(day_path + code):
        normal = pd.DataFrame(columns=['date', 'open', 'high', 'low', 'close', 'amount', 'vol'])
    else:
        normal = pd.read_csv(day_path + code)
    df['pct_chg'] = round((df['close_qfq']/df['pre_close']-1)*100,2)
    df.drop(axis=1, columns=['ts_code','freq', 'pre_close', 'open','open_hfq','high','high_hfq','low','low_hfq','close','close_hfq','change'], inplace=True)
    df.columns = ['date', 'open', 'high', 'low', 'close', 'amount', 'vol', 'rate']
    df[['vol']] = df[['vol']]*1000
    df = df[::-1].reset_index(drop=True)
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
    if len(normal) > 0:
        normal['date'] = pd.to_datetime(normal['date'], format='%Y-%m-%d')
        # 复牌直接加在后面
        try:
            normal_index = df[df['date'] == normal.iat[-2, 0]].index.tolist()[-1]
            normal.drop(normal.tail(2).index, inplace=True)
            normal = normal.append(df[normal_index:], ignore_index=True)

        except:
            if len(df) == 1 and freq == 'day':
                return
            normal_index = 0
            normal = normal.append(df[normal_index :], ignore_index=True)

    else:
        normal = df
    normal = ma(normal)
    normal.to_csv(day_path + code, index=False)


def deal_ts(content,df,freq):
    day_path = content['normal']
    code = df.iat[0, 0][:6] + '.csv'
    if not os.path.exists(day_path + code):
        normal = pd.DataFrame(columns=['date', 'open', 'high', 'low', 'close', 'amount', 'vol'])
    else:
        normal = pd.read_csv(day_path + code)
    df.drop(axis=1, columns=['ts_code', 'pre_close', 'change'], inplace=True)
    df.columns = ['date', 'open', 'high', 'low', 'close', 'amount', 'vol', 'rate']
    df[['rate', 'amount']] = df[['amount', 'rate']]
    df[['vol', 'amount']] = df[['amount', 'vol']]
    df[['vol']] = df[['vol']]*1000
    df = df[::-1].reset_index(drop=True)
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
    if len(normal) > 0:
        normal['date'] = pd.to_datetime(normal['date'], format='%Y-%m-%d')
        # 复牌直接加在后面
        try:
            normal_index = df[df['date'] == normal.iat[-2, 0]].index.tolist()[-1]
            normal.drop(normal.tail(2).index, inplace=True)
            normal = normal.append(df[normal_index:], ignore_index=True)

        except:
            if len(df) == 1 and freq == 'day':
                return
            normal_index = 0
            normal = normal.append(df[normal_index :], ignore_index=True)

    else:
        normal = df
    normal = ma(normal)
    normal.to_csv(day_path + code, index=False)

def deal(content,df,freq):
    if freq == 'day': day_path = content['normal']
    if freq == 'week': day_path = content['week']
    code = df.iat[0, 1][:6] + '.csv'
    if not os.path.exists(day_path + code):
        normal = pd.DataFrame(columns=['date', 'open', 'high', 'low', 'close', 'amount', 'vol'])
    else:
        normal = pd.read_csv(day_path + code)
    df.drop(axis=1, columns=['股票名称', '股票代码', '振幅', '涨跌额', '换手率'], inplace=True)
    df.columns = ['date', 'open', 'high', 'low', 'close', 'amount', 'vol', 'rate']
    df[['high', 'close']] = df[['close', 'high']]
    df[['high', 'low']] = df[['low', 'high']]
    # df['diff'] = ''
    # df['dea'] = ''
    # df['macd'] = ''
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
    if len(normal) > 0:
        normal['date'] = pd.to_datetime(normal['date'], format='%Y-%m-%d')
        # 复牌直接加在后面
        try:
            normal_index = df[df['date'] == normal.iat[-2, 0]].index.tolist()[-1]
            normal.drop(normal.tail(2).index, inplace=True)
            normal = normal.append(df[normal_index:], ignore_index=True)

        except:
            if len(df) == 1 and freq == 'day':
                return
            normal_index = 0
            normal = normal.append(df[normal_index :], ignore_index=True)

    else:
        normal = df
    normal = ma(normal)
    normal.to_csv(day_path + code, index=False)


def macd(df) -> pd.DataFrame:
    if len(df)<36:
        return df
    if 'macd' not in df.columns:
        df = stock_macd(df)

        return df
    else:
        df_temp = df[33:]
        index = df_temp[df_temp['macd'] == ''].index.tolist()
        if index!=[]:
            df_normal = df[index[0]-33:]
            df_normal = stock_macd(df_normal)
            df = df[:index[0]].append(df_normal[33:])

        return df

def ma(df):
    df["ma5"] = round(talib.MA(df['close'], timeperiod=5),2)
    df["ma10"] = round(talib.MA(df['close'], timeperiod=10),2)
    df["ma15"] = round(talib.MA(df['close'], timeperiod=15),2)
    df["ma20"] = round(talib.MA(df['close'], timeperiod=20),2)
    df["ma30"] = round(talib.MA(df['close'], timeperiod=30),2)
    df["ma50"] = round(talib.MA(df['close'], timeperiod=50),2)
    df["ma120"] = round(talib.MA(df['close'], timeperiod=120),2)
    df["ma200"] = round(talib.MA(df['close'], timeperiod=200),2)
    df["ma250"] = round(talib.MA(df['close'], timeperiod=250),2)

    return df

if __name__ == '__main__':
    with open('config.yaml') as f:
        content = yaml.load(f,Loader=yaml.FullLoader)
        f.close()

    # stock_codes = ['603488']
    # df_5: Dict[str, pd.DataFrame] = ef.stock.get_quote_history(stock_codes, klt=101,beg='20231103')
    # for stock_code, df in df_5.items():
    #     deal(content, df, 'day')
    now = datetime.now().date().strftime('%Y%m%d')
    # now = '20250530'
    pro = ts.pro_api()
    df = pro.daily_basic(ts_code='', trade_date=now,fields='ts_code,trade_date,turnover_rate,volume_ratio,pe,total_mv')
    # df = ts.realtime_list(src='dc')

    df = df[~df['ts_code'].astype(str).str.endswith('BJ')]

    df = df.sort_values(by='ts_code').reset_index(drop=True)

    code = ''
    if code=='':

        # df=ef.stock.get_realtime_quotes("沪深A股")

        update(df.iloc[:,0].tolist(),content)
    else:
        index = df.index[df['ts_code'] == code].tolist()[0]
        update(df.iloc[index-1:,0].tolist(),content)


    print('ok')



