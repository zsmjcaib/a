from typing import Dict
import efinance as ef
import pandas as pd
from multiprocessing import Process
from datetime import datetime,timedelta

import talib
import yaml
import os


def init(stock_codes,content):
    beg = '20170101'
    for freq in [101]:
        total = len(stock_codes)
        each = int(total / 30)
        each = each if each>0 else total+1
        start = 0
        end = each
        while end<=total+each:
            df_5: Dict[str, pd.DataFrame] = ef.stock.get_quote_history(stock_codes[start:end], klt=freq, beg=beg)
            for stock_code, df in df_5.items():
                if len(df) == 0:
                    continue
                deal(content, df, 'day')
            df_5 = ef.stock.get_quote_history(stock_codes[start:end], klt=102, beg=beg)
            for stock_code, df in df_5.items():
                if len(df) == 0:
                    continue
                deal(content, df, 'week')
            start += each
            end += each
        # df_5: Dict[str, pd.DataFrame] = ef.stock.get_quote_history(stock_codes[start:end], klt=freq, beg=beg,freq=101)
        # deal(content, df_5, 'day')
        # df_5 = ef.stock.get_quote_history(stock_codes[start:end], klt=freq, beg=beg,freq=102)
        # deal(content, df, 'week')


def deal(content,df,freq):
    if freq == 'day':day_path = content['normal']
    if freq == 'week':day_path = content['week']
    code = df.iat[0,1]+'.csv'
    if not os.path.exists(day_path + code):
        normal = pd.DataFrame(columns=['date','open','high','low','close','amount','vol'])
    else:
        normal = pd.read_csv(day_path + code)
    df.drop(axis=1, columns=['股票名称', '股票代码','振幅','涨跌额','换手率'],inplace=True)
    df.columns = ['date', 'open', 'high', 'low', 'close', 'amount', 'vol','rate']
    df[['high', 'close']] = df[['close', 'high']]
    df[['high', 'low']] = df[['low', 'high']]
    # df['diff'] = ''
    # df['dea'] = ''
    # df['macd'] = ''
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
    if len(normal)>0:
        normal['date'] = pd.to_datetime(normal['date'], format='%Y-%m-%d')
        #复牌直接加在后面
        try:
            normal_index = df[df['date'] == normal.iat[-2,0]].index.tolist()[-1]
            normal.drop(normal.tail(1).index, inplace=True)
        except:normal_index = 0
        normal = normal.append(df[normal_index+1:],ignore_index=True)
    else:
        normal = df
    normal = ma(normal)
    normal.to_csv(day_path + code, index=False)

def ma(df):
    df["ma5"] = talib.MA(df['close'], timeperiod=5)
    df["ma10"] = talib.MA(df['close'], timeperiod=10)
    df["ma15"] = talib.MA(df['close'], timeperiod=15)
    df["ma20"] = talib.MA(df['close'], timeperiod=20)
    df["ma30"] = talib.MA(df['close'], timeperiod=30)
    df["ma50"] = talib.MA(df['close'], timeperiod=50)
    df["ma120"] = talib.MA(df['close'], timeperiod=120)
    df["ma250"] = talib.MA(df['close'], timeperiod=250)

    return df

if __name__ == '__main__':
    with open('config.yaml') as f:
        content = yaml.load(f,Loader=yaml.FullLoader)
        f.close()
    # init(['601899'],content)

    df=ef.stock.get_realtime_quotes('沪深A股')
    total = len(df)
    each = int(total / 6)
    p1 = Process(target=init, args=(df.iloc[0:each,0].tolist(),content))
    p2 = Process(target=init, args=(df.iloc[each:2 * each,0].tolist(),content))
    p3 = Process(target=init, args=(df.iloc[2 * each: 3 * each,0].tolist(),content))
    p4 = Process(target=init, args=(df.iloc[3 * each:4 * each,0].tolist(),content ))
    p5 = Process(target=init, args=(df.iloc[4 * each:5 * each,0].tolist(),content ))
    p6 = Process(target=init, args=(df.iloc[5 * each:total+1,0].tolist(),content ))
    p1.start()
    p2.start()
    p3.start()
    p4.start()
    p5.start()
    p6.start()

