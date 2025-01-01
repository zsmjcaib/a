from typing import Dict
import efinance as ef
import pandas as pd
from datetime import datetime,timedelta

import talib
import yaml
import os

from  utils.getMacd import stock_macd



def update(stock_codes,content,date = 'all'):
    if date == 'all':
        now = datetime.now()
        delta32 = timedelta(days=15)
        beg = (now - delta32).date().strftime('%Y%m%d')
    else:
        beg = datetime.now().date().strftime('%Y%m%d')

    # beg = '20230601'
    # end_date='20241111'
    # beg = '20110101'
    # end_time = '20160401'
    total = len(stock_codes)
    each = int(total / 100)
    # each = 1

    each = each if each > 0 else total + 1
    start = 0
    end = each
    while end <= total + each:
        df_5: Dict[str, pd.DataFrame] = ef.stock.get_quote_history(stock_codes[start:end], klt=101, beg=beg)
        for stock_code, df in df_5.items():
            if len(df) == 0:
                continue
            # print(stock_code)
            deal(content, df, 'day')
        df_5 = ef.stock.get_quote_history(stock_codes[start:end], klt=102, beg=beg)
        for stock_code, df in df_5.items():
            if len(df) == 0:
                continue
            deal(content, df, 'week')
        start += each
        end += each

    # df_5: Dict[str, pd.DataFrame] = ef.stock.get_quote_history(stock_codes[start:end], klt=101, beg=beg)
    # deal(content, df_5, 'day')


def deal(content,df,freq):
    if freq == 'day': day_path = content['normal']
    if freq == 'week': day_path = content['week']
    code = df.iat[0, 1] + '.csv'
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




    df=ef.stock.get_realtime_quotes("沪深A股")
    total = len(df)

    update(df.iloc[:,0].tolist(),content)


    print('ok')



