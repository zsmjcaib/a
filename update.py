from typing import Dict
import efinance as ef
import pandas as pd
from multiprocessing import Process
from datetime import datetime,timedelta

import talib
import yaml
import os

from  utils.getMacd import stock_macd



def update(stock_codes,content,date = 'all'):
    if date == 'all':
        now = datetime.now()
        delta32 = timedelta(days=10)
        beg = (now - delta32).date().strftime('%Y%m%d')
    else:
        beg = datetime.now().date().strftime('%Y%m%d')
    for freq in [101]:
        total = len(stock_codes)
        each = int(total / 30)
        each = each if each>0 else total+1
        start = 0
        end = each
        # while end<=total+each:
        #     df_5: Dict[str, pd.DataFrame] = ef.stock.get_quote_history(stock_codes[start:end], klt=freq, beg=beg)
        #     for stock_code, df in df_5.items():
        #         if len(df) == 0:
        #             continue
        #         deal(content, df, 'day')
        df_5: Dict[str, pd.DataFrame] = ef.stock.get_quote_history(stock_codes[start:end], klt=freq, beg=beg)
        deal(content, df_5, 'day')
        start += each
        end += each

def deal(content,df,freq):
    # normal_path = content['normal_'+str(freq)+'_path']
    # simple_path = content['simple_'+str(freq)+'_path']
    # deal_path = content['deal_'+str(freq)+'_path']
    # line_path = content['line_'+str(freq)+'_path']
    day_path = content['normal']
    code = df.iat[1,1]+'.csv'
    if not os.path.exists(day_path + code):
        normal = pd.DataFrame(columns=['date','open','high','low','close','amount','vol'])
    else:
        normal = pd.read_csv(day_path + code)
    df.drop(axis=1, columns=['股票名称', '股票代码','振幅','涨跌幅','涨跌额','换手率'],inplace=True)
    df.columns = ['date', 'open', 'high', 'low', 'close', 'amount', 'vol']
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
    df["ma5"] = talib.MA(df['close'], timeperiod=5)
    df["ma10"] = talib.MA(df['close'], timeperiod=10)
    df["ma20"] = talib.MA(df['close'], timeperiod=20)
    df["ma30"] = talib.MA(df['close'], timeperiod=30)
    df["ma60"] = talib.MA(df['close'], timeperiod=60)
    df["ma120"] = talib.MA(df['close'], timeperiod=120)
    df["ma250"] = talib.MA(df['close'], timeperiod=250)

    return df

if __name__ == '__main__':
    with open('config.yaml') as f:
        content = yaml.load(f,Loader=yaml.FullLoader)
        f.close()

    # df=ef.stock.get_realtime_quotes()
    # total = len(df)
    # each = int(total / 6)
    # p1 = Process(target=update, args=(df.iloc[0:each,0].tolist(),content))
    # p2 = Process(target=update, args=(df.iloc[each:2 * each,0].tolist(),content))
    # p3 = Process(target=update, args=(df.iloc[2 * each: 3 * each,0].tolist(),content))
    # p4 = Process(target=update, args=(df.iloc[3 * each:4 * each,0].tolist(),content ))
    # p5 = Process(target=update, args=(df.iloc[4 * each:5 * each,0].tolist(),content ))
    # p6 = Process(target=update, args=(df.iloc[5 * each:total+1,0].tolist(),content ))
    # p1.start()
    # p2.start()
    # p3.start()
    # p4.start()
    # p5.start()
    # p6.start()
    update('601899',content)


