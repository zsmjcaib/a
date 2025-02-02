import doupand as dp
from datetime import datetime,timedelta
import os
import pandas as pd
import talib
import yaml
from  ashare import *

dp.set_token("6F80CF411B18687152187857EAC8E6A0AE565798636DE24F0CE754F99B4E0E63")




def deal(content,df,freq,code):
    if freq == 'day': day_path = content['composite']
    # code = df.iat[0, 1] + '.csv'
    if not os.path.exists(day_path + code):
        normal = pd.DataFrame(columns=['date', 'open', 'high', 'low', 'close', 'vol'])
    else:
        normal = pd.read_csv(day_path + code)
    df = df.reset_index()
    df.columns = ['date', 'open', 'high', 'low', 'close', 'vol']
    df['rate'] = round((df['close']/df['close'].shift(1) -1)*100,2)
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
    dr = dp.data_reader()
    from datetime import datetime

    beg = datetime.now().date().strftime('%Y%m%d')

    df = get_price('399006.XSHE', frequency='1d', count=1800, end_date=beg)  # 可以指定结束日期，获取历史行情
    print(df.index[-1])
    with open('config.yaml') as f:
        content = yaml.load(f, Loader=yaml.FullLoader)
        f.close()
    day_path = content['composite']
    code =  '399006.csv'
    deal(content, df, 'day',code)
