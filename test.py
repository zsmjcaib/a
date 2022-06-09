import os
import pandas as pd
import talib
import yaml

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
        path = content['normal']
        f.close()
        for code in os.listdir(path):
            df = pd.read_csv(path+code)
            # df = ma(df)
            df[['amount', 'vol']] = df[['vol', 'amount']]
            df.to_csv(path + code, index=0)
