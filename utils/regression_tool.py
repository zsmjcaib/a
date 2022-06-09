import pandas as pd
import talib

import os

from utils.judge_ma import array_ma


def csv_resample(df, rule) -> pd.DataFrame:
    # 重新采样Open列数据
    df_open = round(df['open'].resample(rule=rule, closed='right', label='left').first(), 2)
    df_high = round(df['high'].resample(rule=rule, closed='right', label='left').max(), 2)
    df_low = round(df['low'].resample(rule=rule, closed='right', label='left').min(), 2)
    df_close = round(df['close'].resample(rule=rule, closed='right', label='left').last(), 2)
    df_volume = round(df['vol'].resample(rule=rule, closed='right', label='left').sum(), 2)
    df_amount = round(df['amount'].resample(rule=rule, closed='right', label='left').sum(), 2)
    # print("新周期数据已生成")
    # 生成新周期数据
    df_15t = pd.DataFrame()
    df_15t = df_15t.assign(open=df_open)
    df_15t = df_15t.assign(high=df_high)
    df_15t = df_15t.assign(low=df_low)
    df_15t = df_15t.assign(close=df_close)
    df_15t = df_15t.assign(amount=df_amount)
    df_15t = df_15t.assign(vol=df_volume)
    # 去除空值
    df_15t = df_15t.dropna()

    return df_15t

# 读取csv文件，返回pd.DataFrame对象
def import_csv(df,rule) -> pd.DataFrame:

    df =df.copy()
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
    df = df.set_index(['date'])
    df = csv_resample(df, rule)
    df.reset_index(inplace=True)
    return df


def macd(df):
    diff, dea, macd = talib.MACD(df["close"],
                                 fastperiod=12,
                                 slowperiod=26,
                                 signalperiod=9)
    df["diff"] = round(diff, 2)
    df["dea"] = round(dea, 2)
    df["macd"] = round(macd * 2, 2)
    return df


def stock_macd(df) -> pd.DataFrame:
    if len(df)<36:
        return df
    if 'macd' not in df.columns:
        df = macd(df)
        return df
    else:
        df_temp = df[33:]
        index = df_temp[df_temp['macd'] == ''].index.tolist()
        if index!=[]:
            df_normal = df[index[0]-33:]
            df_normal = stock_macd(df_normal)
            df = df[:index[0]].append(df_normal[33:])

        return df


def test(code,content):
    real = pd.read_csv(content['normal'] +code)
    normal = real[:250]


    #开始回测
    for i, row in real[250:].iterrows():
        normal = normal.append(row).reset_index(drop=True)
        date = normal['date'].iloc[-1]
        if date == '2019-09-19':
            print('')
        if array_ma(normal):
            record = pd.DataFrame(columns=['date', 'key', 'flag', 'temp'])
            print(code+'  '+normal['date'].iloc[-1])

        l = pd.DataFrame(
            {'code':code,'date': normal['date'].iloc[-1] }, index=[1])
        # if i ==5000:
        #     print(i)






