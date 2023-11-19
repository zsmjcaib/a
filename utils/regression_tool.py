import pandas as pd
import talib
import yaml
from multiprocessing import Process

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


def test(content,j):
    i = 0
    path = content['normal']
    # for j in [ '120', '250','50']:
    rps_temp = pd.DataFrame(columns=['date'])
    if not os.path.exists(content['rpsinfo_'+j]):
        rps = pd.DataFrame(columns=['date'])


        for file in os.listdir(path):
            if file == '.DS_Store': continue
            normal = pd.read_csv(content['normal'] + file)
            # freq=j+"_"+str(file[:6])

            code = str(file[:6])
            if len(normal) > 250:
                normal[code] = (normal['close'] / normal['close'].shift(int(j)) - 1) * 100

                temp = normal[['date', code]]

                if not rps_temp.axes[1].isin([code]).max():
                    rps_temp = pd.merge(rps_temp, temp, how="outer", on="date")
                    rps_temp = rps_temp.sort_values('date')

            i = i + 1
            if i % 500 == 0:
                print(i)

        rps_50 = pd.merge(rps, rps_temp, how="outer", on="date")
        rps_50 = rps_50.sort_values('date')

        rps_50.to_csv(content['rpsinfo_' + j], index=False)
        print(content['rpsinfo_' + j])
    else:
        rps  = pd.read_csv(content['rpsinfo_'+j])
        for file in os.listdir(path):
            if file == '.DS_Store': continue
            normal = pd.read_csv(content['normal'] + file)
            # freq=j+"_"+str(file[:6])

            code = str(file[:6])
            if len(normal) > 250:
                normal[code] = (normal['close'].iloc[-10:] / normal['close'].shift(int(j)).iloc[-10:] - 1) * 100

                temp = normal[['date', code]].iloc[-10:]


                if not rps_temp.axes[1].isin([code]).max():
                    rps_temp = pd.merge(rps_temp, temp, how="outer", on="date")
                    rps_temp = rps_temp.sort_values('date')
            i = i + 1
            if i % 500 == 0:
                print(j+'_'+str(i))
        rps.drop(rps.tail(6).index, inplace=True)
        index = rps_temp[rps_temp['date'] == rps.iat[-1, 0]].index.tolist()[-1]

        rps_50 = rps.append(rps_temp[index+1:], ignore_index=True)
        rps_50.to_csv(content['rpsinfo_' + j], index=False)
        i = 0

        #开始回测
        # for i, row in real[250:].iterrows():
        #     normal = normal.append(row).reset_index(drop=True)
        #     date = normal['date'].iloc[-1]
        #     if date == '2019-09-19':
        #         print('')
        #     if array_ma(normal):
        #         record = pd.DataFrame(columns=['date', 'key', 'flag', 'temp'])
        #         print(code+'  '+normal['date'].iloc[-1])
        #
        #     l = pd.DataFrame(
        #         {'code':code,'date': normal['date'].iloc[-1] }, index=[1])
            # if i ==5000:
            #     print(i)

if __name__ == '__main__':
    with open('../config.yaml') as f:
        content = yaml.load(f,Loader=yaml.FullLoader)
    p1 = Process(target=test, args=(content,'50'))
    p2 = Process(target=test, args=(content,'120'))
    p3 = Process(target=test, args=(content,'250'))
    p1.start()
    p2.start()
    p3.start()
    p1.join()
    p2.join()
    p3.join()
    print('ok')

    # test(content)




