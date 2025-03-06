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
    rps = pd.DataFrame(columns=['date'])

    for file in os.listdir(path):
        if file == '.DS_Store': continue
        normal = pd.read_csv(content['normal'] + file)

        code = str(file[:6])
        if len(normal) > 250:
            normal[code] = (normal['close'] / normal['close'].shift(int(j)) - 1) * 100

            temp = normal[['date', code]]

            if not rps_temp.axes[1].isin([code]).max():
                rps_temp = pd.merge(rps_temp, temp, how="outer", on="date")
                rps_temp = rps_temp.sort_values('date')

        i = i + 1
        if i % 500 == 0:
            print(j + '_' + str(i))
            rps = pd.merge(rps, rps_temp, how="outer", on="date")
            rps_temp = pd.DataFrame(columns=['date'])

    rps_50 = pd.merge(rps, rps_temp, how="outer", on="date")
    rps_50 = rps_50.sort_values('date')

    rps_50.to_csv(content['rpsinfo_' + j], index=False)
    # print(content['rpsinfo_' + j])



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

    for j in ['120','250','50']:
        path = content['rpsinfo_'+j]
        df = pd.read_csv(path, index_col=0)
        result = pd.DataFrame({})
        for i,row in df.iterrows():
            row_T = row.T
            if row_T.isna().all():continue
            # 对每列进行排序处理，即按日期排序
            df_sorted = row_T.sort_values(ascending=False)

            # 提取排好序的 code 数据列（第一列）
            code_series = df_sorted.iloc[:600]

            # 构造结果 DataFrame 对象
            result_df = pd.DataFrame({
                i: code_series.index
            })
            result = pd.concat([result,result_df],axis=1)
        # result.replace('\t', '', regex=True,inplace=True)
        result.to_csv(content['rps_'+j], index=False)
    f.close()





