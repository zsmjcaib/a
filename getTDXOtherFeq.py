import os
import utils.getMacd as getMacd
import pandas as pd
from pandas import Timedelta
import yaml



# 用通达信小周期，生成大周期数据
def csv_resample(df, rule) -> pd.DataFrame:
    # 重新采样Open列数据
    df_open = round(df['open'].resample(rule=rule, closed='right', label='left').first(), 2)
    df_high = round(df['high'].resample(rule=rule, closed='right', label='left').max(), 2)
    df_low = round(df['low'].resample(rule=rule, closed='right', label='left').min(), 2)
    df_close = round(df['close'].resample(rule=rule, closed='right', label='left').last(), 2)
    df_amount = round(df['amount'].resample(rule=rule, closed='right', label='left').sum(), 2)
    df_volume = round(df['vol'].resample(rule=rule, closed='right', label='left').sum(), 2)
    # print("新周期数据已生成")
    # 生成新周期数据
    df_15t = pd.DataFrame()
    df_15t = df_15t.assign(open=df_open)
    df_15t = df_15t.assign(high=df_high)
    df_15t = df_15t.assign(low=df_low)
    df_15t = df_15t.assign(close=df_close)
    df_15t = df_15t.assign(vol=df_volume)
    df_15t = df_15t.assign(amount=df_amount)
    # 去除空值
    df_15t = df_15t.dropna()

    return df_15t


# 根据通达信5分钟周期数据，生成其他周期数据
def lc5_resample(filepath, name, targetdir, rule) -> None:
    # (通达信.lc5文件路径, 通达信.lc5文件名称, 处理后要保存到的文件夹)
    # 设置处理后保存文件的路径和名称
    file_object_path = targetdir + name.split('.')[0]  + '.csv'
    df = import_csv(filepath)
    if rule == '60T':
        df = round(change_13_11_14_12(df), 2)

    df = csv_resample(df, rule)
    data =getMacd.stock_macd(df)
    data.to_csv(file_object_path)




def lc5_rule(rule,path_dir,target_dir):

        # 设置要转换的新周期
        rule_cycle = rule

        # 读取文件夹下的通达信.lc5.csv文件
        listfile = os.listdir(path_dir)
        # 逐个处理文件夹下的通达信.lc5.csv文件，并生成对应的csv文件，保存到对应周期文件夹下
        for fname in listfile:
            lc5_resample(path_dir + fname, fname, target_dir, rule_cycle)



def change_13_11_14_12(df) -> pd.DataFrame:
    date = []
    for i in df.index:
        if i.hour == 13:
            i = i - Timedelta('02:00:00')
        if i.hour == 14 and i.minute == 0 and i.second == 0:
            i = i - Timedelta('02:00:00')
        date.append(i)
    df = df.assign(date=pd.Series(date, index=df.index))
    df.set_index(['date'], inplace=True)
    return df



# 读取csv文件，返回pd.DataFrame对象
def import_csv(stock_code) -> pd.DataFrame:
    df = pd.read_csv(stock_code)
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
    df.set_index(['date'], inplace=True)
    return df

if __name__ == '__main__':
    with open('config.yaml') as f:
        content = yaml.load(f, Loader=yaml.FullLoader)
        f.close()
        path_dir = content['normal_5_path']
        target_dir = content['normal_30_path']
        lc5_rule('30T', path_dir, target_dir)



    # target_dir='D:/project/data/stock/normal/30/'
    # path_dir = 'D:/project/data/stock/normal/5/'
    # lc5_rule('30T',path_dir,target_dir)
    # # 转换成新周期
    #lc5_rule('10T')
    #lc5_rule('15T')

    #lc5_rule('60T')