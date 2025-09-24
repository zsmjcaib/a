import os
# import akshare as ak
import threading

import pandas as pd
import yaml
import efinance as ef
import numpy as np
import math
from utils.deal import find_point
from utils.point import simpleTrend
from utils.deal_day import find_point_day
import tushare as ts
from datetime import datetime

# from utils.std import check

now = ef.stock.get_realtime_quotes()
ts.set_token("2876ea85cb005fb5fa17c809a98174f2d5aae8b1f830110a5ead6211")




# file_list = os.listdir(content['performance'])
# dfs = []
# for file_name in file_list:
#     if file_name.endswith('.csv'):
#         file_path = os.path.join(content['performance'], file_name)
#         df = pd.read_csv(file_path, dtype=str)
#         # df_unique = df.drop_duplicates()
#         dfs.append(df)
#
# # 合并所有数据框
# performance_df = pd.concat(dfs)


def pocket(content,start,end):
    rps_50 = pd.read_csv(content['rps_50'], dtype=str)
    rps_120 = pd.read_csv(content['rps_120'], dtype=str)
    rps_250 = pd.read_csv(content['rps_250'], dtype=str)

    for date, codelist in rps_250.iloc[:, start:end].iteritems():
        # print(date)
        codelist = rps_120[date].iloc[:350].append(rps_250[date].iloc[:300]).append(rps_50[date].iloc[:400]).drop_duplicates()
        for _, code in codelist.iteritems():
            com(date,code)




def com(date, code):
    df = pd.read_csv(content['normal'] + code + '.csv')
    msg=''
    # if  code == '300007' :#and date=='2019-04-10'
    #     print('ok')
    #     print(1)
    #     pass
    try:
        index = df[df["date"] == date].index[0]
    except:
        # print(date)
        return
    close = df.loc[index, 'close']
    if len(df[:index]) < 260:
        return
    market_vaule = now[now['股票代码'] == code]['总市值'].iloc[0]

    if market_vaule == '-':
        return
    new_price = now[now['股票代码'] == code]['最新价'].iloc[0]
    if new_price == '-':
        new_price = now[now['股票代码'] == code]['昨日收盘'].iloc[0]

    market_vaule = market_vaule / new_price * close
    if market_vaule < 7000000000  :
        return
    if df.iloc[index]['close'] < df.iloc[index]['open']:
        return
    high = df.loc[index, 'high']
    last_close = df.loc[index - 1, 'close']
    vol = df.loc[index, 'vol']
    negative_vol_20 = get_vol(df, 'negative', index - 20, index - 1)
    ma20 = df.loc[index, 'ma20']
    rate = df.loc[index, 'rate']
    if rate > 5 and vol > 100000000 and vol > negative_vol_20 * 1.5:  #
        pass
    elif (rate > 9.8 and close > ma20):
        pass
    else:
        return
    if df.iloc[index]['ma50'] > close * 0.98 and close != high:
        return
    if df.iloc[index - 1]['low'] > df.iloc[index - 4:index - 1]['high'].max():
        if df.iloc[index - 1]['low'] > df.iloc[index - 10:index - 1]['close'].max() \
                and df.iloc[index-1]['high']==df.iloc[index-200:index]['high'].max():
            pass
        else:
            return
    if df.iloc[index - 24:index]['close'].max() * 0.95 < df.iloc[index - 24:index]['close'].mean() < \
            df.iloc[index - 24:index]['close'].min() * 1.05:
        if vol > df.iloc[index - 20:index]['vol'].mean() * 3:
            return
    if vol > df.iloc[index - 8:index]['vol'].mean() * 5 and vol < 600000000:
        return
    ma20_list = df.loc[index - 120:index - 1, 'ma20']
    ma20_result = (ma20_list / ma20_list.shift(1) - 1)[1:] * 100
    if (ma20_result > 1).sum() < 4 and (df.iloc[index - 120:index - 1]['rate'] > 7).sum() < 3:
        if (df.iloc[index - 6:index]['rate'] > 5).sum() > 0 and (
                df.iloc[index - 8:index]['rate'] < -6).sum() == 0:
            pass
        elif vol < df.iloc[index - 60:index]['vol'].mean() * 2:
            pass
        elif close > df.iloc[index - 5:index]['close'].mean() * 1.08:
            pass
        else:
            return
    ma20_list = df.loc[index - 8:index - 1, 'ma20']
    ma20_result = (ma20_list / ma20_list.shift(2) - 1)[1:] * 100
    ma30_list = df.loc[index - 8:index - 1, 'ma30']
    ma30_result = (ma30_list / ma30_list.shift(2) - 1)[1:] * 100
    down_5 = df.iloc[index - 8:index + 1]['ma5'] - df.iloc[index - 8:index + 1]['ma20']
    if (not (ma20_result > 0).any() or not (ma30_result > 0).any()) and not (down_5 > 0).any():
        if vol < df.iloc[index - 6:index]['vol'].max() * 1.2:
            return
        if close * 1.07 > df.iloc[index - 40:index]['high'].max():
            return
        if df.iloc[index - 1]['close'] > df.iloc[index - 10:index]['close'].min() * 1.07:
            return
        if (df.iloc[index - 20:index]['rate'] < -6).sum() > 1 or (df.iloc[index - 20:index]['rate'] < -7).sum() > 0:
            return
        test_index = df.iloc[index - 20:index]['low'].idxmin()
        if not (test_index + 4 > index):
            return

    rate = df.loc[index, 'rate']
    open = df.loc[index, 'open']
    close = df.loc[index, 'close']
    low = df.loc[index, 'low']

    if high / last_close > 1.098 and rate < 8 and df.iloc[index]['vol'] < df.iloc[index - 50:index - 1][
        'vol'].max() * 0.85 and close < df.iloc[index - 50:index - 1]['close'].max() * 1.03:
        return


    if open == low and close == high:
        if open > df.iloc[index - 20:index]['high'].max():
            msg+='index+1 open >5 '

    # # 要改 参数2可以
    ma5 = df.iloc[index]['ma5']
    ma10 = df.iloc[index]['ma10']
    ma50 = df.iloc[index]['ma50']
    condition_3 = (df['open'].iloc[index - 2:index + 1] < df['ma10'].iloc[index - 2:index + 1] * 1.02)
    condition_4 = (df['close'].iloc[index - 2:index] < df['ma10'].iloc[index - 2:index] * 1.02)
    flag = True
    if not (condition_3.any() or condition_4.any()):
        if (df.iloc[index - 2:index]['rate'].sum() > 5 or df.iloc[index - 2:index]['rate'].max() > 3.5):
            if close < df.iloc[index - 120:index - 20]['close'].max() * 1.05:
                return
            if (df.iloc[index - 20]['close'] < df.iloc[index - 20]['ma120'] * 1.05).any():
                return
            if df.iloc[index]['ma30'] < ma50 * 1.05:
                return
            if (df.iloc[index - 20:index]['rate'] > 9).sum() < 1:
                return
            if (df.iloc[index - 20:index]['ma5'] < df.iloc[index - 20:index]['ma10']).sum() == 0:
                return
            if ma10>ma20*1.12:
                return
            flag=False
        if not (df['low'].iloc[index - 2:index] < df['ma5'].iloc[index - 2:index] * 1.02).any() and flag:
            return
        if (df.iloc[index - 2:index]['open'] > df.iloc[index - 2:index]['close']).any() and flag:
            if df.iloc[index - 2:index]['rate'].min() > -3.5 and df.iloc[index - 11:index - 1]['rate'].max() < 9 \
                    and vol > df.iloc[index - 2:index]['vol'].max() * 1.2 and low < df.iloc[index][
                'ma5'] * 0.995:
                pass
            elif close > df.iloc[index - 100:index]['close'].max() * 1.04 and open < ma5:
                pass
            elif (df.iloc[index - 2:index]['low'] < df.iloc[index - 2:index]['ma5']).all() and (
                    df.iloc[index - 2:index]['low'] < df.iloc[index - 2:index]['ma10']).any():
                pass
            else:
                return
        elif flag:
            return

    ma20_list = df.loc[index - 20:index, 'ma20']
    ma20_result = (ma20_list / ma20_list.shift(1) - 1)[1:] * 100
    result = (ma20_result > 0).sum()
    if result < 3:
        low_close_20 = df.loc[index - 20:index]['close'].min()
        if low_close_20 * 1.2 < close:
            return

    rate_sum = (abs(df.iloc[index - 51:index - 10]['rate']) > 5).sum()
    rate_sum_1 = (df.iloc[index - 51:index]['open'] / df.iloc[index - 51:index]['close'] > 1.05).sum()
    if (rate_sum > 12 and rate_sum_1 > 4):
        if df.iloc[index-1]['close']>min(df.iloc[index-1]['ma20'],df.iloc[index-1]['ma30'])*1.12 or low>ma5:
            return

    if close < df.iloc[index - 20:index]['close'].max() * 0.95:
        if (df.iloc[index - 20:index]['rate'] < -6).sum() > 1 or (df.iloc[index - 20:index]['rate'] < -4).sum() > 2:
            return
    score = 0
    for i, row in df[index - 6:index].iterrows():  # index-6
        if df.iloc[i]['close'] > df.iloc[i - 100:i - 1]['close'].max():  # i - 100:i - 1
            score += 1

    if score > 4 and (df[index - 12:index]['rate']>8).sum()==0:  # 4
        return
    if df.iloc[index-20:index]['close'].min()*2.5<close or (df.iloc[index-20:index]['rate']>9).sum()>5:
        return
    if (df.iloc[index - 5:index]['open'] / df.iloc[index - 5:index]['close']).max() > 1.05:
        if (df.iloc[index - 6:index]['low'] > df.iloc[index - 6:index]['ma20']).all() and not (
                (df.iloc[index - 3:index]['ma5'] > df.iloc[index - 3:index]['ma10']).all() or not (
                df.iloc[index - 3:index]['ma10'] > df.iloc[index - 3:index]['ma20']).all()):
            return
        if (df.iloc[index - 5:index]['low'] / df.iloc[index - 5:index][
            'ma20']).min() < 1.02 and high != close and rate < 13:
            test_index = df.iloc[index - 9:index - 1]['rate'].idxmax()
            if close * 0.98 < df.iloc[test_index:test_index + 2]['close'].max() \
                    and df.iloc[test_index:test_index + 2]['high'].max() > df.iloc[index - 20:index][
                'high'].max() * 0.98 \
                    and df.iloc[test_index]['rate'] > 7:
                pass
            else:
                return

    if close > df.iloc[index - 60:index]['high'].max() and vol < df.iloc[index - 40:index]['vol'].mean() * 2.5 \
            and (close != high or rate < 9.5) and vol < 250000000 and \
            df.iloc[index]['amount'] < df.iloc[index - 10:index]['amount'].max() * 1.1:
        return

    if (df.iloc[index - 5:index]['close'] > df.iloc[index - 5:index]['ma200'] * 1.1).sum() == 0:
        return
    vol_last_5 = df[index - 5:index][df[index - 5:index]['rate'] < 0]['vol'].max()
    vol_20 = df.loc[index - 20:index - 1, 'vol'].mean() * 1.5
    vol_30 = df.loc[index - 30:index - 1, 'vol'].mean() * 1.5
    result = df[index - 10:index][df.iloc[index - 10:index]['rate'] < 0]

    high_120 = df.loc[index - 120:index - 1, 'high'].max()
    high_index = df[df["high"] == high_120].index
    high_index = high_index[high_index < index][-1]

    ma120 = df.loc[index, 'ma120']
    ma200 = df.loc[index, 'ma200']
    ma250 = df.loc[index, 'ma250']
    ma50 = df.loc[index, 'ma50']
    ma30 = df.loc[index, 'ma30']
    if close*0.95>ma50<df.iloc[index-2]['ma50'] and (close>ma200*1.5 or ma50<max(ma20,ma30)):
        return
    subset = df.iloc[index - 60:index - 1]
    filtered_rows = subset[(subset['rate'] > 9)]
    if not filtered_rows.empty:
        flag = 0
        for last_index in filtered_rows.index:
            if df.iloc[last_index + 1]['open'] > df.iloc[last_index + 1]['close'] * 1.07:
                flag += 1
        if flag == 2:
            return
    subset = df.iloc[index - 150:index - 1]
    filtered_rows = subset[(subset['rate'] > 9.5)]
    if not filtered_rows.empty:
        last_index = filtered_rows.index[-1]
        if df.iloc[last_index + 1]['open'] > df.iloc[last_index + 1]['close'] * 1.08 and \
                df.iloc[last_index + 2:index - 1]['rate'].max() < 9:
            if index < last_index + 10 or df.iloc[last_index + 1]['open'] / df.iloc[last_index + 1]['close'] < \
                    (df.iloc[last_index + 2:index - 1]['open'] / df.iloc[last_index + 2:index - 1][
                        'close']).max():
                pass
            else:
                return

    if df.iloc[index - 9]['close'] > df.iloc[index - 3:index]['close'].min() * 1.13 or \
            df.iloc[index - 8]['close'] > df.iloc[index - 3:index]['close'].min() * 1.12:
        return

    if (df.iloc[index - 40:index - 1]['rate'] < -3).sum() < 4 and (
            df.iloc[index - 40:index - 1]['rate'] < 0).sum() < 13:  #
        return
    subset = df.iloc[index - 80:index - 1]
    filtered_rows = subset[(subset['rate'] > 9.5)]
    if not filtered_rows.empty:
        last_index = filtered_rows.index[-1]
        filtered_high_index = df.iloc[last_index + 1:last_index + 5]['high'].idxmax()
        filtered_min_index = df.iloc[last_index + 1:last_index + 5]['close'].idxmin()
        if filtered_min_index > filtered_high_index:
            if (df.iloc[last_index + 1:last_index + 5]['high'].max() > df.iloc[last_index + 1:last_index + 5][
                'close'].min() * 1.18 or
                df.iloc[last_index + 1:last_index + 6]['high'].max() > df.iloc[last_index + 1:last_index + 6][
                    'close'].min() * 1.2) \
                    and (df.iloc[last_index + 1:index - 1]['rate'] > 7.5).sum() < 1:
                if df.iloc[filtered_high_index - 20]['close'] * 1.7 < df.iloc[filtered_high_index]['high']:
                    return
                if df.iloc[filtered_high_index]['high'] < close:
                    return
    filtered_rows = subset[(subset['open'] > close) | (subset['close'] > close)]
    if not filtered_rows.empty:
        last_index = filtered_rows.index[-1]
        if df.iloc[last_index]['open'] > df.iloc[last_index]['close'] * 1.07 and \
                df.iloc[last_index - 3:last_index]['rate'].max() > 8:
            return
    subset = df.iloc[index - 50:index - 1]
    filtered_rows = subset[(subset['rate'] > 9.5)]
    if len(filtered_rows) > 0:
        last_index = filtered_rows.index[-1]
        if (df.iloc[last_index + 1:last_index + 7]['rate'] < 0).all() or (
                df.iloc[last_index + 1:last_index + 13]['rate'] < 0).sum() > 8:
            if df.iloc[last_index-3:last_index]['rate'].max()<8:
                return
    if df.iloc[index - 50:index - 5]['high'].max() * 0.9 < df.iloc[index]['close'] < \
            df.iloc[index - 50:index - 5]['high'].max():
        max_5_index = df.iloc[index - 50:index - 5]['high'].nlargest(9).index
        if (df.iloc[max_5_index]['high'] / df.iloc[max_5_index][['open', 'close']].max(
                axis=1) > 1.05).sum() > 1 and \
                (df.iloc[max_5_index]['high'] / df.iloc[max_5_index][['open', 'close']].max(
                    axis=1) > 1.03).sum() > 3:
            return
        test_index = df.iloc[index - 20:index - 5]['high'].idxmax()
        if df.iloc[test_index]['high'] / df.iloc[test_index][['open', 'close']].max() > 1.05:
            if (df.iloc[test_index]['rate'] < 0):
                return
        score = 0
        max_5_index = df.iloc[index - 50:index - 5]['high'].nlargest(9).index
        max_5_index = df.iloc[max_5_index][df.iloc[max_5_index]['high'] * 1.03 > close].index
        for i, row in df.iloc[max_5_index].iterrows():
            if df.iloc[i]['high'] > max(df.iloc[i]['close'], df.iloc[i]['open']) * 1.05:
                score += 1
        if score > 1:
            return
    if df.iloc[index - 2]['rate'] > 5 and close != high and rate < 15:
        if df.iloc[index - 1]['high'] / df.iloc[index - 1]['close'] > 1.02 and df.iloc[index - 1]['open'] > \
                df.iloc[index - 1]['close'] \
                and df.iloc[index - 1]['open'] / df.iloc[index - 1]['low'] > 1.02:
            return
    subset = df.iloc[index - 150:index - 15]
    filtered_rows = subset[(subset['rate'] < - 7)]
    if len(filtered_rows) > 0:
        last_index = filtered_rows.index[-1]
        if df.iloc[last_index - 1]['rate'] > 5 and open < df.iloc[last_index]['open'] \
                and df.iloc[last_index:index - 1]['rate'].max() < 8 and close < df.iloc[last_index - 1][
            'close'] * 1.05:
            return
    subset = df.iloc[index - 50:index - 5]
    filtered_rows = subset[(subset['rate'] >9)]
    if len(filtered_rows) > 0:
        last_index = filtered_rows.index[-1]
        if (df.iloc[last_index+1:last_index+9]['open']>df.iloc[last_index+1:last_index+9]['close']).sum()>5 \
                and df.iloc[last_index+1:last_index+8]['rate'].max()<7 and close<df.iloc[index-50:index]['high'].max():
            return
    score = 0
    lst = []
    for i in range(index - 60, index - 2):
        if df.iloc[i]['high'] == df.iloc[i - 3:i + 4]['high'].max() and high * 1.02 > df.iloc[i][
            'high'] > high * 0.99:
            lst.append(i)
    j = 0
    while j < len(lst) - 1:
        if lst[j + 1] == lst[j] + 1:
            lst.pop(j)
        else:
            j += 1
    if len(lst) > 2:
        score += 1
    lst = []
    for i in range(index - 30, index - 2):
        if df.iloc[i]['high'] == df.iloc[i - 2:i + 3]['high'].max() and high * 1.02 > df.iloc[i][
            'high'] > high * 0.98:
            lst.append(i)
    j = 0
    while j < len(lst) - 1:
        if lst[j + 1] == lst[j] + 1:
            lst.pop(j)
        else:
            j += 1
    if len(lst) > 2:
        score += 1
    if score > 0 and not (2.8 > df.iloc[index - 10:index]['rate'].abs().mean() > 2):
        return
    t=0
    test_index_list = df.iloc[index - 45:index - 2]['high'].nlargest(8).index
    for test_index in test_index_list:
        if high>df.iloc[test_index]['high'] :break
        if df.iloc[test_index]['vol']>vol:
            if df.iloc[test_index]['rate']<0:
                t=t+1
    if t>2 and close<max(ma20,ma30,ma50)*1.05:
        return
    if close > ma120 * 1.05:
        if not (vol_20 < vol * 1.05 or vol_30 < vol * 1.05 or rate > 9.8):
            return
        test_index = df.iloc[index - 9:index - 1]['rate'].idxmax()
        if (df.iloc[index-10:index]['rate']>9).sum()>1 :
            pass
        elif result.empty:
            pass
        elif vol > result['vol'].max() * 1.4 or ((rate > 9.8 and close == high) or rate > 13) \
                or (vol > 3000000000 and vol > vol_last_5):
            pass
        elif (df.iloc[index - 80:index]['close'] * 1.04 > df.iloc[index - 80:index]['ma30']).all() or \
                (df.iloc[index - 100:index]['close'] * 1.02 > df.iloc[index - 100:index]['ma50']).all():
            pass
        elif df.iloc[index - 20:index]['vol'].mean() * 1.2 < vol and high < df.iloc[index - 40:index][
            'high'].max() * 1.04 \
                and (df.iloc[index - 10:index]['ma10'] < df.iloc[index - 10:index]['close']).all():
            pass

        elif (df.iloc[index - 50:index]['close'] > df.iloc[index - 50:index]['ma50']).all() and vol > result[
            'vol'].max() * 1.1 \
                and (vol > df.iloc[index - 10:index]['vol'].max() * 1.1 or (
                df.iloc[index - 10:index]['rate'] > 4).sum() > 1) or \
                (df.iloc[index - 11:index]['close'] > df.iloc[index - 11:index]['open']).sum() > 6:
            pass
        elif  (df.iloc[index - 20:index]['low'] > df.iloc[index - 20:index]['ma20'] * 1.02).all():
            pass
        elif close * 0.98 < df.iloc[test_index:test_index + 2]['close'].max() \
                and df.iloc[test_index:test_index + 2]['high'].max() > df.iloc[index - 20:index][
            'high'].max() * 0.98 \
                and df.iloc[test_index]['rate'] > 7:
            pass
        elif np.max(df.iloc[index - 6:index][['close', 'open']].values) < np.min(
                df.iloc[index - 6:index][['close', 'open']].values) * 1.07 and \
                np.max(df.iloc[index - 6:index][['high', 'low']].values) < np.min(
            df.iloc[index - 6:index][['high', 'low']].values) * 1.11 \
                and high > df.iloc[index - 80:index]['high'].max() * 1.03 and vol > df.iloc[index - 20:index][
            'vol'].max() * 0.75:
            pass
        elif (df.iloc[index - 20:index + 1]['low'] > df.iloc[index - 20:index + 1]['ma20'] * 1.0).all() and \
                (df.iloc[index - 10:index + 1]['close'] > df.iloc[index - 10:index + 1]['ma20'] * 1.05).all():
            pass
        elif df.iloc[index - 20:index]['high'].max() == df.iloc[index - 80:index]['high'].max() \
                and (df.iloc[index - 5:index]['low'] < df.iloc[index - 5:index]['ma20']).any():
            test_index = df.iloc[index - 20:index]['high'].idxmax()
            if df.iloc[test_index:index]['close'].min() * 1.1 < df.iloc[test_index]['high'] \
                    and ((df.iloc[index - 4:index]['close'] < df.iloc[index - 4:index]['ma10']).all()
                         or (df.iloc[index - 10:index]['close'] < df.iloc[index - 10:index]['ma10']).sum() > 7) \
                    and df.iloc[test_index]['high'].max() > df.iloc[test_index - 30:test_index - 10][
                'high'].max() * 1.06:

                pass
            else:

                return
        else:
            return
    else:
        return
    if df.iloc[index - 1]['close'] == df.iloc[index - 1]['high'] and df.iloc[index - 1][
        'rate'] > 9.5 and close != high:
        return

    if high > close * 1.04 and rate < 10:
        return
    ma20_sum = (df.iloc[index - 41:index]['ma20'] > df.iloc[index - 41:index]['close']).sum()
    ma20_close = (df.iloc[index - 21:index]['close'] / df.iloc[index - 21:index]['ma20'] > 1.1).sum()
    diff = (df.iloc[index - 6:index]['open'] - df.iloc[index - 6:index]['close']) / df.iloc[index - 6:index]['open']
    negative_sum = diff[diff < 0].sum()
    negative_amount = (df.iloc[index - 6:index]['open'] > df.iloc[index - 6:index]['close']).sum()
    if ma20_sum == 0 and (ma20_close < 3 or (negative_sum < -0.1 and negative_amount < 3)):
        return

    if df.iloc[index]['amount'] < df.iloc[index - 51:index]['amount'].max() * 0.4 and close != df.iloc[index][
        'high']:
        return
    if df.iloc[index]['amount'] < df.iloc[index - 51:index]['amount'].max() * 0.3 and close < \
            df.iloc[index - 51:index]['close'].max():
        return

    if close != high and close < df.iloc[index - 16:index]['high'].max() * 1.03 and \
            close > df.iloc[index - 251:index]['high'].max() and df.iloc[index]['rate'] < 10.5:
        test_index = df.iloc[index - 16:index]['high'].idxmax()
        if df.iloc[test_index]['close'] * 1.03 < df.iloc[test_index][
            'high'] \
                and df.iloc[test_index]['open'] * 1.03 < df.iloc[test_index]['high']:
            if  not((df.iloc[index-30:index]['ma30']<df.iloc[index-30:index]['close']).all() and ma20>ma30*1.04):
                return

    if (df.iloc[index - 30:index]['close'] > df.iloc[index - 30:index]['ma30']).all() and \
            (df.iloc[index - 30:index]['low']*1.04 > df.iloc[index - 30:index]['ma30']).all():
        test_data = df.iloc[index - 21:index]
        if len(test_data[(test_data['ma5'] < test_data['ma20'])]) + len(
                test_data[(test_data['ma10'] < test_data['ma20'])]) > 6:
            return
    if df.iloc[index - 1]['close'] > df.iloc[index - 100:index - 1]['close'].max() and df.iloc[index - 2][
        'close'] > df.iloc[index - 100:index - 2]['close'].max() \
            and df.iloc[index - 1]['ma20'] > df.iloc[index - 2]['ma20'] * 1.005 and df.iloc[index - 1]['ma30'] > \
            df.iloc[index - 2]['ma30'] * 1.005:
        test_index = df.iloc[:index][df.iloc[:index]['ma20'] > df.iloc[:index]['ma10']].index[-1] - 2
        if df.iloc[test_index]['close'] * 1.4 < close:
            return
    if high > df.iloc[index - 100:index]['high'].max() and close < df.iloc[index - 100:index]['close'].max():
        return
    if low > df.iloc[index - 60:index]['high'].max() * 0.99 and (close != high or rate < 9.8) \
            and (df.iloc[index - 15:index]['close'] > df.iloc[index - 15:index]['ma20']).all():
        return
    if ((close - open) / close < 0.02 and close != high) or open == high:
        return
    if high / close < 1.01 and close != high and 10.5 > rate > 9 and close > df.iloc[index - 100:index]['close'].max() \
            and (df.iloc[index - 2:index]['rate'] > 0).all() and (df.iloc[index - 2:index]['close'] > df.iloc[index - 2:index]['open']).all():
        return
    if rate > 14 and df.iloc[index]['amount'] < df.iloc[index - 30:index][
        'amount'].max() and high != close and high > df.iloc[index - 30:index]['high'].max() \
            and df.iloc[index - 30:index]['rate'].max() < rate - 0.5:
        return
    if (df.iloc[index - 60:index]['close'] > df.iloc[index - 60:index]['ma30']).all() \
            and (df.iloc[index - 60:index]['ma30'] * 1.15 > df.iloc[index - 60:index]['close']).all():
        return
    if (df.iloc[index - 100:index]['close'] > df.iloc[index - 100:index]['ma50']).all() \
            and (df.iloc[index - 100:index + 1]['ma50'] * 1.25 > df.iloc[index - 100:index + 1]['close']).all():
        return

    if (df.iloc[index - 20:index]['close'] < df.iloc[index - 20:index]['ma120']).any():
        if df.iloc[index]['ma120'] < df.iloc[index]['ma250'] * 1.25 and close < df.iloc[index]['ma250'] * 1.45:
            return
    # v型
    if (df.iloc[index - 21:index]['close'] < df.iloc[index - 21:index]['ma120']).any() and close > ma120 * 1.2:
        if df.iloc[index - 15:index]['high'].max() * 1.04 < close > close and \
                close * 1.05 > df.iloc[index - 60:index - 15]['close'].max() \
                and df.iloc[index - 60:index - 15]['high'].max() * 1.05 > close:
            return
    test_index = df.iloc[index - 11:index - 2][(df.iloc[index - 11:index - 2]['high'] > close)]
    if len(test_index) > 0:
        test_index = test_index.index[-1]
        if df.iloc[test_index]['high'] > df.iloc[test_index]['low'] * 1.08 and df.iloc[test_index][
            'rate'] < -4 and df.iloc[test_index]['low'] < df.iloc[test_index]['ma5']:
            return
    ma120_list = df.iloc[index - 10:index - 1]['ma120']
    ma120_check = (ma120_list / ma120_list.shift(1) - 1)[1:] < 0
    ma200_list = df.iloc[index - 10:index - 1]['ma200']
    ma200_check = (ma200_list / ma200_list.shift(2) - 1)[1:] < 0
    ma250_list = df.iloc[index - 10:index - 1]['ma250']
    ma250_check = (ma250_list / ma250_list.shift(1) - 0.999)[1:] < 0
    if ma250_check.any():
        if close < df.iloc[index]['ma250'] * 1.4 or close < df.iloc[index]['ma120'] * 1.4:
            return

    ma120_list = df.loc[index - 10:index, 'ma120']
    ma120_results = (ma120_list / ma120_list.shift(2) - 1)[1:] * 100
    ma120_result = (ma120_results < 0).sum()
    if ma120_result > 4:
        return
    subset = df.iloc[index - 120:index - 1]
    filtered_rows = subset[(subset['high'] > subset['close'] * 1.17)]
    if len(filtered_rows) > 0:
        last_index = filtered_rows.index[-1]
        if df.iloc[last_index]['high'] > close and df.iloc[last_index]['open'] > df.iloc[last_index]['close'] \
                and df.iloc[last_index]['high'] == df.iloc[last_index:index]['high'].max():
            return
    if (abs(df.iloc[index - 20:index]['rate']) > 2.5).sum() < 3 and (abs(df.iloc[index - 5:index]['rate']) > 2.5).sum() ==0:
        return
    rate_list = df.iloc[index - 41:index - 1]
    rate_condition_4 = (rate_list['rate'] > 5).sum()
    rate_condition_5 = (rate_list['open'] / rate_list['close'] > 1.045).sum()
    rate_condition_6 = (rate_list['rate'] > 3.5).sum()
    rate_condition_8 = (rate_list['rate'] > 9).sum()
    rate_condition_9 = (rate_list[-20:]['open'] / rate_list[-20:]['close'] > 1.06).sum()
    rate_condition_10 = (rate_list[-20:]['rate'] < -6).sum()
    rate_condition_11 = (rate_list[-20:]['rate'] > 3).sum()
    rate_condition_12 = (rate_list[-30:]['open'] / rate_list[-30:]['close'] > 1.03).sum()

    if rate_condition_4 < 3 and (
            rate_condition_5 > 1 or rate_condition_9 > 0 or rate_condition_10 > 0) and rate_condition_6 < 5 \
            and (rate_condition_8 < 1) and close < df.iloc[index - 61:index - 1]['high'].max():
        return

    if rate_condition_11 == 0 and rate_condition_12 > 0:  # 太少了
        return

    open_list = df.iloc[index - 26:index - 5]['open']
    close_list = df.iloc[index - 26:index - 5]['close']
    check_1 = (open_list / close_list.shift(-1))[:-1] > 1.1
    if check_1.any() and close < df.iloc[index - 21:index]['close'].max() and high != close:
        return

    score = 0
    if ma200_check.any() or ma120_check.any():
        score += 1

    condition = (df['low'].iloc[index - 30:index] < df['ma200'].iloc[index - 30:index] * 1.05).sum()
    if condition > 0:
        if not (close > df.iloc[index - 200:index - 40]['close'].max() * 1.2
                and (df.iloc[index - 20:index]['close'] < df.iloc[index - 20:index]['ma20']).sum() == 0
                and (df.iloc[index - 20:index]['ma5'] < df.iloc[index - 20:index]['ma10'] * 0.99).sum() == 0
                and (df.iloc[index - 30:index]['rate'] > 5).sum() > 2):
            if not (df.iloc[index]['vol'] > df.iloc[index - 15:index]['vol'].max()
                    and df.iloc[index - 1]['close'] > df.iloc[index - 1]['ma50'] * 1.08
                    and (df.iloc[index - 60:index]['close'] > df.iloc[index - 60:index]['ma200'] * 0.91).all()
                    and df.iloc[index]['vol'] > df.iloc[index - 5:index]['vol'].max() * 1.2):
                score += 1
    test_index = df.iloc[index - 20:index]['high'].idxmax()
    if df.iloc[test_index]['high'] / max(df.iloc[test_index]['open'], df.iloc[test_index]['close']) > 1.04 \
            and df.iloc[test_index]['high'] == df.iloc[index - 80:index]['high'].max():
        if min(df.iloc[test_index]['open'], df.iloc[test_index]['close']) * 1.03 > \
                max(df.iloc[index - 30:index]['open'].max(), df.iloc[index - 30:index]['close'].max()) and \
                close > max(df.iloc[test_index]['open'], df.iloc[test_index]['close']):
            return
    if df.iloc[index - 1]['rate'] > 6:
        if vol < df.iloc[index - 1]['vol'] or (
                df.iloc[index - 1]['rate'] > 6 and df.iloc[index - 1]['high'] != df.iloc[index - 1]['close']
                and close != high and vol > df.iloc[index - 12:index]['vol'].min() * 8 and rate < 10) or \
                (df.iloc[index - 1]['vol'] < df.iloc[index - 20:index - 1]['vol'].mean() * 1.3 and
                 df.iloc[index - 1]['high'] != df.iloc[index - 1]['close']):
            return

    test_high_index = df.iloc[index - 60:index]['high'].idxmax()
    test_low_index = df.iloc[test_high_index:index]['low'].idxmin()
    test_high = df.iloc[test_high_index]['high']
    test_low = df.iloc[test_low_index]['low']

    if test_high > test_low * 1.6 and (test_high - test_low) * 0.65 > high - test_low > (
            test_high - test_low) * 0.35:
        return
    if df.iloc[index - 1]['high'] > df.iloc[index - 1]['close'] * 1.13 and df.iloc[index - 1]['high'] > \
            df.iloc[index - 1]['open'] * 1.03:
        return
    test_index = df.iloc[index-30:index]['close'].idxmax()
    if df.iloc[test_index-1]['rate']>5 and df.iloc[test_index+1]['rate']<-6 \
            and df.iloc[test_index]['high']*1.06>close and (df.iloc[test_index:index]['rate']).max()<6 \
            and df.iloc[test_index+1]['open']/df.iloc[test_index+1]['close']>1.04 \
            and df.iloc[test_index+1]['high']<max(df.iloc[test_index-1]['high'],df.iloc[test_index]['high']):
        return
    if df.iloc[index]['ma10']<df.iloc[index]['ma20']*1.03 and df.iloc[index]['ma10']<df.iloc[index]['ma30']*1.04 \
        and df.iloc[index]['ma10']<df.iloc[index]['ma50']*1.05 and close!=high and rate<13\
             and (close>df.iloc[index]['ma120']*1.4 or close>df.iloc[index]['ma250']*1.6 or close>df.iloc[index]['ma200']*1.5) :
        return

    ma20_mean = df.iloc[index - 45:index - 5]['ma20'].mean()
    if df.iloc[index - 45:index - 5]['ma20'].max() * 0.96 < ma20_mean < df.iloc[index - 45:index - 5][
        'ma20'].min() * 1.04:
        ma20_list = df.iloc[index - 50:index - 10]['ma20']
        ma20_check = (ma20_list / ma20_list.shift(1) - 1)[1:]
        if close > df.iloc[index - 160:index - 5]['high'].max() * 0.98 and (
                (ma20_check < -0.006).sum() + (ma20_check > 0.006).sum()) < 8:
            return
    if df.iloc[index - 45:index - 5]['ma20'].max() * 0.95 < ma20_mean < df.iloc[index - 45:index - 5][
        'ma20'].min() * 1.05:
        ma20_list = df.iloc[index - 50:index - 10]['ma20']
        ma20_check = (ma20_list / ma20_list.shift(1) - 1)[1:]
        if close > df.iloc[index - 160:index - 5]['high'].max() * 0.98 \
                and ((ma20_check < -0.006).sum() + (ma20_check > 0.006).sum()) < 8 and vol < \
                df.iloc[index - 30:index - 10]['vol'].max() * 1.4:
            return
    ma10_list = df.loc[index - 20:index, 'ma10']
    result = (ma10_list / ma10_list.shift(1) - 1)[1:] * 100
    if (ma10/df.iloc[index-12]['ma10']-1)*1.8<(df.iloc[index-12]['ma10']/df.iloc[index-24]['ma10']-1)\
                and (result > -0).all():
        return
    if (df.iloc[index-10:index-1]['high'].max()<df.iloc[index-10:index-1]['low'].min()*1.23 and
        df.iloc[index-10:index-1]['close'].max()<df.iloc[index-10:index-1]['close'].min()*1.13) \
            and (df.iloc[index-21:index-10]['close'].max()>df.iloc[index-21:index-10]['close'].min()*1.3
            or df.iloc[index-26:index-10]['close'].max()>df.iloc[index-26:index-10]['close'].min()*1.4):
        return
    close_list = df.loc[index - 60:index, 'close']
    result = (close_list / close_list.shift(10) - 1)[10:] * 100
    if ((result>25).any() and (abs(result.iloc[-15:-2])<10).all() and close>df.iloc[index-100:index]['high'].max()) :
        if (close<ma250*1.4 or close<ma200*1.4 or close<ma50*1.1 or close>ma250*2):
            return
        if vol*1.1<df.iloc[index-40:index]['vol'].max() :
            return
    if (df.iloc[index-120:index]['rate']>7).sum()<1 and (df.iloc[index-100:index]['rate']>6).sum()<2 \
            and (df.iloc[index-80:index]['rate']<-4).sum()<3:
        return
    if df.iloc[index-2:index+1]['vol'].max()<df.iloc[index-40:index]['vol'].max()*0.35 \
            or df.iloc[index-2:index+1]['vol'].max()<df.iloc[index-60:index]['vol'].max()*0.25 \
            or df.iloc[index - 5:index + 1]['vol'].max() < df.iloc[index - 25:index-8]['vol'].max()* 0.55:
        return
    ma50_mean = df.iloc[index - 20:index - 1]['ma50'].mean()
    if df.iloc[index - 20:index - 1]['ma20'].max() * 0.95 < ma50_mean < df.iloc[index - 20:index - 1][
        'ma20'].min() * 1.05 :
        if df.iloc[index-8:index]['open'].min()*1.2<close:
            pass
        elif high>df.iloc[index-100:index]['high'].max() and (vol < df.iloc[index-15:index]['vol'].max()) and close!=high:
            return
        elif high<df.iloc[index-100:index]['high'].max() and \
                (close<df.iloc[index-20:index]['high'].max() or df.iloc[index]['ma50']<=df.iloc[index-5:index]['ma50'].max()*1.002):
            return
    if  high*1.2<df.iloc[index-120:index]['high'].max() and (close>ma50*1.05 or close!=high)  :
        return
    if (df.iloc[index - 20:index]['rate'] > rate * 1.1).sum() == 1 and (df.iloc[index - 20:index]['rate'] > rate).sum() == 1 \
            and vol < df.iloc[index - 5:index]['vol'].mean() * 1.5 and df.iloc[index - 1]['close'] > \
            df.iloc[index - 1]['ma50'] * 1.05:
        return
    if close<df.iloc[index-1]['high'] and (rate<9.5 or high!=close):
        return

    if (df.iloc[index-12:index]['rate']>rate).sum()>1 and close!=high and close>df.iloc[index-13:index]['close'].min()*1.15 \
            and close > df.iloc[index - 5:index]['close'].min() * 1.07 and (df.iloc[index-12:index-7]['rate']>rate).sum()>0\
            and (df.iloc[index-12:index]['rate']<2).sum()>6 and (df.iloc[index-12:index]['rate']<0).sum()>4 \
            and (df.iloc[index-7:index]['rate']>rate).sum()>0:
        return
    if ma30 > ma50 *1.15  and ma30>ma250*2:
        return
    max_test = max(df.iloc[index - 1]['ma5'],df.iloc[index - 1]['ma10'],df.iloc[index - 1]['ma20'],df.iloc[index - 1]['ma30'])
    min_test = min(df.iloc[index - 1]['ma5'],df.iloc[index - 1]['ma10'],df.iloc[index - 1]['ma20'],df.iloc[index - 1]['ma30'])
    if df.iloc[index-6]['ma30']>df.iloc[index-5]['ma30'] and df.iloc[index-6]['ma20']>df.iloc[index-5]['ma20']:
        if max_test<min_test*1.015:
            pass
        elif max(ma5,ma10,ma20,ma30,ma50) == ma20 or max(ma5,ma10,ma20,ma30,ma50) == ma30:
            pass
        else:
            return
    if (df.iloc[index-20:index]['close']<df.iloc[index-20:index]['ma20']).sum()>11:
        if df.iloc[index-1]['close']>df.iloc[index-4:index]['close'].min()*1.06 and df.iloc[index-2]['close']>df.iloc[index-2]['ma20']:
            return
    if df.iloc[index]['ma30']>df.iloc[index]['ma20']*1.0>df.iloc[index]['ma50']*1.0 and df.iloc[index]['ma30']>df.iloc[index]['ma50']*1.03:
        test_index = df.iloc[index-50:index]['high'].idxmax()
        test_high = df.iloc[index-50:index]['high'].max()
        test_low = df.iloc[test_index:index]['low'].min()
        if high*0.98 <test_high<high*1.05 and test_high>test_low*1.2:
            return
    high_date = df.loc[high_index, 'date']
    try:
        week = pd.read_csv(content['week'] + code + '.csv')
    except:return
    if week_tight1(week, code, date, high_date, rate):
        try:
            if len(df[:index]) < 250:
                return

            simple = week.iloc[0:10, 0:7].copy()
            try:week_index = week[week["date"] >= date].index[0]
            except:week_index=week.index[-1]+1
            if week[week_index - 16:week_index]['rate'].abs().max() < 8:
                return
            deal = simpleTrend(week[:week_index], simple)
            simple = pd.DataFrame(columns=['date', 'key', 'flag', 'temp'])
            week_deal = find_point(deal, simple)
            week_deal['rate'] = week_deal['key'] / week_deal['key'].shift(1) - 1
            week_deal = merge(week_deal, week)
            # # 要改 参数2要改
            boolean, multiple, start_date = price_change(week[:week_index], week_deal)

            if not boolean:
                if start_date == 1:
                    if close < ma50 * 1.2 or close < df.iloc[index - 200:index - 10][
                        'close'].max() * 1.08 or close < df.iloc[index - 200:index - 8]['high'].max() * 1.05:
                        return
                if start_date == 2:
                    condition = (week.iloc[week_index - 20:week_index]['ma20'] <
                                 week.iloc[week_index - 20:week_index]['ma30'] * 1.02).sum()
                    condition_2 = (week.iloc[week_index - 20:week_index]['ma30'] <
                                   week.iloc[week_index - 20:week_index]['ma50'] * 1.02).sum()
                    if condition != 0 or condition_2 != 0:
                        return
            simple = df.iloc[index - 250:index - 240, 0:7].copy()
            deal = simpleTrend(df[index - 240:index + 1], simple)
            simple = pd.DataFrame(columns=['date', 'key', 'flag', 'temp'])
            day_deal = find_point(deal, simple)
            day_deal['rate'] = day_deal['key'] / day_deal['key'].shift(1) - 1

            if score > 0:
                index_list = day_deal[day_deal['rate'] > 0.2].index
                if len(index_list) > 0:
                    if df.iloc[index - 1]['close'] < df.iloc[index - 1]['ma120'] * 1.15 or df.iloc[index - 1][
                        'close'] < df.iloc[index - 1]['ma200'] * 1.15 \
                            or df.iloc[index - 1]['close'] < df.iloc[index - 1]['ma250'] * 1.15:
                        return
                    low_index = df.iloc[index - 60:index]['close'].idxmin()
                    if df.iloc[low_index]['close'] * 1.1 > df.iloc[low_index]['ma120'] or df.iloc[low_index][
                        'close'] * 1.1 > df.iloc[low_index]['ma200']:
                        return
                    if score != 2:
                        if day_deal.iloc[index_list[-1]]['rate'] < 0.5 or df.iloc[index - 1]['close'] < \
                                df.iloc[index - 1]['ma120'] * 1.15:
                            return
                else:
                    return
            day_deal_copy = day_deal.copy()
            day_deal_merge = day_merge(day_deal_copy, df[index - 250:index + 1])

            test_index = day_deal[day_deal['flag'] == 'max'].index[-1]
            if day_deal.iloc[test_index]['rate'] > 0.3 and day_deal.iloc[-1]['flag'] == 'min':
                test_date_max = str(day_deal.iloc[test_index]['date'])[:10]
                test_date_strart = str(day_deal.iloc[test_index - 1]['date'])[:10]

                test_date_max_index = df[df["date"] == test_date_max].index[0]
                test_date_strart_index = df[df["date"] == test_date_strart].index[0]
                if test_date_max_index + 4 > index:
                    pass
                elif day_deal.iloc[test_index]['rate'] <= 0.4 and test_date_strart_index + 20 < index:
                    return

                elif test_date_strart_index + 30 < index and day_deal.iloc[test_index]['rate'] > 0.4 \
                        and df.iloc[test_date_max_index - 5:test_date_max_index + 2]['high'].max() == \
                        df.iloc[test_date_max_index - 100:test_date_max_index + 2]['high'].max():

                    return

            test_index = day_deal[day_deal['flag'] == 'min'].index[-1]
            test_index_merge = day_deal_merge[day_deal_merge['flag'] == 'min'].index[-1]
            if day_deal.iloc[test_index]['rate'] < -0.15 and day_deal_merge.iloc[test_index_merge - 1][
                'rate'] > 0.5:
                test_index = df[df["date"] == day_deal_merge.iloc[test_index_merge - 1]['date']].index[0]

                if (df.iloc[test_index:index]['low'] < df.iloc[test_index:index]['ma20']).any() \
                        and not (
                        df.iloc[test_index - 10:test_index]['close'] < df.iloc[test_index - 10:test_index][
                    'ma20']).any() and \
                        df.iloc[index - 100:index]['high'].max() * 1.06 > high > df.iloc[index - 100:index][
                    'high'].max() * 1.02:
                    return

            # # 要改 参数2要改
            boolean, multiple, start_date = day_price_change(df[index - 250:index + 1].reset_index(drop=True),
                                                             day_deal_merge[1:].reset_index(drop=True))

            if not boolean and close < df[index - 250:index]['close'].max():
                return

            boolean = True
            if start_date != 0:
                start_index = df[df['date'] <= start_date][-10:]['close'].idxmin()
                if start_index + 10 > index:
                    pass
                else:
                    start_high_index = df.loc[start_index:index - 10]['close'].idxmax()
                    deal_index = deal[deal["date"] >= start_date].index[0]
                    if index - start_high_index > 90:
                        flag = 0
                        for i in range(len(deal) - 3, deal_index, -1):
                            if deal.iloc[i]['high'] > df.iloc[start_high_index - 3:start_high_index + 4][
                                'high'].max() * 0.98 and deal.iloc[i]['high'] == deal.iloc[i - 4:i + 3][
                                'high'].max():
                                test_index = df[df["date"] == str(deal.iloc[i]['date'])[:-9]].index[0]
                                if index - test_index < 80:
                                    flag = 1
                        if flag == 0:
                            return

                test_start = df[start_index:index][df[start_index:index]['rate'] > 6]

                if len(test_start) > 0:
                    test_start_index = test_start.index[0]
                    test_rate = (df.iloc[index - 1]['close'] / df.iloc[test_start_index - 1]['close'] - 1) * 100
                    rate_max_sum = df[start_index - 1:index - 1]['rate'].nlargest(2).sum()
                    last_test_rate = (df.iloc[test_start_index - 1]['close'] /
                                      df.iloc[test_start_index - 30:test_start_index - 1]['close'].min() - 1) * 100
                    if test_rate > 15 and rate_max_sum > test_rate * 0.75 and 150 > index - test_start_index > 15 and last_test_rate < 25:
                        if df.iloc[index - 40]['ma250'] < df.iloc[index - 30]['ma250']:
                            return

                boolean = multiple_top(df.loc[start_index - 15:index])
                if not boolean:
                    if vol < df.iloc[index - 11:index]['vol'].max() * 0.95:
                        if high==close and rate>9.5:
                            msg+=' 注意量'
                            pass
                        else:
                            return
                    test_index = df.iloc[start_index:index - 10]['close'].idxmax()
                    test_low_index = df.iloc[test_index:index - 1]['close'].idxmin()
                    test_close = df.iloc[test_low_index]['close']
                    if (df.iloc[test_index]['close'] * 0.75 > test_close and close * 0.8 > test_close) or \
                            (df.iloc[test_index][
                                 'close'] * 0.80 > test_close and close * 0.80 > test_close and index - test_low_index < 20):
                        pass
                    else:
                        return

            rate_list = df.iloc[index - 31:index]['rate']
            rate_condition_1 = rate_list[-13:] < -9
            rate_condition_2 = rate_list[-8:] < -5  # good
            rate_condition_3 = rate_list[-5:] > 9
            rate_condition_7 = (rate_list[-25:] > 9.5).sum()
            rate_list = df.iloc[index - 31:index]['close'] / df.iloc[index - 31:index]['open']
            rate_condition_4 = rate_list[-3:-1] < 0.93
            rate_condition_5 = rate_list[-13:] < 0.92
            if rate_condition_5.any():
                if (rate_list[-13:] < 0.91).any() and not rate_condition_1.any():
                    pass
                elif not rate_condition_1.any():
                    return
            if rate_condition_4.any():
                test_index = rate_list[-3:-1][rate_list[-3:-1] < 0.93].index.tolist()[-1]
                if df.iloc[test_index]['open'] * 0.97 < close < df.iloc[test_index]['open'] * 1.03 and close * 1.04 > \
                        df.iloc[index - 30:index]['high'].max():
                    pass
                else:
                    return
            score = 0
            if rate_condition_7 > 2:
                score += 1
                low_index = df.iloc[index - 25:index]['close'].idxmin()
                test_rate = df.iloc[index - 10:index]['rate'].abs().mean()
                test_min_rate = df.iloc[index - 10:index]['rate'].abs().nlargest(5).min()
                test_min_rate_2 = df.iloc[index - 10:index]['rate'].nlargest(4).min()
                if df.iloc[low_index]['close'] > df.iloc[low_index]['ma120'] and df.iloc[low_index]['close'] > \
                        df.iloc[low_index]['ma200']:
                    if (df.iloc[index - 25:index]['open'] > df.iloc[index - 25:index][
                                'close'] * 1.08).sum() == 0 and df.iloc[index - 1]['rate'].max() < 6:
                        msg+=" hard chance"
                    elif test_rate > 5 and test_min_rate > 5 and test_min_rate_2 > 4:
                        pass
                    else:
                        return
            min_20 = df.iloc[index - 20:index - 1]['close'].min()
            last_high = df.iloc[index - 80:index - 22]['close'].max()
            if min_20 / df.iloc[index - 1]['close'] < 0.8 and min_20 * 1.1 < last_high and df.iloc[index - 1][
                'close'] > last_high and score == 0:
                if df.iloc[index - 1]['ma250'] * 1.5 > df.iloc[index - 1]['close']:
                    return
                if ((df.iloc[index - 30:index]['close'] * 1.05 < df.iloc[index - 30:index]['open']).sum() > 2 or
                    (df.iloc[index - 30:index]['close'] * 1.03 < df.iloc[index - 30:index]['open']).sum() > 5 or
                    (df.iloc[index - 30:index]['rate'] < -7).sum() > 0) and (
                        df.iloc[index - 30:index]['rate'] > 6).sum() < 5:
                    return

            if rate_condition_2.any() and rate_condition_3.any():
                return
            if rate_condition_2.any():
                flag = 0
                if df.iloc[index - 5:index]['rate'].min() < -7:
                    if df.iloc[index - 1]['rate'] < -7 and (
                            df.iloc[index - 2]['open'] * 1.06 < df.iloc[index - 2]['close'] or
                            df.iloc[index - 2]['rate'] > 6):
                        flag += 1
                        return
                    if df.iloc[index - 2:index]['rate'].min() < -5 and not df.iloc[index - 1]['rate'] < -7 and close*1.03 >df.iloc[index-30:index]['high'].max():
                        flag += 1
                        return
                    if flag == 0 and not (low < ma10 and low < ma5):
                        return

            if rate_condition_3.any():
                if close < df.iloc[index - 5:index]['high'].max():
                    pass
                elif not (df.iloc[index - 5:index]['close'].max() > df.iloc[index - 80:index - 10][
                    'high'].max() * 1.05 and vol > df.iloc[index - 3:index]['vol'].max()):
                    test_index = df.iloc[index - 5:index]['rate'].idxmax()
                    if not ((df.iloc[test_index:index]['rate'] > 0).all() and open > df.iloc[test_index][
                        'close'] and df.iloc[index]['ma250'] * 1.6 < close and df.iloc[index][
                                'ma250'] * 1.2 < ma120 and rate + 1 > df.iloc[index - 1]['rate'] and
                            df.iloc[index - 2]['ma50'] >=
                            df.iloc[index - 3]['ma50']):
                        return

                low_250 = df.iloc[index - 250:index - 1]['close'].min()
                if low_250 > df.iloc[index]['close'] * 0.58 and low_250 > df.iloc[index - 1]['close'] * 0.64:
                    return

                ma5_list = df.iloc[index - 4:index]['ma5']
                ma5_check = (ma5_list / ma5_list.shift(1) - 1)[1:] < 0
                if df.iloc[index - 5:index]['close'].max() > close and close != high and ma5_check.any():
                    test_index = df.iloc[index - 9:index - 1]['rate'].idxmax()
                    if close * 0.98 < df.iloc[test_index:test_index + 2]['close'].max() \
                            and df.iloc[test_index:test_index + 2]['high'].max() > df.iloc[index - 20:index][
                        'high'].max() * 0.98 \
                            and df.iloc[test_index]['rate'] > 7:
                        pass
                    else:
                        return

        except:
            print(code + ' ' + date + '-----------------------------')
            return

        df_zs = pd.read_csv(content['composite'] + '399006' + '.csv')
        zs_index = df_zs[df_zs["date"] == date].index[0]
        num = 0
        if (close / df.iloc[index - 15]['close'] < df_zs.iloc[zs_index]['close'] / df_zs.iloc[zs_index - 15][
            'close']):
            num += 1
        if (close / df.iloc[index - 30]['close'] < df_zs.iloc[zs_index]['close'] / df_zs.iloc[zs_index - 30][
            'close']):
            num += 1
        if (close / df.iloc[index - 45]['close'] < df_zs.iloc[zs_index]['close'] / df_zs.iloc[zs_index - 45][
            'close']):
            num += 1
        if (close / df.iloc[index - 60]['close'] < df_zs.iloc[zs_index]['close'] / df_zs.iloc[zs_index - 60][
            'close']):
            num += 1
        if num > 3:
            return
        # print(code + ' ' + date)

        if vol < max(df.iloc[index - 21:index]['vol'].mean(), df.iloc[index - 11:index]['vol'].mean()) * 1.2 \
                and close == high and close < ma50 * 1.4:
            msg+='open+1<close*1.015 : return'
        try:
                evaluate, high_index = evaluates1(df, index)
                remarks, remarks_info = last_check(df, index)
                remarks_1, remarks_2, t_5, t_5_max = last_check_1(df, index)
                t3 = -999
                if high_index != '':
                    t3 = round((df.iloc[index + 1:high_index]['low'].min() / df.iloc[index + 1][
                        'open'] - 1) * 100, 2)
                print(code + ' ' + date + ' ' + str(
                    evaluate) + ' ' + remarks + ' ' + remarks_info + ' ' + remarks_1 + ' ' + remarks_2 + ' ' + t_5 + ' ' + t_5_max+' '+msg)
                length = evaluate_result.shape[0]
                evaluate_result.loc[length] = {"code": code, "date": date, "result": evaluate,
                                               "remarks": remarks, "remarks_info": remarks_info,
                                               "remarks_1": remarks_1, "t+3": remarks_2,
                                               "t+5": t_5,
                                               "t+5_max": t_5_max, "t3": t3}
        except:print(code + ' ' + date+' '+msg)

def evaluates(df, index):
    price = df.iloc[index + 1]['open']

    max = 0
    min = 10000

    for i, row in df.iloc[index + 2:index + 40].iterrows():
        end_price = row['close']

        if end_price / price > 1.45:
            return 1, i
        elif end_price / price < 0.94:
            high = df.iloc[index + 2:i]['close'].max()
            high_index = df.iloc[index + 1:i]['close'].idxmax()
            if high / price > 1.35:
                return 2, high_index
            if high / price > 1.25:
                return 3, high_index
            if high / price > 1.15:
                return 4, high_index
            if high / price > 1.1:
                return 5, high_index
            if high / price > 1.05:
                return 6, high_index
            max_index = i - index
            for j, roww in df.iloc[i:index + 40].iterrows():
                endd_price = roww['close']
                if max < endd_price:
                    max = endd_price
                    max_index = j - index
                if min >= endd_price:
                    min = endd_price
                    min_index = j - index
                    if endd_price / price < 0.89:
                        if max / price > 1.45:
                            return 12, ''
                        if max / price > 1.35:
                            return 22, ''
                        if max / price > 1.25:
                            return 32, ''
                        if max / price > 1.15:
                            return 42, ''
                        if max / price > 1.1:
                            return 52, ''
                        if max / price > 1.05:
                            return 62, ''
                        for k, rowww in df.iloc[j:index + 40].iterrows():
                            enddd_price = rowww['close']
                            if max < enddd_price:
                                max = enddd_price
                                max_index = k - index
                            if min >= enddd_price:
                                min = enddd_price
                                min_index = k - index
                                if enddd_price / price < 0.82:
                                    if max / price > 1.45:
                                        return 13, ''
                                    if max / price > 1.35:
                                        return 23, ''
                                    if max / price > 1.25:
                                        return 33, ''
                                    if max / price > 1.15:
                                        return 43, ''
                                    if max / price > 1.1:
                                        return 53, ''
                                    if max / price > 1.05:
                                        return 63, ''
                                    else:
                                        return 10, ''
                        if max / price > 1.45:
                            return 13, ''
                        if max / price > 1.35:
                            return 23, ''
                        if max / price > 1.25:
                            return 33, ''
                        if max / price > 1.15:
                            return 43, ''
                        if max / price > 1.1:
                            return 53, ''
                        if max / price > 1.05:
                            return 63, ''
                        else:
                            return 9, ''
            if max / price > 1.45:
                return 12, ''
            if max / price > 1.35:
                return 22, ''
            if max / price > 1.25:
                return 32, ''
            if max / price > 1.15:
                return 42, ''
            if max / price > 1.1:
                return 52, ''
            if max / price > 1.05:
                return 62, ''
            return 8, ''

    max_close = df.iloc[index:index + 40]['close'].max()
    high_index = df.iloc[index + 1:index + 40]['close'].idxmax()

    if max_close / price > 1.35:
        return 2, high_index
    if max_close / price > 1.25:
        return 3, high_index
    if max_close / price > 1.15:
        return 4, high_index
    if max_close / price > 1.1:
        return 5, high_index
    if max_close / price > 1.05:
        return 6, high_index
    else:
        return 7, high_index

def evaluates1(df, index):
    price = df.iloc[index + 1]['open']
    high_index = df.iloc[index + 2:index + 40]['close'].idxmax()
    high_price = df.iloc[high_index]['close']
    if high_price / price > 1.45:
        low_index = df.iloc[index + 1:high_index]['close'].idxmin()
        low_price = df.iloc[low_index]['close']
        if low_price/price<0.82:
            return 41,''
        if low_price/price<0.89:
            return 31,''
        if low_price/price<0.94:
            return 21,''
        return 1, ''
    if high_price / price > 1.35:
        low_index = df.iloc[index + 1:high_index]['close'].idxmin()
        low_price = df.iloc[low_index]['close']
        if low_price/price<0.82:
            return 42,''
        if low_price/price<0.89:
            return 32,''
        if low_price/price<0.94:
            return 22,''
        return 2, ''
    if high_price / price > 1.25:
        low_index = df.iloc[index + 1:high_index]['close'].idxmin()
        low_price = df.iloc[low_index]['close']
        if low_price/price<0.82:
            return 43,''
        if low_price/price<0.89:
            return 33,''
        if low_price/price<0.94:
            return 23,''
        return 3, ''
    if high_price / price > 1.15:
        low_index = df.iloc[index + 1:high_index]['close'].idxmin()
        low_price = df.iloc[low_index]['close']
        if low_price/price<0.82:
            return 44,''
        if low_price/price<0.89:
            return 34,''
        if low_price / price < 0.94:
                return 24, ''
        return 4, ''
    if high_price / price > 1.1:
        low_index = df.iloc[index + 1:high_index]['close'].idxmin()
        low_price = df.iloc[low_index]['close']
        if low_price/price<0.82:
            return 45,''
        if low_price/price<0.89:
            return 35,''
        if low_price/price<0.94:
            return 25,''
        return 5, ''
    if high_price / price > 1.05:
        low_index = df.iloc[index + 1:high_index]['close'].idxmin()
        low_price = df.iloc[low_index]['close']
        if low_price/price<0.82:
            return 46,''
        if low_price/price<0.89:
            return 36,''
        if low_price/price<0.94:
            return 26,''
        return 6, ''
    low_index = df.iloc[index + 2:index+40]['close'].idxmin()
    low_price = df.iloc[low_index]['close']
    if low_price/price<0.89:
        if low_price / price < 0.81:
            return 10,''
        return 9,''
    return 8,''

def test(content, date, code,evaluate_result):
    df = pd.read_csv(content['normal'] + code + '.csv')

    if date == '':
        for index, row in df.iloc[255:].iterrows():
            # index = df[df["date"] == date].index[0]
            date = row['date']
            if date == '2021-06-22':
                print(1)
                pass
            index = df[df["date"] == date].index[0]
            close = df.loc[index, 'close']




            market_vaule = now[now['股票代码'] == code]['总市值'].iloc[0]
            if market_vaule == '-':
                continue
            market_vaule = market_vaule / df['close'].iloc[-1] * close
            if market_vaule < 8000000000:
                continue

            # 参数2可以
            condition = (df['low'].iloc[index - 30:index] < df['ma200'].iloc[index - 30:index] * 1.05)
            if condition.any():
                continue

            # # 要改 参数2可以
            condition_3 = (df['open'].iloc[index - 2:index + 1] < df['ma10'].iloc[index - 2:index + 1] * 1.02)
            condition_4 = (df['close'].iloc[index - 2:index] < df['ma10'].iloc[index - 2:index] * 1.02)
            if not (condition_3.any() or condition_4.any()):
                continue

            ma20_list = df.loc[index - 20:index, 'ma20']
            ma20_result = (ma20_list / ma20_list.shift(1) - 1)[1:] * 100
            result = (ma20_result > 0).sum()
            if result < 3:
                low_close_20 = df.loc[index - 20:index]['close'].min()
                if low_close_20 * 1.2 < close:
                    continue

            open = df.loc[index, 'open']
            rate = df.loc[index, 'rate']

            vol = df.loc[index, 'vol']
            last_close = df.loc[index - 1, 'close']
            negative_vol_20 = get_vol(df, 'negative', index - 20, index - 1)
            negative_vol_10 = get_vol(df, 'negative', index - 10, index - 1)

            vol_20 = df.loc[index - 20:index - 1, 'vol'].mean() * 1.5
            vol_30 = df.loc[index - 30:index - 1, 'vol'].mean() * 1.5
            result = df[index - 10:index][df.iloc[index - 10:index]['rate'] < 0]

            high_20 = df.loc[index - 20:index - 1, 'close'].max()
            ma50 = df.loc[index, 'ma50']
            ma20 = df.loc[index, 'ma20']
            high_120 = df.loc[index - 120:index - 1, 'high'].max()
            high_index = df[df["high"] == high_120].index
            high_index = high_index[high_index < index][-1]

            ma120 = df.loc[index, 'ma120']

            max_20 = df.iloc[index - 19:index]['close'].max()
            max_25 = df.iloc[index - 24:index]['close'].max()
            # 影响小 参数2还可以
            if close > ma120 * 1.05 and (close > max_25 or (close > max_20 and rate > 9.8)):
                if vol_20 < vol or vol_30 < vol or rate > 9.8:
                    pass
                if result.empty:
                    pass
                elif vol > result['vol'].max() * 1.4 or rate > 9.8:
                    pass
                else:
                    continue
            else:
                continue

            if ( vol > 100000000
                    and ((close > high_20 and (
                            vol > negative_vol_20 * 1.5 or negative_vol_20 == 0) and high_index + 30 <= index)
                         or (close > ma20 and (
                                    vol > negative_vol_10 * 1.5 or negative_vol_10 == 0) and high_index + 20 <= index))):
                pass

            elif (rate > 9.8 and close > ma20):
                # if (positive_vol_10 < negative_vol_10 * 1.2 or positive_vol_20 < negative_vol_20 * 1.1):
                pass
            else:
                continue
            low_xx = df.loc[high_index:index - 1, 'low'].min()

            last_ma5 = df.loc[index - 1, 'ma5']
            last_ma10 = df.loc[index - 1, 'ma10']
            before_ma5 = df.loc[index - 3, 'ma5']
            before_ma10 = df.loc[index - 3, 'ma10']
            # 要改 参数2要改 暂停使用
            # if low_xx / high_120 > 0.5 and open < high_120:
            #     if close < ma50 * 1.3 or (close < ma50 * (1.3 + (rate - 10) * 0.01)):
            #         pass
            #     if (open<last_ma5 or open<last_ma10) and open < ma20*1.1:
            #         pass
            #     else:
            #         continue

            if True:

                # 参数2要改
                # if ma120 > ma250 or (close > df.loc[index - 9, 'close']): 没用到
                #     pass
                # else:continue
                if True:
                    high_date = df.loc[high_index, 'date']

                    week = pd.read_csv(content['week'] + code + '.csv')

                    if week_tight(week, code, date, high_date, rate):
                        simple = week.iloc[0:10, 0:7].copy()
                        week_index = week[week["date"] >= date].index[0]
                        deal = simpleTrend(week[:week_index], simple)
                        simple = pd.DataFrame(columns=['date', 'key', 'flag', 'temp'])
                        week_deal = find_point(deal, simple)
                        week_deal['rate'] = week_deal['key'] / week_deal['key'].shift(1) - 1
                        week_deal = merge(week_deal, week)
                        # 要改 参数2要改
                        boolean, multiple, start_date = price_change(week[:week_index], week_deal)
                        if not boolean:
                            continue
                        if start_date != 0:
                            start_index = df[df['date'] == start_date].index[0]
                            boolean = multiple_top(df.loc[start_index - 15:index])
                            if not boolean:
                                continue
                    else:
                        continue
                    # 要改 参数2要改 区别不大
                    # if (market_vaule > 100000000000 or get_per(code, date)) or (
                    #         market_vaule > 8000000000 and get_per(code, date)):
                    #     pass
                    # else:
                    #     continue
                    #
                    #
                    #         low_15 = df.loc[index - 20:index]['low'].min()
                    #         low_50 = df.loc[index - 50:index]['low'].min()
                    #         low_100 = df.loc[index - 100:index]['low'].min()
                    #         high = df.iloc[high_index]['close']
                    #         if not ((low_15 > low_50 or high < df.iloc[index]['close'])
                    #                 or (low_15 == low_50 > low_100 and high * 0.9 < df.iloc[index]['close'])
                    #                 or (low_15 == low_50 > low_100 and high * 0.8 < df.iloc[index]['close'] and
                    #                     df.iloc[index]['rate'] > 9.8)):
                    #             if multiple < 2.5:
                    #                 # print(code + ' ' + date + ' check')
                    #
                    #                 continue
                    #
                    #         gap_index = week[week["date"] >= date].index[0]
                    #         string = gap(week.loc[:gap_index], high_120)
                    #         # print(code + ' ' + date + ' ' + string + ' ' + rps50 + rps120 + rps250)

                ma120 = df.loc[index, 'ma120']
                ma250 = df.loc[index, 'ma250']
                ma120_list = df.loc[index - 10:index, 'ma120']
                ma120_results = (ma120_list / ma120_list.shift(2) - 1)[1:] * 100
                ma120_result = (ma120_results < 0).sum()
                if ma120_result > 7 or ma250 > ma120:
                    continue

                year_low = df.iloc[index - 250:index]['close'].min()
                # if close<ma120 or close<ma250 or ma250>ma120 or ma50<ma120 or ma50<ma250 or close < year_low * 1.3:

                rate_list = df.loc[index - 31:index - 1, 'rate']
                rate_condition_1 = rate_list[-20:] < -9
                rate_condition_2 = rate_list[-10:] < -5  # good
                rate_condition_3 = rate_list[-5:] > 9

                rate_high_list = df.loc[high_index - 31:high_index - 1, 'rate']

                # rate_condition_4 = (rate_high_list < -8).sum()
                # rate_condition_5 = (rate_high_list[-20:] < -5).sum()
                # rate_condition_6 = (rate_high_list[-20:] > 9).sum()

                if rate_condition_1.any() or rate_condition_2.any() or rate_condition_3.any():
                    continue

                low_15 = df.loc[index - 15:index]['low'].min()
                low_50 = df.loc[index - 50:index]['low'].min()
                low_100 = df.loc[index - 100:index]['low'].min()
                high = df.iloc[high_index]['close']
                # if not ((low_15 > low_50 or high < df.iloc[index]['close'])
                #         or (low_15 == low_50 > low_100 and high * 0.9 < df.iloc[index]['close'])
                #         or (low_15 == low_50 > low_100 and high * 0.8 < df.iloc[index]['close'] and
                #             df.iloc[index]['rate'] > 9.8)):
                #     if multiple < 2.5:

                evaluate = evaluates(df, index)
                print(code + ' ' + date + ' ' + str(evaluate))
                new = pd.DataFrame({"code": code, "date": date, "result": evaluate,"remarks":""}, index=[1])

                evaluate_result = evaluate_result.append(new, ignore_index=True)

    else:
        # if date == '2019-10-24':
        #     print(1)
        index = df[df["date"] == date].index[0]
        close = df.loc[index, 'close']
        evaluate = evaluates(df, index)
        remarks = last_check(df, index)
        remarks_1,remarks_2 = last_check_1(df, index)

        # print(code + ' ' + date + ' ' + str(evaluate)+' '+remarks+' ' +remarks_1+' ' +remarks_2)
        # length = evaluate_result.shape[0]
        # evaluate_result.loc[length] = {"code": code, "date": date, "result": evaluate,"remarks":remarks,"remarks_1":remarks_1,"remarks_2":remarks_2}
        # return

        # high = df.iloc[index+1]['high']
        # open = df.loc[index+1, 'open']
        # close = df.loc[index+1, 'close']
        # if high/close>1.03 :
        #     print(code + ' ' + date + ' ' + str(evaluate)+' '+remarks+' ' +remarks_1+' ' +remarks_2)
        #     length = evaluate_result.shape[0]
        #     evaluate_result.loc[length] = {"code": code, "date": date, "result": evaluate,"remarks":remarks,"remarks_1":remarks_1,"remarks_2":remarks_2}
        #     return
        #
        #
        # return
        ma20_list = df.loc[index - 8:index-1, 'ma20']
        ma20_result = (ma20_list / ma20_list.shift(2) - 1)[1:] * 100
        ma30_list = df.loc[index - 8:index-1, 'ma30']
        ma30_result = (ma30_list / ma30_list.shift(2) - 1)[1:] * 100
        down_5 = df.iloc[index - 8:index+1]['ma5'] - df.iloc[index - 8:index+1]['ma20']
        if (not (ma20_result>0).any() or not (ma30_result>0).any()) and not (down_5>0).any() :
            return
        rate = df.loc[index, 'rate']
        open = df.loc[index, 'open']
        high = df.loc[index, 'high']
        close = df.loc[index, 'close']
        low = df.loc[index, 'low']
        data_max = open if open>close else close
        data_min = close  if open>close else open
        max_20 = df.iloc[index - 21:index-17]['close'].max()

        last_close = df.loc[index - 1, 'close']

        min_5 = df.iloc[index - 5:index - 1]['close'].min()
        if 1.03<last_close/min_5<=1.1 and close<df.iloc[index-100:index]['close'].max(): #测试中有效果  1.03<last_close/min_5<=1.1 and close<df.iloc[index-100:index]['close'].max()
            # print(code + ' ' + date + ' ' + str(evaluate) + ' ' + remarks + ' ' + remarks_1 + ' ' + remarks_2)
            # length = evaluate_result.shape[0]
            # evaluate_result.loc[length] = {"code": code, "date": date, "result": evaluate, "remarks": remarks,
            #                                "remarks_1": remarks_1, "remarks_2": remarks_2}

            return

        if high/last_close>1.096 and rate<8:
            return

        if max_20/close>1.05: #
            return

        if ((high/data_max + data_min/low -2)>0.05 and rate <10.1) or ((high/data_max) + (data_min/low) -2)>0.08:
            return


        market_vaule = now[now['股票代码'] == code]['总市值'].iloc[0]
        if market_vaule == '-':
            return
        market_vaule = market_vaule / df['close'].iloc[-1] * close
        if market_vaule < 8000000000:
            return


        # 参数2可以
        condition = (df['low'].iloc[index - 30:index] < df['ma200'].iloc[index - 30:index] * 1.05)
        if condition.any():
            return

        # # 要改 参数2可以
        condition_3 = (df['open'].iloc[index - 2:index + 1] < df['ma10'].iloc[index - 2:index + 1] * 1.02)
        condition_4 = (df['close'].iloc[index - 2:index] < df['ma10'].iloc[index - 2:index] * 1.02)
        if not (condition_3.any() or condition_4.any()):
            return

        ma20_list = df.loc[index - 20:index, 'ma20']
        ma20_result = (ma20_list / ma20_list.shift(1) - 1)[1:] * 100
        result = (ma20_result > 0).sum()
        if result < 3:
            low_close_20 = df.loc[index - 20:index]['close'].min()
            if low_close_20 * 1.2 < close:
                return



        vol = df.loc[index, 'vol']
        vol_last_5 = df[index - 5:index][df[index - 5:index]['rate']<0 ]['vol'].max()
        negative_vol_20 = get_vol(df, 'negative', index - 20, index - 1)
        negative_vol_10 = get_vol(df, 'negative', index - 10, index - 1)

        vol_20 = df.loc[index - 20:index - 1, 'vol'].mean() * 1.5
        vol_30 = df.loc[index - 30:index - 1, 'vol'].mean() * 1.5
        result = df[index - 10:index][df.iloc[index - 10:index]['rate'] < 0]

        high_20 = df.loc[index - 20:index - 1, 'close'].max()
        ma50 = df.loc[index, 'ma50']
        ma20 = df.loc[index, 'ma20']
        high_120 = df.loc[index - 120:index - 1, 'close'].max()
        high_index = df[df["close"] == high_120].index
        high_index = high_index[high_index < index][-1]
        ma120 = df.loc[index, 'ma120']




        min_10 = df.iloc[index - 10:index-1]['close'].min()

        # if  last_close/min_10>1.15:
        #     remarks = last_check(df, index)
        #     print(code + ' ' + date + ' ' + str(evaluate) + ' ' + remarks)
        #     length = evaluate_result.shape[0]
        #     evaluate_result.loc[length] = {"code": code, "date": date, "result": evaluate, "remarks": remarks}


        # 影响小 参数2还可以

        if close > ma120 * 1.05 :
            if vol_20 < vol or vol_30 < vol or rate > 9.8:
                pass
            if result.empty:
                pass
            elif vol > result['vol'].max() * 1.4 or ((rate > 9.8 and code[:3]!='688' and code[:3]!='300') or
                                                         (rate > 18 and (code[:3]=='688' or code[:3]=='300'))) or\
                    (vol >3000000000 and vol >vol_last_5):
                pass
            else:
                return
        else:
            return

        if  vol > 100000000 and vol > negative_vol_20 * 1.5 :
            pass
        elif (rate > 9.8 and close > ma20):
            # if (positive_vol_10 < negative_vol_10 * 1.2 or positive_vol_20 < negative_vol_20 * 1.1):
            pass
        else:
            return


        # max_25 = df.iloc[index - 24:index]['close'].max()
        # if close < max_25:


        rate_list = df.loc[index-31:index - 1, 'rate']
        rate_condition_1 = rate_list[-12:]<-9
        rate_condition_2 = rate_list[-8:] < -5 #good
        rate_condition_3 = rate_list[-5:] > 9
        rate_condition_7 = (rate_list[-25:] > 9.5).sum()


        if rate_condition_1.any() or rate_condition_2.any() or rate_condition_3.any() or rate_condition_7>2:
            return
        rate_list = df.iloc[index-31:index - 1]['close'] / df.iloc[index-31:index - 1]['open']
        rate_condition_4 = rate_list[-8:] < 0.93
        rate_condition_5 = rate_list[-12:] < 0.92
        if rate_condition_4.any() or rate_condition_5.any():
            # remarks = last_check(df, index)
            # print(code + ' ' + date + ' ' + str(evaluate) + ' ' + remarks)
            # length = evaluate_result.shape[0]
            # evaluate_result.loc[length] = {"code": code, "date": date, "result": evaluate,
            #                                "remarks": remarks}
            return




        # else:
        #     if rate_condition_5 <  3 and rate_condition_6 < 4 and rate_condition_4 < 3:
        #         print(code + ' ' + date + ' ' + str(evaluate))
        #         length = evaluate_result.shape[0]
        #         evaluate_result.loc[length] = {"code": code, "date": date, "result": evaluate,"remarks":remarks}
        #     return


        low_xx = df.loc[high_index:index - 1, 'low'].min()

        last_ma5 = df.loc[index - 1, 'ma5']
        last_ma10 = df.loc[index - 1, 'ma10']
        before_ma5 = df.loc[index - 3, 'ma5']
        before_ma10 = df.loc[index - 3, 'ma10']


        if True:

            # 参数2要改
            # if ma120 > ma250 or (close > df.loc[index - 9, 'close']): 没用到
            #     pass
            # else:continue
            if True:
                high_date = df.loc[high_index, 'date']

                week = pd.read_csv(content['week'] + code + '.csv')

                # if week_tight1(week, code, date, high_date, rate):
                if True:
                    if len(df[:index])<250:
                        return

                    simple = week.iloc[0:10, 0:7].copy()
                    week_index = week[week["date"] >= date].index[0]
                    deal = simpleTrend(week[:week_index], simple)
                    simple = pd.DataFrame(columns=['date', 'key', 'flag', 'temp'])
                    week_deal = find_point(deal, simple)
                    week_deal['rate'] = week_deal['key'] / week_deal['key'].shift(1) - 1
                    week_deal = merge(week_deal, week)
                    # # 要改 参数2要改
                    boolean, multiple, start_date = price_change(week[:week_index], week_deal)

                    if not boolean:
                        # remarks = last_check(df, index)
                        # print(code + ' ' + date + ' ' + str(evaluate) + ' ' + remarks)
                        # length = evaluate_result.shape[0]
                        # evaluate_result.loc[length] = {"code": code, "date": date, "result": evaluate,
                        #                                "remarks": remarks}
                        return
                    simple = df.iloc[index-250:index-240, 0:7].copy()
                    deal = simpleTrend(df[index-240:index], simple)
                    simple = pd.DataFrame(columns=['date', 'key', 'flag', 'temp'])
                    day_deal = find_point(deal, simple)
                    day_deal['rate'] = day_deal['key'] / day_deal['key'].shift(1) - 1
                    day_deal = day_merge(day_deal, df[index-250:index])
                    # # 要改 参数2要改
                    boolean, multiple, start_date = day_price_change(df[index-250:index].reset_index(drop=True), day_deal[1:].reset_index(drop=True))

                    if not boolean and close < df[index-250:index]['close'].max():
                        # print(code + ' ' + date + ' ' + str(evaluate))
                        # length = evaluate_result.shape[0]
                        # evaluate_result.loc[length] = {"code": code, "date": date, "result": evaluate,"remarks":remarks}
                        return



                    if start_date != 0:
                        start_index = df[df['date'] <= start_date][-10:]['close'].idxmin()
                        start_high_index = df.loc[start_index:index - 10]['close'].idxmax()
                        if index - start_high_index > 120:
                            # print(code + ' ' + date + ' ' + str(evaluate))
                            # length = evaluate_result.shape[0]
                            # evaluate_result.loc[length] = {"code": code, "date": date, "result": evaluate,"remarks":remarks}
                            return

                        boolean = multiple_top(df.loc[start_index - 15:index])
                        if not boolean:
                            # print(code + ' ' + date + ' ' + str(
                            #     evaluate) + ' ' + remarks + ' ' + remarks_1 + ' ' + remarks_2)
                            # length = evaluate_result.shape[0]
                            # evaluate_result.loc[length] = {"code": code, "date": date, "result": evaluate,
                            #                                "remarks": remarks, "remarks_1": remarks_1,
                            #                                "remarks_2": remarks_2}
                            return

                        week_strat_index = week[week['date'] >= start_date].index[0]
                        if not fluency(week[week_strat_index:week_index]):
                            # print(code + ' ' + date + ' ' + str(evaluate))
                            # length = evaluate_result.shape[0]
                            # evaluate_result.loc[length] = {"code": code, "date": date, "result": evaluate,"remarks":remarks}
                            return


                # 要改 参数2要改 区别不大
                # if (market_vaule > 100000000000 or get_per(code, date)) or (
                #         market_vaule > 8000000000 and get_per(code, date)):
                #     pass
                # else:
                #     continue
                #
                #
                #         low_15 = df.loc[index - 20:index]['low'].min()
                #         low_50 = df.loc[index - 50:index]['low'].min()
                #         low_100 = df.loc[index - 100:index]['low'].min()
                #         high = df.iloc[high_index]['close']
                #         if not ((low_15 > low_50 or high < df.iloc[index]['close'])
                #                 or (low_15 == low_50 > low_100 and high * 0.9 < df.iloc[index]['close'])
                #                 or (low_15 == low_50 > low_100 and high * 0.8 < df.iloc[index]['close'] and
                #                     df.iloc[index]['rate'] > 9.8)):
                #             if multiple < 2.5:
                #                 # print(code + ' ' + date + ' check')
                #
                #                 continue
                #
                #         gap_index = week[week["date"] >= date].index[0]
                #         string = gap(week.loc[:gap_index], high_120)
                #         # print(code + ' ' + date + ' ' + string + ' ' + rps50 + rps120 + rps250)

            ma120 = df.loc[index, 'ma120']
            ma250 = df.loc[index, 'ma250']
            ma120_list = df.loc[index - 10:index, 'ma120']
            ma120_results = (ma120_list / ma120_list.shift(2) - 1)[1:] * 100
            ma120_result = (ma120_results < 0).sum()
            if ma120_result > 7 or ma250 > ma120:
                return

            # low_15 = df.loc[index - 20:index]['low'].min()
            # low_50 = df.loc[index - 50:index]['low'].min()
            # low_100 = df.loc[index - 100:index]['low'].min()
            # high = df.iloc[high_index]['close']
            # if not ((low_15 > low_50 or high < df.iloc[index]['close'])
            #         or (low_15 == low_50 > low_100 and high * 0.9 < df.iloc[index]['close'])
            #         or (low_15 == low_50 > low_100 and high * 0.8 < df.iloc[index]['close'] and
            #             df.iloc[index]['rate'] > 9.8)):
            #     if multiple < 2.5:
            #         # print(code + ' ' + date + ' check')
            #         return
            #     low_before = df.loc[high_index - 40:high_index]['low'].min()
            #     if low_15 < low_before:
            #         print(code + ' ' + date + ' ' + str(evaluate))
            #         return


            if rate_condition_1.any() or rate_condition_2.any() or rate_condition_3.any():

                return

            else:
                print(code + ' ' + date + ' ' + str(evaluate)+' '+remarks+' ' +remarks_1+' ' +remarks_2)
                length = evaluate_result.shape[0]
                evaluate_result.loc[length] = {"code": code, "date": date, "result": evaluate,"remarks":remarks,"remarks_1":remarks_1,"remarks_2":remarks_2}
                return

def week_tight(week, code, date, high_date, rate):
    index = week[week["date"] >= date].index[0]

    week_20 = week.loc[index, 'ma20']
    week_30 = week.loc[index, 'ma30']
    week_50 = week.loc[index, 'ma50']
    week_5 = week.loc[index, 'ma5']
    week_10 = week.loc[index, 'ma10']
    week_5_last = week.loc[index - 1, 'ma5']
    week_10_last = week.loc[index - 1, 'ma10']

    # if (week_20 < week_30 * 1.05) and (week_30 < week_50 * 1.05) and week_5 < week_5_last and week_10 < week_10_last: #没用
    #     return False
    #
    # if week_20 > week_50 and week_30 > week_50:
    #     pass
    # else:
    #     return False

    week_close = week.loc[index, 'close']

    # if week_50 * 1.05 > week_close:#没用
    #     return False
    # rate_50 = (week['ma50'].iloc[index - 10:index] / week['ma50'].shift(2).iloc[index - 10:index] - 1)
    # if (rate_50 < 0).any():
    #     if (rate_50 < -0.015).any(): return
    #     if ((week['ma30'].iloc[index - 10:index] / week['ma30'].shift(2).iloc[index - 10:index] - 1) < 0).any() and \
    #             ((week['ma20'].iloc[index - 10:index] / week['ma20'].shift(2).iloc[index - 10:index] - 1) < 0).any():
    #         return
    # else:return True

    high_index = week[week["date"] >= high_date].index[0]
    if index == high_index: return
    low = week.loc[high_index:index - 1, 'low'].min()
    low_index = week[week["low"] == low].index[-1]
    flag = 1
    if low_index + 1 == index:
        if week.loc[high_index:index - 1, 'close'].max() > week.loc[high_index:index - 1, 'close'].min() * 1.15:
            return False
    else:

        if date == '2018-01-10' and code == '603589':
            pass
        negative = week[low_index:index][week.iloc[low_index:index]['rate'] < 0]
        amount = len(negative)
        if amount != 0:
            negative_vol = negative['vol'].sum() / amount
        else:
            negative_vol = 0

        positive = week[low_index:index][week.iloc[low_index:index]['rate'] > 0]
        amount = len(positive)
        if amount != 0:
            positive_vol = positive['vol'].sum() / amount
        else:
            positive_vol = 0
        if (positive_vol == 0 or negative_vol * 0.8 > positive_vol) and rate < 9.7:
            return False
        else:return True
    if flag != 1:
        price_list = np.array((week.iloc[low_index:index]['close'] + week.iloc[low_index:index]['open']) / 2)
        mean_price = np.mean(price_list)
        std_price = np.std(price_list)
        std = std_price / mean_price
        if std < 0.05:
            flag = 1
    if flag == 1:
        market_vaule = now[now['股票代码'] == code]['总市值'].iloc[0]
        try:
            if market_vaule > 8000000000:
                if week_5 < week_20 and week_10 < week_20:
                    return False
                return True
        except:
            print(code + ' ' + date + '-----------------------------')
        # print(code + ' ' + date)


def week_tight1(week, code, date, high_date, rate):
    try:
        week_index = week[week["date"] >= date].index[0]
    except:
        week_index=week.index[-1]+1
    start_1_index = week.iloc[week_index - 16:week_index]['close'].idxmin()
    start_2_index = week.iloc[week_index - 16:week_index]['open'].idxmin()
    start_1 = week.iloc[week_index - 16:week_index]['close'].min()
    start_2 = week.iloc[week_index - 16:week_index]['open'].min()
    start_index = start_1_index if start_1 < start_2 else start_2_index
    end = week.iloc[start_index:week_index]['close'].idxmax()
    rate_index = week.iloc[start_index:week_index]['rate'].idxmax()
    rate_dif = week.iloc[rate_index]['close'] - week.iloc[rate_index - 1]['close']
    if (week.iloc[start_index:week_index]['rate'].max() > 20 and (
            (week.iloc[end]['close'] - rate_dif) / week.iloc[start_index]['open'] - 1) * 100 < 10) or \
            (week.iloc[start_index:week_index]['rate'].max() > 35 and (
                    (week.iloc[week_index-1]['close'] - rate_dif) / week.iloc[start_index]['open'] - 1) * 100 < 25):
        return

    if (week.iloc[week_index - 6:week_index]['high'] / week.iloc[week_index - 6:week_index][['open', 'close']].max(axis=1)).nlargest(
            4).min() > 1.05 \
            and week.iloc[week_index - 2]['high'] != week.iloc[week_index - 6:week_index]['high'].max() and week.iloc[week_index - 6:week_index][
        'high'].max() == week.iloc[week_index - 20:week_index]['high'].max():
        if not(week.iloc[week_index - 6:week_index]['close']>week.iloc[week_index - 6:week_index]['open']).sum()>4:
            return

    # t = (week.iloc[week_index-4:week_index]['high']/week.iloc[week_index-4:week_index][['open', 'close']].max(axis=1)).nlargest(4).sort_values(ascending=True).reset_index(drop=True)
    # if t.min()>1.03 and week.iloc[week_index-4:week_index]['high'].max()>week.iloc[week_index-20:week_index-8]['high'].max() and\
    #        not (week.iloc[week_index-4]['close']<week.iloc[week_index-2]['close'] and
    #             week.iloc[week_index-3]['close']<week.iloc[week_index-1]['close']) and week.iloc[week_index-1]['rate']<20:
    #     return
    try:
        high_index = week[week["date"] >= high_date].index[0]
    except: high_index=week.index[-1]+1
    if week_index == high_index: return  True
    low = week.loc[high_index:week_index - 1, 'low'].min()
    low_index = week[week["low"] == low].index[-1]
    if low_index + 2 < week_index:

        # if date == '2018-01-10' and code == '603589':
        #     pass
        negative = week[low_index:week_index][week.iloc[low_index:week_index]['rate'] < 0]
        amount = len(negative)
        if amount != 0:
            negative_vol = negative['vol'].sum() / amount
        else:
            negative_vol = 0

        positive = week[low_index:week_index][week.iloc[low_index:week_index]['rate'] > 0]
        amount = len(positive)
        if amount != 0:
            positive_vol = positive['vol'].sum() / amount
        else:
            positive_vol = 0
        # if (positive_vol == 0 or negative_vol * 0.8 > positive_vol) and rate < 9.7:
        #     return False

    return True

def price_change(week, week_deal):
    flag = 0

    # qualified_date = week.iloc[-1]['date'] - timedelta(days=365)
    # qualified_data = (week_deal['date'] >= qualified_date) & (week_deal['rate']>0.55)
    qualified_data = (week_deal.iloc[-6:]['rate'] > 0.55)

    result = week_deal.iloc[-6:][qualified_data]

    deal_high_index_list = result.index
    # for deal_high_index in reversed(deal_high_index_list):
    # rate = week_deal.iloc[deal_high_index]['rate']

    if len(deal_high_index_list) == 0 or (result['key']<0).any():
        return False, 0, 1
    deal_high_index = deal_high_index_list[-1]
    high = week_deal.iloc[deal_high_index]['key']
    start = week_deal.iloc[deal_high_index - 1]['key']
    start_date = week_deal.iloc[deal_high_index - 1]['date']
    high_index = week[week["high"] == high].index.tolist()[-1]
    if deal_high_index > 1:
        low_last_deal_date = week_deal.iloc[deal_high_index - 1]['date']
        low_last_week_index = week[week['date'] == low_last_deal_date].index.tolist()[-1]
        if week['high'].iloc[low_last_week_index:low_last_week_index + 4].max() > high:
            high_index = week['high'].iloc[low_last_week_index:low_last_week_index + 4].idxmax()
            high = week['high'].iloc[high_index]

    abs_increase = high - start

    low = week["close"].iloc[high_index:].min()

    abs_fall = high - low
    if abs_fall / abs_increase < 0.68:
        flag = 1
    elif low / start > 1.5:
        flag = 1

    if low / start<1.3 :
        return False, '111', 2




    # if high * 0.7 < low and high * 0.92 > low:
    #     flag = 1
    # else:
    #     # print(code+'_'+str(normal.iloc[-1]['date']))
    #     pass
    if flag == 1:
        return True, high / start, start_date
    return False, 0, start_date


def gap(week, high):
    if high / week["open"].iloc[-1] > 1.25:
        rate = str(week["rate"].iloc[-1])
        return "等待week " + rate
    return " "


def fluency(df):
    df = df.reset_index(drop=True)
    index = df[df['rate']>=5].index[0]
    data = df[index:].copy().reset_index(drop=True)
    high_index = data['close'].idxmax()
    data_5 = ((data['rate']>5) & (data['rate']<=20)).sum()
    data_20 = (data['rate']>20).sum()
    data_up = (data['rate']>0).sum()
    data_down = (data['rate']<0).sum()
    data_deal = data.loc[:high_index]
    len_data = len(data)
    all_rate = data_deal.iloc[-1]['close']/data_deal.iloc[0]['open'] - 1
    rate_20_sum = (data_deal[data_deal['rate'] > 20]['rate'].sum()) /100
    rate_down = (data_deal[data_deal['rate'] < -3]['rate'].sum()) /100
    rate_up = (data_deal[data_deal['rate'] > 3]['rate'].sum()) /100
    # rate_10_sum = data[(data['rate'] <= 20) & (df['rate'] > 5)]['rate'].sum()


    if abs(rate_down)/rate_up > 0.5:
        return False

    # if abs(rate_down)/all_rate > 0.4:
    #     return False



    # if rate_20_sum/all_rate>0.5:
    #
    #     # if (data_5+data_20)/len_data>0.3:
    #     #     return True
    #     if data_20/data_up>0.3:
    #         return False

    # if data_20/len_data>0.1:
    #     return False
    # if (data_5+data_20)/len_data < 0.2:
    #     return False
    return True


def merge(df, week):
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
    if len(df) < 6:
        return df
    if df.iloc[-3]['flag'] == 'max':
        start = -5
    else:
        start = -4
    i = 0
    for index, row in df[start::-2].iterrows():
        i += 1
        if i > 3 or index <= 0:
            break
        # if i == 0:
        #     continue
        max_index = week[week['date'] == row['date']].index[0]
        min_index = week[week['date'] == df.loc[index + 1]['date']].index[0]

        judge_data = week.loc[max_index:min_index]
        if not (judge_data['close'] < judge_data['ma20']).any():
            df.drop(index=index, inplace=True)
            df.drop(index=index + 1, inplace=True)
            df.loc[index + 2, 'rate'] = df.loc[index + 2]['key'] / df.loc[index - 1]['key'] - 1
            return df.reset_index(drop=True)

    return df

def day_price_change(data, day_deal):
    flag = 0

    # qualified_date = week.iloc[-1]['date'] - timedelta(days=365)
    # qualified_data = (week_deal['date'] >= qualified_date) & (week_deal['rate']>0.55)
    # max_rate = day_deal['rate'].max()
    try:max_rate = day_deal[day_deal['rate']>0.7].iloc[-1]['rate']*0.6
    except:max_rate = 0.3
    qualified_data = (day_deal['rate'] > max_rate)
    result = day_deal[qualified_data]

    deal_high_index_list = result.index
    # for deal_high_index in reversed(deal_high_index_list):
    # rate = week_deal.iloc[deal_high_index]['rate']

    if len(deal_high_index_list) == 0 or (result['key']<0).any():
        return False, 0, 0

    deal_high_index = deal_high_index_list[-1]
    high = day_deal.iloc[deal_high_index]['key']
    high_index = data[data["high"] == high].index.tolist()[-1]

    if deal_high_index == 0:
        start = data.iloc[10:high_index]['low'].min()
        start_index = data[10:][data[10:]["low"] == start].index.tolist()[-1]
        start_date = data.iloc[start_index]['date']
    else:
        start = day_deal.iloc[deal_high_index - 1]['key']
        start_date = day_deal.iloc[deal_high_index - 1]['date']
    if deal_high_index > 1:
        low_last_deal_date = day_deal.iloc[deal_high_index - 1]['date']
        low_last_week_index = data[data['date'] == low_last_deal_date].index.tolist()[-1]
        if data['high'].iloc[low_last_week_index:low_last_week_index + 4].max() > high:
            high_index = data['high'].iloc[low_last_week_index:low_last_week_index + 4].idxmax()
            high = data['high'].iloc[high_index]


    abs_increase = high - start

    low = data["close"].iloc[high_index:].min()

    abs_fall = high - low
    if abs_fall / abs_increase < 0.68:
        flag = 1
    # elif low / start > 1.5:
    #     flag = 1

    if low / start<1.15 :
        return False, '111', start_date




    # if high * 0.7 < low and high * 0.92 > low:
    #     flag = 1
    # else:
    #     # print(code+'_'+str(normal.iloc[-1]['date']))
    #     pass
    if flag == 1:
        return True, high / start, start_date
    return False, 0, start_date

def day_merge(df, data):
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
    if len(df) < 6:
        return df
    if df.iloc[-1]['flag'] == 'max':
        start = -2
    else:
        start = -1

    index = len(df) + start
    while index>2:
        row = df.iloc[index]
        end_index = data[data['date'] == df.loc[index - 1]['date']].index[0]
        start_index = data[data['date'] == df.loc[index - 2]['date']].index[0]
        judge_data = data.loc[start_index:end_index]
        index = index - 1
        if row['flag'] == 'max':
            if not (judge_data['close'] < judge_data['ma50']).any() and df.iloc[index+1]['key'] > df.iloc[index-1]['key']:
                df.drop(index=index, inplace=True)
                df.drop(index=index - 1, inplace=True)
                df.loc[index+1, 'rate'] = df.loc[index+1]['key'] / df.loc[index - 2]['key'] - 1
                index = index - 1
                df = df.reset_index(drop=True)
        else:
            if not (judge_data['close'] > judge_data['ma50']).any() and df.iloc[index+1]['key'] < df.iloc[index-1]['key']:
                df.drop(index=index, inplace=True)
                df.drop(index=index - 1, inplace=True)
                df.loc[index+1, 'rate'] = df.loc[index+1]['key'] / df.loc[index - 2]['key'] - 1
                index = index - 1
                df = df.reset_index(drop=True)


    return df




def get_vol(df, attitude, start, end):
    if attitude == 'negative':
        result = df[start:end][df.iloc[start:end]['rate'] < 0]
    else:
        result = df[start:end][df.iloc[start:end]['rate'] > 0]

    amount = len(result)
    if amount != 0:
        result = result['vol'].sum() / amount
    else:
        result = 0
    return result


def multiple_top(df):
    # if (df.iloc[-11:]['rate'] < -9).any():
    #     return False

    simple = df.iloc[0:10, 0:7].copy()
    deal = simpleTrend(df, simple)
    simple = pd.DataFrame(columns=['date', 'key', 'flag', 'temp', 'index'])
    df_deal = find_point_day(deal, simple, df.reset_index(drop=True))
    df_deal['date'] = df_deal['date'].dt.strftime('%Y-%m-%d')
    df_deal = df_deal[:-1]
    key = df.iloc[-1]['high']
    stand_df = df.reset_index(drop=True)
    for index, row in df_deal.iterrows():
        df_deal.iat[index, 4] = stand_df[stand_df['date'] == row['date']].index[0]
    df_clean_init = df_deal[df_deal['flag'] == 'max']
    # df_clean = df_clean[df_clean['key'] * 0.9 < key]

    df_clean = df_clean_init[df_clean_init['key'] * 1.05 > key].reset_index(drop=True)  # * 1.035
    df_clean = df_clean[df_clean['index'] + 8 < len(stand_df)].reset_index(drop=True)
    if len(df_clean) == 0:
        return True

    df_max_index = df_clean['key'].idxmax()
    df_max = df_clean['key'].iloc[df_max_index]
    df_max_date = df_clean['date'].iloc[df_max_index]
    df_max_index = stand_df[stand_df['date'] == df_max_date].index[0]
    test_low_index = stand_df.iloc[df_max_index:]['close'].idxmin()
    test_low = stand_df.iloc[test_low_index]['close']

    now_index = len(stand_df) - 1

    df_clean_max = df_clean['key'].max()
    df_clean_max_index = df_clean['key'].idxmax()
    df_clean_deal = df_clean[df_clean_max_index:][
        df_clean[df_clean_max_index:]['key'] * 1.035 > df_clean_max].reset_index(drop=True)
    now_price = df.iloc[-1]['close']
    if len(df_clean_deal) == 1:
        max_vol = stand_df.iloc[df_max_index - 2:df_max_index + 2]['vol'].max()
        # 测试关
        if max_vol * 0.7 > stand_df.iloc[-1]['vol']:
            if df_clean_max * 0.95 < now_price:
                return False  # 1101研究中

        # else:return True
        test_index = df_clean_deal.iloc[-1]['index']

        test_key = df_clean_deal.iloc[-1]['key']
        condition = (stand_df[test_index:]['close'] < stand_df[test_index:]['ma50']).sum()

        test_clean_index = df_clean_init[df_clean_init['key'] == test_key].index[-1]
        df_clean_test = df_clean_init.loc[test_clean_index:]
        df_clean_test = df_clean_test[df_clean_test['key'] * 1.035 > test_key].reset_index(drop=True)
        if len(df_clean_test) > 1 or now_price * 1.05 < test_key:
            pass

        # 测试关
        elif (test_key / test_low - 1 > 0.24 and now_index - test_index < 30):
            return False
        elif condition > 1 and now_index - test_index < 40 and test_key / test_low - 1 > 0.2:
            return False

    # if len(df_clean) > 0:
    #     # test_index = df_clean.iloc[-1]['index']
    #     test_index = df_clean['index'].iloc[df_clean['key'].idxmax()]
    #
    #     test_low_index = stand_df.iloc[test_index:]['close'].idxmin()
    #     test_low = stand_df.iloc[test_low_index]['close']
    #     test_price = stand_df.iloc[-1]['close']
    #     if test_price/test_low -1 >0.3:
    #         test_rise = stand_df.iloc[test_low_index:]['rate'][stand_df.iloc[test_low_index:]['rate']>0].sum()
    #         test_down = abs(stand_df.iloc[test_low_index:]['rate'][stand_df.iloc[test_low_index:]['rate']<0].sum())
    #         if test_rise/test_down>2.5:
    #             return False
    # return False

    # 可以继续挖掘 2709 20210517
    if df_clean.iloc[-1]['date'] == df_max_date and len(df_clean) > 1 and df.iloc[-1][
        'close'] < df_max * 0.98 and test_low < df_max * 0.85:
        return False

    if stand_df.iloc[-1]['high'] < (stand_df.iloc[:-2]['high']).max():
        df_clean_deal_2 = df_clean[df_clean_max_index:][df_clean[df_clean_max_index:]['key'] * 1.05 > key].reset_index(
            drop=True)
        df_clean_deal_2 = df_clean_deal_2[df_clean_deal_2['key'] * 0.965 < key].reset_index(drop=True)
        length = len(df_clean_deal_2)
        score = 0
        score_1 = 0
        for i, row in df_clean_deal_2.iterrows():
            test_index = row['index']
            if stand_df.iloc[test_index]['rate'] >= 0:
                test_index = test_index + 1
            if ((stand_df.iloc[test_index:test_index + 3]['rate'] > 0).sum() < 2 or
                    ((stand_df.iloc[test_index:test_index + 3]['close'] - stand_df.iloc[test_index:test_index + 3][
                        'open']) < 0).sum() > 1):
                if (stand_df.iloc[test_index - 1]['vol'] * 1.2 > stand_df.iloc[-1]['vol'] or stand_df.iloc[-1][
                    'high'] != stand_df.iloc[-1]['close']) \
                        and stand_df.iloc[test_index - 1]['vol'] * 2 > stand_df.iloc[-1]['vol']:
                    score += 1
                if stand_df.iloc[test_index - 3:test_index + 3]['vol'].max() < stand_df.iloc[-1]['vol']:
                    score_1 += 1

        if score > 1:
            test_index = df_clean_deal_2.iloc[-1]['index']
            last_max_index = df_clean_deal_2.iloc[-2]['index']
            deal_max_index = df_deal[df_deal['date'] == df_clean_deal_2.iloc[-2]['date']].index[0]
            last_min_index = df_deal.iloc[deal_max_index + 1]['index']
            last_down = stand_df.iloc[last_max_index]['high'] / stand_df.iloc[last_min_index - 1:last_min_index + 1][
                'close'].min() - 1
            now_down = stand_df.iloc[test_index]['high'] / (stand_df.iloc[test_index:-1]['close']).min() - 1
            if now_down * 1.25 > last_down:
                return False
        if length > 1 and score_1 == 0 and df_clean_max != df_clean_deal_2['key'].max() and stand_df.iloc[-1][
            'close'] * 1.06 < stand_df.iloc[:-5]['close'].max():
            return False
        # if length>0:
        #     return False

    # if df_max * 0.95 > key and df_max_index != 0:
    #     point_similar = df_clean[df_clean['key'] * 0.965 < key].reset_index(drop=True)
    #     if len(point_similar) > 0:
    #
    #         first_date = point_similar['date'].iloc[0]
    #         if df_max_date > first_date:
    #             stand_max_index = stand_df[stand_df['date'] == df_max_date].index[0]
    #             df_clean['index'] = df_clean['index'] - (2 * stand_max_index - len(stand_df) + 1)
    #             if ((df_clean['index'] > -8) & (df_clean['index'] < 8)).any():
    #                 return False
    #
    # df_clean = df_clean[df_clean['key'] * 1.02 > key].reset_index(drop=True)
    # df_clean = df_clean[df_clean['key'] * 0.98 < key].reset_index(drop=True)
    #
    # if len(df_clean) > 1 and df_max * 0.94 < key and len(df) + 1 - 35 < df_clean.iloc[-1]['index']:
    #     condition = stand_df.iloc[-8:][stand_df.iloc[-8:]['rate'] < -5]
    #     if len(condition) > 0:
    #
    #         condition_index = condition.index[-1]
    #         min_index = stand_df.loc[condition_index:]['low'].idxmin()
    #         min_rate = stand_df['rate'].iloc[min_index]
    #         if min_rate >= -2:
    #             low = stand_df['low'].iloc[min_index]
    #             hachure = min(stand_df['close'].iloc[min_index], stand_df['close'].iloc[min_index]) / low - 1
    #             if hachure > 0.035:
    #                 return True
    #             else:
    #                 if min_rate < 2:
    #                     vol_mean = stand_df[-10:]['vol'].mean()
    #                     min_mean = stand_df['vol'].iloc[min_index]
    #                     if min_mean < vol_mean * 0.6:
    #                         return True
    #                     return False
    #         else:
    #             return False
    #
    #     last_index = df_clean.iloc[-1]['index']
    #     before_last_index = df_clean.iloc[-2]['index']
    #     last_low = stand_df.iloc[last_index:]['low'].min()
    #     before_low = stand_df.iloc[before_last_index:last_index]['low'].min()
    #     if before_low > last_low:
    #         min_index = stand_df.loc[last_index:]['low'].idxmin()
    #         min_rate = stand_df['rate'].iloc[min_index]
    #         if min_rate >= -2:
    #             low = stand_df['low'].iloc[min_index]
    #             hachure = min(stand_df['close'].iloc[min_index], stand_df['close'].iloc[min_index]) / low - 1
    #             if hachure > 0.035:
    #                 return True
    #         return False
    #
    #     condition_data = stand_df.iloc[last_index:][stand_df.iloc[last_index:]['rate'] < 0]
    #     if len(condition_data) == 0:
    #         pass
    #     else:
    #         min_index = stand_df[last_index:]['low'].idxmin()
    #
    #         min_rate = stand_df['rate'].iloc[min_index]
    #         if min_rate >= -2:
    #             low = stand_df['low'].iloc[min_index]
    #             hachure = min(stand_df['close'].iloc[min_index], stand_df['open'].iloc[min_index]) / low - 1
    #             if hachure < 0.025:
    #
    #                 if min_rate < 2:
    #                     vol_mean = stand_df[-10:]['vol'].mean()
    #                     min_mean = stand_df['vol'].iloc[min_index]
    #                     if min_mean > vol_mean * 0.6:
    #                         return False
    return True

def last_check(data,index):
    # if data.iloc[index]['close']>data.iloc[index-250:index]['close'].max():
    result=''
    if data.iloc[index+1]['close']<data.iloc[index+1]['open'] :
        if data.iloc[index+2]['rate']<0:
            result= "wrong"

    else:result= "right"
        # if data.iloc[index+1]['close']<data.iloc[index+1]['open'] :#and data.iloc[index+1]['vol'] > data.iloc[index]['vol'] * 0.7 效果好
        #     # if data.iloc[index+2]['rate']<0:

    num = (data.iloc[index+1:index+4]['close'] < data.iloc[index+1:index+4]['open']).sum()
    num1=0
    for i in range(1,4):
        if  data.iloc[index+i]['rate']<0:
            num1+=1

    return result+'-'+str(num),result+'-'+str(num)+'-'+str(num1)

def last_check_1(data,index):
    test_index = index + 4
    close = data.iloc[index]['close']
    open = data.iloc[index + 1]['open']
    test = data.iloc[index + 1:test_index]['close'].max()
    test_close = data.iloc[test_index - 1]['close']
    vaule = str(round((test_close / open - 1) * 100, 1))

    test_index = index + 6
    test_close = data.iloc[test_index - 1]['close']
    vaule1 = str(round((test_close / open - 1) * 100, 1))
    t_5_max = str(round((data.iloc[index + 1:test_index]['close'].max() / open - 1) * 100, 1))
    if close > test:
        return 'wrong', vaule, vaule1, t_5_max
    return 'right', vaule, vaule1, t_5_max

def get_per(code, date):
    # return True
    df = performance_df[(performance_df['股票代码'] == code) & (performance_df['公告日期'] < date)]
    df.reset_index(drop=True, inplace=True)
    df_sorted = df.sort_values(by='报告期')[-8:]
    df_sorted = df_sorted.dropna()
    df_sorted.reset_index(drop=True, inplace=True)
    df_sorted['营业收入同比增长'] = df_sorted['营业收入同比增长'].astype(float)
    df_sorted['净利润同比增长'] = df_sorted['净利润同比增长'].astype(float)
    df_sorted.loc[df_sorted['净利润同比增长'] > 120, '净利润同比增长'] = 120
    df_sorted.loc[df_sorted['净利润同比增长'] < -50, '净利润同比增长'] = -50

    profit_rate = df_sorted['净利润同比增长'].mean()

    # data = ak.stock_a_indicator_lg(code)
    # last_date = datetime.strptime(df_sorted.iloc[-2]["报告期"], '%Y-%m-%d').date()
    # last_pe = data[data["trade_date"] <= last_date].iloc[-1]["pe"]
    # last_peg = df_sorted.iloc[-2]["净利润同比增长"]/last_pe
    # now_pe = data[data["trade_date"] <= datetime.strptime(date, '%Y-%m-%d').date()].iloc[-1]["pe"]
    # now_peg = df_sorted.iloc[-1]["净利润同比增长"]/now_pe
    # print(code+" "+date+ " last_pe = "+str(last_pe)+ " last_peg = "+str(last_peg)+" now_pe = "+str(now_pe) +" now_peg = "+str(now_peg))

    df_sorted.loc[df_sorted['营业收入同比增长'] > 120, '营业收入同比增长'] = 120
    income_rate = df_sorted['营业收入同比增长'].mean()

    # print(code+'   '+date+'   '+str(profit_rate)+'   '+str(income_rate))
    if income_rate > 10:
        pass
    else:
        last_negative_index = df_sorted[df_sorted['营业收入同比增长'] < 0]
        if len(last_negative_index) > 0:
            last_negative_index = last_negative_index.index[-1]
            df_up = df_sorted[last_negative_index + 1:]['营业收入同比增长'].mean()
            df_down = df_sorted[:last_negative_index]['营业收入同比增长'].mean()
            abs = df_up - df_down
            if abs < 20 or math.isnan(abs):
                return False

    if profit_rate >= 20:
        return True
        # print(code + '   ' + date + '   profit_rate:' +str(profit_rate) + '   income_rate:' + str(income_rate))
    else:
        last_negative_index = df_sorted[df_sorted['净利润同比增长'] < 0]
        if len(last_negative_index) > 0:
            last_negative_index = last_negative_index.index[-1]
        else:
            # print(code + '   ' + date + '   no:')
            return True
        df_up = df_sorted[last_negative_index + 1:]['净利润同比增长'].mean()
        df_down = df_sorted[:last_negative_index]['净利润同比增长'].mean()
        abs = df_up - df_down
        if (abs > 20 or math.isnan(abs)):
            return True
        # print(code + '   ' + date + '   abs:' +str(abs) + '   income_rate:' + str(income_rate))

        # market_vaule = now[now['股票代码'] == code]['总市值'].iloc[0]

        # print(code + '   ' + date + '   profit_rate:' + str(profit_rate) + '   income_rate:' + str(income_rate)+'    大于1000')

        return False


evaluate_result = pd.DataFrame(
    columns=['code', 'date', 'result', 'remarks', 'remarks_info', 'remarks_1', 't+3', 't+5', 't+5_max', 't3'])
dfs = []
with open('../config.yaml') as f:
    content = yaml.load(f, Loader=yaml.FullLoader)
if __name__ == '__main__':
    com('2025-09-19','002460')
    # pocket(content,-1,1000000)
    # evaluate_result.to_csv(content['result'] + 'result+114.csv', index=False)


