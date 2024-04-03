import os
# import akshare as ak
import pandas as pd
import yaml
import efinance as ef
import numpy as np
import math
from utils.deal import find_point
from utils.point import simpleTrend
from utils.deal_day import find_point_day
from datetime import datetime, timedelta

# from utils.std import check

now = ef.stock.get_realtime_quotes()
with open('../config.yaml') as f:
    content = yaml.load(f, Loader=yaml.FullLoader)
file_list = os.listdir(content['performance'])
dfs = []
for file_name in file_list:
    if file_name.endswith('.csv'):
        file_path = os.path.join(content['performance'], file_name)
        df = pd.read_csv(file_path, dtype=str)
        # df_unique = df.drop_duplicates()
        dfs.append(df)

# 合并所有数据框
performance_df = pd.concat(dfs)


def pocket(content):
    rps_50 = pd.read_csv(content['rps_50'], dtype=str)
    rps_120 = pd.read_csv(content['rps_120'], dtype=str)
    rps_250 = pd.read_csv(content['rps_250'], dtype=str)

    evaluate_result = pd.DataFrame(columns=['code', 'date', 'result','remarks','remarks_info','remarks_1','t+3','t+5','t+5_max','t3'])
    for date, codelist in rps_250.iloc[:, -1:].iteritems():
        codelist = rps_120[date].iloc[:400].append(rps_250[date].iloc[:500]).append(rps_50[date].iloc[:300]).drop_duplicates()
        # codelist = rps_120[date].iloc[:].append(rps_250[date].iloc[:]).append(rps_50[date].iloc[:]).drop_duplicates()
        for _, code in codelist.iteritems():
            df = pd.read_csv(content['normal'] + code + '.csv')

            # if  code == '688063' :#and date=='2019-04-10'
            #     print(1)
            #     pass
            try:
                index = df[df["date"] == date].index[0]
            except:
                # print(date)
                continue
            close = df.loc[index, 'close']
            if len(df[:index]) < 260:
                continue
            market_vaule = now[now['股票代码'] == code]['总市值'].iloc[0]
            if market_vaule == '-':
                continue
            market_vaule = market_vaule / df['close'].iloc[-1] * close
            if market_vaule < 8000000000 or df.iloc[index]['close'] < df.iloc[index]['open']:
                continue

            last_close = df.loc[index - 1, 'close']
            vol = df.loc[index, 'vol']
            negative_vol_20 = get_vol(df, 'negative', index - 20, index - 1)
            ma20 = df.loc[index, 'ma20']
            rate = df.loc[index, 'rate']
            if rate > 6 and vol > 100000000 :#and vol > negative_vol_20 * 1.5
                pass
            elif (rate > 9.8 and close > ma20):
                pass
            else:
                continue

            ma20_list = df.loc[index - 8:index - 1, 'ma20']
            ma20_result = (ma20_list / ma20_list.shift(2) - 1)[1:] * 100
            ma30_list = df.loc[index - 8:index - 1, 'ma30']
            ma30_result = (ma30_list / ma30_list.shift(2) - 1)[1:] * 100
            down_5 = df.iloc[index - 8:index + 1]['ma5'] - df.iloc[index - 8:index + 1]['ma20']
            if (not (ma20_result > 0).any() or not (ma30_result > 0).any()) and not (down_5 > 0).any():
                continue
            rate = df.loc[index, 'rate']
            open = df.loc[index, 'open']
            high = df.loc[index, 'high']
            close = df.loc[index, 'close']
            low = df.loc[index, 'low']
            data_max = open if open > close else close
            data_min = close if open > close else open
            max_20 = df.iloc[index - 21:index - 17]['close'].max()

            if high / last_close > 1.096 and rate < 8 and df.iloc[index]['vol'] < df.iloc[index - 50:index - 1][
                'vol'].max() * 0.85 and close < df.iloc[index - 50:index - 1]['close'].max() * 1.03:
                continue

            if max_20 / close > 1.05:  #
                continue

            if ((high / data_max + data_min / low - 2) > (close - open) / close and high != data_max and (
                    data_min / low - 1) < (high / data_max - 1) * 3) or (
                    (high / data_max) + (data_min / low) - 2) > 0.08:  #
                if data_min / low > 1.01:
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

            rate_sum = (abs(df.iloc[index - 51:index - 10]['rate']) > 5).sum()
            rate_sum_1 = (df.iloc[index - 51:index]['open'] / df.iloc[index - 51:index]['close'] > 1.05).sum()
            if (rate_sum > 13 and rate_sum_1 > 5):
                continue

            min_20 = df.iloc[index - 20:index - 1]['close'].min()
            last_high = df.iloc[index - 80:index - 22]['close'].max()
            if min_20 / df.iloc[index - 1]['close'] < 0.8 and min_20 * 1.1 < last_high and df.iloc[index - 1][
                'close'] > last_high:
                continue

            score = 0
            for i, row in df[index - 6:index].iterrows():  # index-6
                if df.iloc[i]['close'] > df.iloc[i - 100:i - 1]['close'].max():  # i - 100:i - 1
                    score += 1

            if score > 4:  # 4
                continue

            if (df.iloc[index - 5:index]['open'] / df.iloc[index - 5:index]['close']).max() > 1.05:
                if (df.iloc[index - 4:index]['low'] > df.iloc[index - 4:index]['ma20']).all() and not (
                        (df.iloc[index - 3:index]['ma5'] > df.iloc[index - 3:index]['ma10']).all() or not (
                        df.iloc[index - 3:index]['ma10'] > df.iloc[index - 3:index]['ma20']).all()):
                    continue
                if (df.iloc[index - 5:index]['low'] / df.iloc[index - 5:index]['ma20']).min() < 1.02 and high != close:
                    continue

            if close > df.iloc[index - 60:index]['high'].max() and vol < df.iloc[index - 40:index]['vol'].mean() * 2.5 \
                    and (close != high or rate < 9.5) and vol < 250000000 and \
                    df.iloc[index]['amount'] < df.iloc[index - 10:index]['amount'].max() * 1.1:
                continue


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

            subset = df.iloc[index - 80:index - 1]
            filtered_rows = subset[(subset['rate'] > 9.5)]
            if not filtered_rows.empty:
                flag = 0
                for last_index in filtered_rows.index:
                    if df.iloc[last_index + 1]['open'] > df.iloc[last_index + 1]['close'] * 1.05:
                        flag += 1
                if flag > 2:
                    continue
            subset = df.iloc[index - 150:index - 1]
            filtered_rows = subset[(subset['rate'] > 9.5)]
            if not filtered_rows.empty:
                last_index = filtered_rows.index[-1]
                if df.iloc[last_index + 1]['open'] > df.iloc[last_index + 1]['close'] * 1.08:
                    continue

            if df.iloc[index - 9]['close'] > df.iloc[index - 3:index]['close'].min() * 1.13 or \
                    df.iloc[index - 8]['close'] > df.iloc[index - 3:index]['close'].min() * 1.12:
                continue

            subset = df.iloc[index - 80:index - 1]
            filtered_rows = subset[(subset['rate'] > 9.5)]
            if not filtered_rows.empty:
                last_index = filtered_rows.index[-1]
                if (df.iloc[last_index + 1:last_index + 5]['high'].max() > df.iloc[last_index + 1:last_index + 5][
                    'close'].min() * 1.18 or
                    df.iloc[last_index + 1:last_index + 6]['high'].max() > df.iloc[last_index + 1:last_index + 6][
                        'close'].min() * 1.2) \
                        and (df.iloc[last_index + 1:index - 1]['rate'] > 7.5).sum() < 1:
                    continue
            filtered_rows = subset[(subset['open'] > close) | (subset['close'] > close)]
            if not filtered_rows.empty:
                last_index = filtered_rows.index[-1]
                if df.iloc[last_index]['open'] > df.iloc[last_index]['close'] * 1.07 and \
                        df.iloc[last_index - 3:last_index]['rate'].max() > 8:
                    continue
            subset = df.iloc[index - 50:index - 1]
            filtered_rows = subset[(subset['rate'] > 9.5)]
            if len(filtered_rows) > 0:
                last_index = filtered_rows.index[-1]
                if (df.iloc[last_index + 1:last_index + 7]['rate'] < 0).all() or (df.iloc[last_index + 1:last_index + 13]['rate'] < 0).sum() > 8:
                    continue
            if df.iloc[index - 60:index]['high'].max() * 0.9 < df.iloc[index]['close'] < df.iloc[index - 60:index][
                'high'].max():
                max_5_index = df.iloc[index - 60:index]['high'].nlargest(8).index
                if (df.iloc[max_5_index]['high'] / df.iloc[max_5_index][['open', 'close']].max(
                        axis=1) > 1.05).sum() > 1 and \
                        (df.iloc[max_5_index]['high'] / df.iloc[max_5_index][['open', 'close']].max(
                            axis=1) > 1.03).sum() > 3:
                    continue
            if df.iloc[index - 2]['rate'] > 5 and close != high and rate<15:
                if df.iloc[index - 1]['high'] / df.iloc[index - 1]['close'] > 1.02 and df.iloc[index - 1]['open'] >df.iloc[index - 1]['close'] \
                        and df.iloc[index - 1]['open'] / df.iloc[index - 1]['low'] > 1.02:
                    continue

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
                continue

            if close > ma120 * 1.05:
                if not(vol_20 < vol*1.05 or vol_30 < vol*1.05 or rate > 9.8):
                    continue
                if result.empty:
                    pass
                elif vol > result['vol'].max() * 1.4 or ((rate > 9.8 and close == high) or rate > 18) \
                        or (vol > 3000000000 and vol > vol_last_5):
                    pass
                elif (df.iloc[index - 80:index]['close'] * 1.04 > df.iloc[index - 80:index]['ma30']).all() or \
                        (df.iloc[index - 100:index]['close'] * 1.02 > df.iloc[index - 100:index]['ma50']).all():
                    pass
                elif df.iloc[index - 20:index]['vol'].mean() * 0 < vol and high < df.iloc[index - 40:index][
                    'high'].max() * 1.04 \
                        and (df.iloc[index - 10:index]['ma10'] < df.iloc[index - 10:index]['close']).all():
                    pass

                elif (df.iloc[index - 50:index]['close'] > df.iloc[index - 50:index]['ma50']).all() and vol > result[
                    'vol'].max() * 1.1 \
                        and (vol > df.iloc[index - 10:index]['vol'].max() * 1.1 or (
                        df.iloc[index - 10:index]['rate'] > 4).sum() > 1):
                    pass
                else:
                    continue
            else:
                continue
            if df.iloc[index - 1]['close'] == df.iloc[index - 1]['high'] and df.iloc[index - 1][
                'rate'] > 9.5 and close != high:
                continue


            ma20_sum = (df.iloc[index - 41:index]['ma20'] > df.iloc[index - 41:index]['close']).sum()
            ma20_close = (df.iloc[index - 21:index]['close'] / df.iloc[index - 21:index]['ma20'] > 1.1).sum()
            diff = (df.iloc[index - 6:index]['open'] - df.iloc[index - 6:index]['close']) / df.iloc[index - 6:index]['open']
            negative_sum = diff[diff < 0].sum()
            negative_amount = (df.iloc[index - 6:index]['open'] > df.iloc[index - 6:index]['close']).sum()
            if ma20_sum == 0 and (ma20_close < 3 or (negative_sum < -0.1 and negative_amount<3)):
                continue

            if df.iloc[index]['amount'] < df.iloc[index - 51:index]['amount'].max() * 0.4 and close != df.iloc[index]['high']:
                continue
            if df.iloc[index]['amount'] < df.iloc[index - 51:index]['amount'].max() * 0.3 and close < df.iloc[index - 51:index]['close'].max():
                continue

            if close != high and close < df.iloc[index - 16:index]['high'].max() * 1.03 and \
                    close > df.iloc[index - 251:index]['high'].max() and df.iloc[index]['rate'] < 10.5:
                test_index = df.iloc[index - 16:index]['high'].idxmax()
                if df.iloc[test_index]['close'] * 1.05 < close and df.iloc[test_index]['close'] * 1.03 <df.iloc[test_index]['high'] \
                        and df.iloc[test_index]['open'] * 1.03 < df.iloc[test_index]['high']:
                        continue

            if (df.iloc[index - 30:index]['close'] > df.iloc[index - 30:index]['ma30']).all():
                test_data = df.iloc[index - 21:index]
                if len(test_data[(test_data['ma5'] < test_data['ma20'])]) + len(
                        test_data[(test_data['ma10'] < test_data['ma20'])]) > 6:
                    continue
            if df.iloc[index - 1]['close'] > df.iloc[index - 100:index - 1]['close'].max() and df.iloc[index - 2][
                'close'] > df.iloc[index - 100:index - 2]['close'].max() \
                    and df.iloc[index - 1]['ma20'] > df.iloc[index - 2]['ma20'] * 1.005 and df.iloc[index - 1]['ma30'] > \
                    df.iloc[index - 2]['ma30'] * 1.005:
                test_index = df.iloc[:index][df.iloc[:index]['ma20'] > df.iloc[:index]['ma10']].index[-1] - 2
                if df.iloc[test_index]['close'] * 1.4 < close:
                    continue
            if high > df.iloc[index - 100:index]['high'].max() and close < df.iloc[index - 100:index]['close'].max():
                continue
            if low > df.iloc[index - 60:index]['high'].max() * 0.99 and (close != high or rate < 9.8) \
                    and (df.iloc[index - 15:index]['close'] > df.iloc[index - 15:index]['ma20']).all():
                continue
            if ((close - open) / close < 0.02 and close != high) or open==high:
                continue
            if high / close < 1.01 and close != high and 10.5 > rate > 9 and close > df.iloc[index - 100:index]['close'].max() \
                    and (df.iloc[index - 2:index]['rate'] > 0).all():
                continue
            if rate > 15 and df.iloc[index]['amount'] < df.iloc[index - 40:index]['amount'].max() * 1.15:
                continue
            if (df.iloc[index - 60:index]['close'] > df.iloc[index - 60:index]['ma30']).all() \
                    and (df.iloc[index - 60:index]['ma30'] * 1.15 > df.iloc[index - 60:index]['close']).all():
                continue
            if (df.iloc[index - 100:index]['close'] > df.iloc[index - 100:index]['ma50']).all() \
                    and (df.iloc[index - 100:index + 1]['ma50'] * 1.25 > df.iloc[index - 100:index + 1]['close']).all():
                continue

            if (df.iloc[index - 20:index]['close'] < df.iloc[index - 20:index]['ma120']).any():
                if df.iloc[index]['ma120'] < df.iloc[index]['ma250'] * 1.25 and close < df.iloc[index]['ma250'] * 1.45:
                    continue
            ma120_list = df.iloc[index - 10:index - 1]['ma120']
            ma120_check = (ma120_list / ma120_list.shift(1) - 1)[1:] < 0
            ma200_list = df.iloc[index - 30:index - 1]['ma200']
            ma200_check = (ma200_list / ma200_list.shift(2) - 1)[1:] < 0
            ma250_list = df.iloc[index - 10:index - 1]['ma250']
            ma250_check = (ma250_list / ma250_list.shift(1) - 0.999)[1:] < 0
            if ma250_check.any():
                continue

            socre = 0
            if ma200_check.any() or ma120_check.any():
                socre += 1

            condition = (df['low'].iloc[index - 30:index] < df['ma200'].iloc[index - 30:index] * 1.05).sum()
            if condition > 0:
                if not (close > df.iloc[index - 200:index - 40]['close'].max() * 1.2
                        and (df.iloc[index - 20:index]['close'] < df.iloc[index - 20:index]['ma20']).sum() == 0
                        and (df.iloc[index - 20:index]['ma5'] < df.iloc[index - 20:index]['ma10'] * 0.99).sum() == 0
                        and (df.iloc[index - 30:index]['rate'] > 5).sum() > 2):
                    if not (df.iloc[index]['vol'] > df.iloc[index - 15:index]['vol'].max()
                            and df.iloc[index - 1]['close'] > df.iloc[index - 1]['ma50'] * 1.08
                            and (df.iloc[index - 60:index]['close'] > df.iloc[index - 60:index]['ma200'] * 0.92).all()
                            and df.iloc[index]['vol'] > df.iloc[index - 5:index]['vol'].max() * 1.2):
                        socre += 1

            if socre == 1:
                continue

            ma120_list = df.loc[index - 10:index, 'ma120']
            ma120_results = (ma120_list / ma120_list.shift(2) - 1)[1:] * 100
            ma120_result = (ma120_results < 0).sum()
            if ma120_result > 4:
                continue
            subset = df.iloc[index - 120:index - 1]
            filtered_rows = subset[(subset['high'] > subset['close'] * 1.17)]
            if len(filtered_rows) > 0:
                last_index = filtered_rows.index[-1]
                if df.iloc[last_index]['high'] > close and df.iloc[last_index]['open'] > df.iloc[last_index]['close'] \
                        and df.iloc[last_index]['high'] == df.iloc[last_index:index]['high'].max():
                    continue

            high_date = df.loc[high_index, 'date']
            week = pd.read_csv(content['week'] + code + '.csv')

            if week_tight1(week, code, date, high_date, rate):
                try:
                    if len(df[:index]) < 250:
                        continue

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
                        if start_date == 1 and close<df.iloc[index-250:index-6]['high'].max()*1.1:
                            continue
                        if start_date == 2:
                            condition = (week.iloc[week_index - 20:week_index]['ma20'] <
                                         week.iloc[week_index - 20:week_index]['ma30'] * 1.02).sum()
                            condition_2 = (week.iloc[week_index - 20:week_index]['ma30'] <
                                           week.iloc[week_index - 20:week_index]['ma50'] * 1.02).sum()
                            if condition != 0 or condition_2 != 0:
                                continue
                    simple = df.iloc[index - 250:index - 240, 0:7].copy()
                    deal = simpleTrend(df[index - 240:index], simple)
                    simple = pd.DataFrame(columns=['date', 'key', 'flag', 'temp'])
                    day_deal = find_point(deal, simple)
                    day_deal['rate'] = day_deal['key'] / day_deal['key'].shift(1) - 1
                    day_deal = day_merge(day_deal, df[index - 250:index])
                    # # 要改 参数2要改
                    boolean, multiple, start_date = day_price_change(df[index - 250:index].reset_index(drop=True),
                                                                     day_deal[1:].reset_index(drop=True))

                    if not boolean and close < df[index - 250:index]['close'].max():
                        # print(code + ' ' + date + ' ' + str(evaluate))
                        # length = evaluate_result.shape[0]
                        # evaluate_result.loc[length] = {"code": code, "date": date, "result": evaluate,"remarks":remarks}
                        continue

                    boolean=True
                    if start_date != 0:
                        start_index = df[df['date'] <= start_date][-10:]['close'].idxmin()
                        start_high_index = df.loc[start_index:index - 10]['close'].idxmax()
                        deal_index = deal[deal["date"] >= start_date].index[0]
                        if index - start_high_index > 90:
                            flag = 0
                            for i in range(len(deal) - 3, deal_index, -1):
                                if deal.iloc[i]['high'] > df.iloc[start_high_index - 3:start_high_index + 4][
                                    'high'].max() * 0.98 and deal.iloc[i]['high'] == deal.iloc[i - 4:i + 5][
                                    'high'].max():
                                    test_index = df[df["date"] == str(deal.iloc[i]['date'])[:-9]].index[0]
                                    if index - test_index < 80:
                                        flag = 1
                            if flag == 0:
                                continue

                        test_start = df[start_index:index][df[start_index:index]['rate']>6]

                        if len(test_start)>0:
                            test_start_index=test_start.index[0]
                            test_rate = (df.iloc[index-1]['close']/df.iloc[test_start_index-1]['close'] -1)*100
                            rate_max_sum = df[start_index-1:index-1]['rate'].nlargest(2).sum()
                            last_test_rate = (df.iloc[test_start_index-1]['close']/df.iloc[test_start_index-30:test_start_index-1]['close'].min() -1)*100
                            if test_rate>15 and rate_max_sum>test_rate*0.75 and 150>index-test_start_index>15 and last_test_rate<25:
                                continue

                        boolean = multiple_top(df.loc[start_index - 15:index])
                        if not boolean:
                            if vol < df.iloc[index - 11:index]['vol'].max() * 0.95:
                                continue
                            test_index = df.iloc[start_index:index - 10]['close'].idxmax()
                            test_low_index = df.iloc[test_index:index - 1]['close'].idxmin()
                            test_close = df.iloc[test_low_index]['close']
                            if (df.iloc[test_index]['close'] * 0.75 > test_close and close * 0.8 > test_close) or \
                                    (df.iloc[test_index]['close'] * 0.80 > test_close and close * 0.80 > test_close and index - test_low_index < 20):
                                pass
                            else:
                                continue

                    if boolean:
                        week_30 = round((week.iloc[week_index - 29:week_index]['close'].sum() + close) / 30, 2)
                        week_50 = round((week.iloc[week_index - 49:week_index]['close'].sum() + close) / 50, 2)
                        if week_30 * 0.98 > week_50:
                            pass
                        else:
                            flag=0
                            if week_50 < week.loc[week_index - 1, 'ma50'] * 1.01 or week_30 < week.loc[week_index - 1, 'ma30'] * 1.02:
                                if close > df.iloc[index - 120:index]['high'].max():
                                    if (df.iloc[index - 40:index]['close'] < df.iloc[index - 40:index]['ma250'] * 1.05).any():
                                        if (df.iloc[index - 60:index]['close'] < df.iloc[index - 60:index]['ma250'] * 1).any():
                                            if not (df.iloc[index - 80:index]['close'] < df.iloc[index - 80:index]['ma250'] * 0.9).any():
                                                flag=1
                                            elif (df.iloc[index - 30:index]['rate'] < -5).sum() > 2 or (df.iloc[index - 20:index]['rate'] < -6).sum() > 0:
                                                continue
                                            elif close > max(ma120, ma200, ma250) * 1.25 and close >df.iloc[index - 30:index]['close'].max() * 1.05 \
                                                    and high < df.iloc[index - 30:index]['high'].max() * 1.03:
                                                    flag = 1
                            if flag==0:
                                continue
                        low_250 = df.iloc[index - 250:index - 1]['close'].min()
                        if low_250 > df.iloc[index]['close'] * 0.58 and low_250 > df.iloc[index - 1]['close'] * 0.64:
                            continue


                        ma5_list = df.iloc[index - 4:index]['ma5']
                        ma5_check = (ma5_list / ma5_list.shift(1) - 1)[1:] < 0
                        if df.iloc[index - 5:index]['close'].max() > close and close != high and ma5_check.any():
                            continue




                        # week_strat_index = week[week['date'] >= start_date].index[0]
                        # if not fluency(week[week_strat_index:week_index]):
                        #     # print(code + ' ' + date + ' ' + str(evaluate))
                        #     # length = evaluate_result.shape[0]
                        #     # evaluate_result.loc[length] = {"code": code, "date": date, "result": evaluate,"remarks":remarks}
                        #     continue
                except:
                    print(code + ' ' + date + '-----------------------------')
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


                rate_list = df.loc[index - 31:index - 1, 'rate']
                rate_condition_1 = rate_list[-12:] < -9
                rate_condition_2 = rate_list[-8:] < -5  # good
                rate_condition_3 = rate_list[-5:] > 9
                rate_condition_7 = (rate_list[-25:] > 9.5).sum()
                rate_list = df.iloc[index - 31:index - 1]['close'] / df.iloc[index - 31:index - 1]['open']
                rate_condition_4 = rate_list[-8:] < 0.93
                rate_condition_5 = rate_list[-12:] < 0.92
                if rate_condition_1.any() or rate_condition_7 > 2 or rate_condition_4.any() or rate_condition_5.any():
                    continue
                if rate_condition_2.any() and rate_condition_3.any():
                    continue
                if rate_condition_2.any():
                    if not (df.iloc[index - 5:index]['rate'].min() > -7 and close > df.iloc[index - 60:index - 10][
                        'close'].max() * 1.15 and vol > df.iloc[index - 10:index]['vol'].max() * 0.65) or \
                            (close < df.iloc[index - 80:index - 15]['high'].max() * 0.96 and vol >
                             df.iloc[index - 10:index]['vol'].max() * 1.1 and close == high):
                        continue
                if rate_condition_3.any():
                    if not (df.iloc[index - 5:index]['close'].max() > df.iloc[index - 80:index - 10][
                        'high'].max() * 1.05 and vol > df.iloc[index - 3:index]['vol'].max()):
                        continue

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
                    continue

                if rate_condition_11 == 0 and rate_condition_12 > 0:  # 太少了
                    continue

                open_list = df.iloc[index - 21:index]['open']
                close_list = df.iloc[index - 21:index]['close']
                check_1 = (open_list / close_list.shift(-1))[:-1] > 1.1
                if check_1.any() and close < df.iloc[index - 21:index]['close'].max() and high != close:
                    continue
                # low_15 = df.loc[index - 15:index]['low'].min()
                # low_50 = df.loc[index - 50:index]['low'].min()
                # low_100 = df.loc[index - 100:index]['low'].min()
                # high = df.iloc[high_index]['close']
                # if not ((low_15 > low_50 or high < df.iloc[index]['close'])
                #         or (low_15 == low_50 > low_100 and high * 0.9 < df.iloc[index]['close'])
                #         or (low_15 == low_50 > low_100 and high * 0.8 < df.iloc[index]['close'] and
                #             df.iloc[index]['rate'] > 9.8)):
                #     if multiple < 2.5:

                print(code + ' ' + date )


            #     evaluate, high_index = evaluates(df, index)
            #     remarks, remarks_info = last_check(df, index)
            #     remarks_1, remarks_2, t_5, t_5_max = last_check_1(df, index)
            #     t3 = -999
            #     if high_index != '':
            #         t3 = round((df.iloc[index + 1:high_index]['low'].min() / df.iloc[index + 1]['open'] - 1) * 100, 2)
            #     print(code + ' ' + date + ' ' + str(
            #         evaluate) + ' ' + remarks + ' ' + remarks_info + ' ' + remarks_1 + ' ' + remarks_2 + ' ' + t_5 + ' ' + t_5_max)
            #     length = evaluate_result.shape[0]
            #     evaluate_result.loc[length] = {"code": code, "date": date, "result": evaluate,
            #                                    "remarks": remarks, "remarks_info": remarks_info,
            #                                    "remarks_1": remarks_1, "t+3": remarks_2, "t+5": t_5,
            #                                    "t+5_max": t_5_max, "t3": t3}
            # evaluate_result.to_csv(content['result'] + 'result+1001.csv', index=False)



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

            if (close / last_close > 1.06 and vol > 100000000
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

        if rate > 1.06 and vol > 100000000 and vol > negative_vol_20 * 1.5 :
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
    week_index = week[week["date"] >= date].index[0]

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
                    (week.iloc[week_index]['close'] - rate_dif) / week.iloc[start_index]['open'] - 1) * 100 < 25):
        return

    if (week.iloc[week_index - 6:week_index]['high'] / week.iloc[week_index - 6:week_index][['open', 'close']].max(axis=1)).nlargest(
            4).min() > 1.05 \
            and week.iloc[week_index - 2]['high'] != week.iloc[week_index - 6:week_index]['high'].max() and week.iloc[week_index - 6:week_index][
        'high'].max() == week.iloc[week_index - 20:week_index]['high'].max():
        return

    t = (week.iloc[week_index-4:week_index]['high']/week.iloc[week_index-4:week_index][['open', 'close']].max(axis=1)).nlargest(4).sort_values(ascending=True).reset_index(drop=True)
    if t.min()>1.03 and week.iloc[week_index-4:week_index]['high'].max()>week.iloc[week_index-20:week_index-8]['high'].max() and\
           not (week.iloc[week_index-4]['close']<week.iloc[week_index-2]['close'] and week.iloc[week_index-3]['close']<week.iloc[week_index-1]['close']):
        return

    high_index = week[week["date"] >= high_date].index[0]
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
        if (positive_vol == 0 or negative_vol * 0.8 > positive_vol) and rate < 9.7:
            return False

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
    if (df.iloc[-11:]['rate'] < -9).any():
        return False

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


if __name__ == '__main__':
    evaluate_result = pd.DataFrame(columns=['code', 'date', 'result','remarks','remarks_1','remarks_2'])

    pocket(content)

    # code = '300580'
    # date_str = '20231106'
    # if date_str!='':
    #     date = datetime.strptime(date_str, '%Y%m%d').date()
    #     date_str = date.strftime('%Y-%m-%d')
    # test(content,date_str,code,evaluate_result)
    #
    # df=pd.read_csv('/Users/zsmjcaib/Desktop/code/data/pocket.csv',dtype=str)
    # for index,row in df.iterrows():
    #     code = row['code']
    #     s = '000000'
    #     num = len(code)
    #     if num>0:
    #         code = s[num:] + code
    #     date = row['date']
    #     date = datetime.strptime(date, '%Y%m%d').date()
    #     date_str = date.strftime('%Y-%m-%d')
    #     test(content,date_str,code,evaluate_result)

    # df=pd.read_csv('/Users/zsmjcaib/Desktop/code/data/failpocket.csv',dtype=str)
    # for index,row in df.iterrows():
    #     code = row['code']
    #     date = row['date']
    #     date = datetime.strptime(date, '%Y%m%d').date()
    #     date_str = date.strftime('%Y-%m-%d')
    #     test(content,date_str,code,evaluate_result)

    # df=pd.read_csv('/Users/zsmjcaib/Desktop/code/data/result2/result+80.csv',dtype=str) #24 23
    # for index,row in df.iterrows():
    #     code = row['code']
    #     s = '000000'
    #     num = len(code)
    #     if num>0:
    #         code = s[num:] + code
    #     date = row['date']
    #     try:
    #         date = datetime.strptime(date, '%Y%m%d').date()
    #         date_str = date.strftime('%Y-%m-%d')
    #     except:date_str = date
    #     test(content,date_str,code,evaluate_result)
    # evaluate_result.to_csv(content['result'] + 'result+81.csv', index=False)
