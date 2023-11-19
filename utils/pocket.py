import os
# import akshare as ak
import pandas as pd
import yaml
import efinance as ef
import numpy as np
from datetime import datetime, timedelta
import math
from utils.deal import find_point
from utils.point import simpleTrend
from utils.deal_day import find_point_day

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

    evaluate_result = pd.DataFrame(columns=['code', 'date', 'result'])
    for date, codelist in rps_250.iloc[:, -1:].iteritems():
        # codelist = pd.append([ rps_120[date].iloc[:400], rps_250[date].iloc[:400]]).drop_duplicates(inplace=True)
        codelist = rps_120[date].iloc[:400].append(rps_250[date].iloc[:500]).append(
            rps_50[date].iloc[:300]).drop_duplicates()
        for _, code in codelist.iteritems():
            df = pd.read_csv(content['normal'] + code + '.csv')
            if date == '2019-08-02' and code == '300223':
                # print(1)
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
                elif vol > result['vol'].max() * 1.4 or ((rate > 9.8 and code[:3] != '688' and code[:3] != '300') or
                                                         (rate > 15 and (code[:3] == '688' or code[
                                                                                              :3] == '300'))) or vol > 3000000000:
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

            high_date = df.loc[high_index, 'date']

            week = pd.read_csv(content['week'] + code + '.csv')

            if week_tight(week, code, date, high_date, rate):  # 改了
                # if True:

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
                    start_index = df[df['date'] <= start_date][-10:]['close'].idxmin()
                    start_high_index = df.loc[start_index:index - 10]['close'].idxmax()
                    if index - start_high_index > 120:
                        continue
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
            rate_condition_1 = rate_list[-12:] < -9
            rate_condition_2 = rate_list[-8:] < -5  # good
            rate_condition_3 = rate_list[-5:] > 9

            rate_high_list = df.loc[high_index - 31:high_index - 1, 'rate']

            # rate_condition_4 = (rate_high_list < -8).sum()
            # rate_condition_5 = (rate_high_list[-20:] < -5).sum()
            # rate_condition_6 = (rate_high_list[-20:] > 9).sum()

            if rate_condition_1.any() or rate_condition_2.any() or rate_condition_3.any():
                continue

            print(code + ' ' + date + ' ' )
                # new = pd.DataFrame({"code": code, "date": date}, index=[1])

                # evaluate_result = evaluate_result.append(new, ignore_index=True)

    # evaluate_result.to_csv(content['result'] + 'result+.csv', index=False)


def evaluates(df, index):
    price = df.iloc[index + 1]['open']
    for i, row in df.iloc[index:index + 60].iterrows():
        end_price = row['close']
        if end_price / price > 1.3:
            return 1
        elif end_price / price < 0.9:
            high = df.iloc[index:i]['close'].max()
            if high / price >1.2:
                return 3
            for j, roww in df.iloc[i:index + 60].iterrows():
                endd_price = roww['close']
                if endd_price / price > 1.3:
                    return 2

                elif endd_price / price < 0.8:
                    low = df.loc[index :index +20]['close'].min()
                    if low / price>0.9:
                        return 5
                    return 6

    max_close = df.iloc[index:index + 60]['close'].max()
    if max_close / price > 1.2:
        return 3
    else: return 4


def test(content, date, code,evaluate_result):
    df = pd.read_csv(content['normal'] + code + '.csv')
    rps_50 = pd.read_csv(content['rps_50'], dtype=str)

    rps_120 = pd.read_csv(content['rps_120'], dtype=str)
    rps_250 = pd.read_csv(content['rps_250'], dtype=str)

    if date == '':
        for index, row in df.iloc[255:].iterrows():
            # index = df[df["date"] == date].index[0]
            date = row['date']
            if date == '2021-06-22':
                print(1)
                pass
            if (code in rps_120[date].values) or (code in rps_250[date].values) or (code in rps_50[date].values):
                condition = (df['low'].iloc[index - 30:index] < df['ma200'].iloc[index - 30:index] * 1.05)
                if condition.any():
                    continue

                condition_3 = (df['open'].iloc[index - 2:index + 1] < df['ma10'].iloc[index - 2:index + 1] * 1.02)
                condition_4 = (df['close'].iloc[index - 2:index] < df['ma10'].iloc[index - 2:index] * 1.02)
                if not (condition_3.any() or condition_4.any()):
                    continue

                close = df.loc[index, 'close']
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
                vol_20 = df.loc[index - 20:index - 1, 'vol'].mean() * 1.5
                vol_30 = df.loc[index - 30:index - 1, 'vol'].mean() * 1.5

                last_close = df.loc[index - 1, 'close']
                negative_vol_20 = get_vol(df, 'negative', index - 20, index - 1)
                negative_vol_10 = get_vol(df, 'negative', index - 10, index - 1)
                result = df[index - 10:index][df.iloc[index - 10:index]['rate'] < 0]

                high_20 = df.loc[index - 19:index - 1, 'close'].max()
                ma50 = df.loc[index, 'ma50']
                ma20 = df.loc[index, 'ma20']
                high_120 = df.loc[index - 120:index - 1, 'high'].max()
                high_index = df[df["high"] == high_120].index
                high_index = high_index[high_index < index][-1]

                ma120 = df.loc[index, 'ma120']
                max_20 = df.iloc[index - 19:index]['close'].max()
                max_25 = df.iloc[index - 24:index]['close'].max()

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
                if (close / last_close > 1.06 and vol > 100000000 and
                        ((close > high_20 and (
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

                if low_xx / high_120 > 0.5 and open < high_120:
                    if close < ma50 * 1.3 or (close < ma50 * (1.3 + (rate - 10) * 0.01)):
                        pass
                    else:
                        continue
                    ma120 = df.loc[index, 'ma120']
                    ma250 = df.loc[index, 'ma250']
                    if ma120 > ma250 or (close > df.loc[index - 9, 'close']):
                        high_date = df.loc[high_index, 'date']
                        week = pd.read_csv(content['week'] + code + '.csv')
                        if week_tight1(week, code, date, high_date, rate):
                            simple = week.iloc[0:10, 0:7].copy()
                            week_index = week[week["date"] >= date].index[0]
                            deal = simpleTrend(week[:week_index], simple)
                            simple = pd.DataFrame(columns=['date', 'key', 'flag', 'temp'])
                            week_deal = find_point(deal, simple)
                            week_deal['rate'] = week_deal['key'] / week_deal['key'].shift(1) - 1
                            week_deal = merge(week_deal, week)
                            boolean, multiple, start_date = price_change(week[:week_index], week_deal)
                            if not boolean:
                                continue
                            start_index = df[df['date'] == start_date].index[0]
                            boolean = multiple_top(df.loc[start_index - 15:index])
                            if not boolean:
                                continue
                            market_vaule = now[now['股票代码'] == code]['总市值'].iloc[0]
                            if (market_vaule > 100000000000 or get_per(code, date)) or (
                                    market_vaule > 8000000000 and get_per(code, date)):
                                try:
                                    rps50 = '  50_' + str(rps_50[rps_50[date] == code].index[0])
                                except:
                                    rps50 = ''
                                try:
                                    rps120 = '  120_' + str(rps_120[rps_120[date] == code].index[0])
                                except:
                                    rps120 = ''
                                try:
                                    rps250 = '  250_' + str(rps_250[rps_250[date] == code].index[0])
                                except:
                                    rps250 = ''

                                low_15 = df.loc[index - 20:index]['low'].min()
                                low_50 = df.loc[index - 50:index]['low'].min()
                                low_100 = df.loc[index - 100:index]['low'].min()
                                high = df.iloc[high_index]['close']
                                if not ((low_15 > low_50 or high < df.iloc[index]['close'])
                                        or (low_15 == low_50 > low_100 and high * 0.9 < df.iloc[index]['close'])
                                        or (low_15 == low_50 > low_100 and high * 0.8 < df.iloc[index]['close'] and
                                            df.iloc[index]['rate'] > 9.8)):
                                    if multiple < 2.5:
                                        print(code + ' ' + date + ' check')
                                    pass

                                index = week[week["date"] >= date].index[0]
                                string = gap(week.loc[:index], high_120)
                                print(code + ' ' + date + ' ' + string + ' ' + rps50 + rps120 + rps250)

    else:
        # if date == '2019-10-24':
        #     print(1)
        index = df[df["date"] == date].index[0]
        close = df.loc[index, 'close']

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
        high_120 = df.loc[index - 120:index - 1, 'close'].max()
        high_index = df[df["close"] == high_120].index
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
            elif vol > result['vol'].max() * 1.4 or rate > 9.8 or vol >1000000000:
                pass
            else:
                return
        else:
            return

        if (close / last_close > 1.06 and vol > 100000000
                and ((close > high_20 and (
                        vol > negative_vol_20 * 1.5 or negative_vol_20 == 0) and high_index + 30 <= index)
                     or (close > ma20 and (
                                vol > negative_vol_10 * 1.5 or negative_vol_10 == 0) and high_index + 20 <= index))):
            pass

        elif (rate > 9.8 and close > ma20 and high_index + 5 <= index):
            # if (positive_vol_10 < negative_vol_10 * 1.2 or positive_vol_20 < negative_vol_20 * 1.1):
            pass
        else:
            return


        evaluate = evaluates(df, index)

        rate_list = df.loc[index-31:index - 1, 'rate']
        rate_condition_1 = rate_list[-20:]<-9
        rate_condition_2 = rate_list[-10:] < -5 #good
        rate_condition_3 = rate_list[-5:] > 9

        rate_high_list = df.loc[high_index-31:high_index - 1, 'rate']

        rate_condition_4 = (rate_high_list < -8).sum()
        rate_condition_5 = (rate_high_list[-20:] < -5).sum()
        rate_condition_6 = (rate_high_list[-20:] > 9).sum()

        if rate_condition_1.any() or rate_condition_2.any() or rate_condition_3.any():

            return

        # else:
        #     if rate_condition_5 <  3 and rate_condition_6 < 4 and rate_condition_4 < 3:
        #         print(code + ' ' + date + ' ' + str(evaluate))
        #         # new = pd.DataFrame({"code": code, "date": date, "result": evaluate}, index=[1])
        #         len = evaluate_result.shape[0]
        #         evaluate_result.loc[len] = {"code": code, "date": date, "result": evaluate}
        #     return


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
                        return
                    if start_date != 0:
                        start_index = df[df['date'] == start_date].index[0]
                        boolean = multiple_top(df.loc[start_index - 15:index])
                        if not boolean:
                            return
                    else:return
                else:
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

            year_low = df.iloc[index - 250:index]['close'].min()
            # if close < ma120 or close < ma250 or ma250 > ma120 or ma50 < ma120 or ma50 < ma250 or close < year_low * 1.3:
            #     pass
            # else:return
            # if  open > high_120:
            if rate_condition_1.any() or rate_condition_2.any() or rate_condition_3.any():

                return

            else:

                print(code + ' ' + date + ' ' + str(evaluate))
                len = evaluate_result.shape[0]
                evaluate_result.loc[len] = {"code": code, "date": date, "result": evaluate}
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
    if (week_20 < week_30 * 1.05) and (week_30 < week_50 * 1.05) and week_5 < week_5_last and week_10 < week_10_last:
        return
    if week_20 > week_50 or week_30 > week_50:
        pass
    else:
        return

    week_close = week.loc[index, 'close']

    if week_50 * 1.05 > week_close: return
    rate_50 = (week['ma50'].iloc[index - 10:index] / week['ma50'].shift(2).iloc[index - 10:index] - 1)
    if (rate_50 < 0).any():
        if (rate_50 < -0.015).any(): return
        if ((week['ma30'].iloc[index - 10:index] / week['ma30'].shift(2).iloc[index - 10:index] - 1) < 0).any() and \
                ((week['ma20'].iloc[index - 10:index] / week['ma20'].shift(2).iloc[index - 10:index] - 1) < 0).any():
            return

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


def week_tight1(week, code, date, high_date, rate):
    index = week[week["date"] >= date].index[0]
    week_20 = week.loc[index, 'ma20']
    week_30 = week.loc[index, 'ma30']
    week_50 = week.loc[index, 'ma50']
    week_5 = week.loc[index, 'ma5']
    week_10 = week.loc[index, 'ma10']
    week_5_last = week.loc[index - 1, 'ma5']
    week_10_last = week.loc[index - 1, 'ma10']

    if (week_20 < week_30 * 1.05) and (week_30 < week_50 * 1.05) and week_5 < week_5_last and week_10 < week_10_last:
        return

    if week_20 > week_50 or week_30 > week_50:
        pass
    else:
        return
    week_close = week.loc[index, 'close']

    if week_50 * 1.05 > week_close: return
    rate_50 = (week['ma50'].iloc[index - 10:index] / week['ma50'].shift(2).iloc[index - 10:index] - 1)
    if (rate_50 < 0).any():
        if (rate_50 < -0.015).any(): return
        if ((week['ma30'].iloc[index - 10:index] / week['ma30'].shift(2).iloc[index - 10:index] - 1) < 0).any() and \
                ((week['ma20'].iloc[index - 10:index] / week['ma20'].shift(2).iloc[index - 10:index] - 1) < 0).any():
            return

    high_index = week[week["date"] >= high_date].index[0]
    flag = 1
    if index != high_index:
        low = week.loc[high_index:index - 1, 'low'].min()
        low_index = week[week["low"] == low].index[-1]
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
                    # print(code+' '+date+' stop at -3')
                    return False
                return True
        except:
            print(code + ' ' + date + '-----------------------------')
        # print(code + ' ' + date)


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
        return False, 0, 0
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
        return False, '111', start_date
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
    df_deal = find_point_day(deal, simple)
    df_deal['date'] = df_deal['date'].dt.strftime('%Y-%m-%d')
    df_deal = df_deal[:-1]
    key = df.iloc[-1]['high']
    df_clean_init = df_deal[df_deal['flag'] == 'max']
    # df_clean = df_clean[df_clean['key'] * 0.9 < key]
    df_clean = df_clean_init[df_clean_init['key'] * 1.035 > key].reset_index(drop=True)
    if len(df_clean) == 0:
        return True

    df_max_index = df_clean['key'].idxmax()
    df_max = df_clean['key'].iloc[df_max_index]
    df_max_date = df_clean['date'].iloc[df_max_index]
    stand_df = df.reset_index(drop=True)
    df_max_index = stand_df[stand_df['date'] == df_max_date].index[0]
    test_low_index = stand_df.iloc[df_max_index:]['close'].idxmin()
    test_low = stand_df.iloc[test_low_index]['close']
    for index, row in df_clean.iterrows():
        df_clean.iat[index, 4] = stand_df[stand_df['date'] == row['date']].index[0]

    now_index = len(stand_df) - 1

    df_clean_max = df_clean['key'].max()
    df_clean_max_index = df_clean['key'].idxmax()
    df_clean_deal = df_clean[df_clean_max_index:][df_clean[df_clean_max_index:]['key'] * 1.035 > df_clean_max].reset_index(drop=True)
    now_price = df.iloc[-1]['close']
    if len(df_clean_deal) == 1:
        test_index = df_clean_deal.iloc[-1]['index']

        test_key = df_clean_deal.iloc[-1]['key']
        condition = (stand_df[test_index:]['close'] < stand_df[test_index:]['ma50']).sum()

        test_clean_index = df_clean_init[df_clean_init['key'] == test_key].index[-1]
        df_clean_test = df_clean_init.loc[test_clean_index:]
        df_clean_test = df_clean_test[df_clean_test['key'] * 1.035 > test_key].reset_index(drop=True)
        if len(df_clean_test) >1 or now_price *1.05 < test_key:
            return True

        if (test_key / test_low - 1 > 0.24 and now_index - test_index < 30)  :
            return False
        if condition>1 and now_index - test_index < 40 and test_key / test_low - 1 > 0.2:
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


    if df_clean.iloc[-1]['date'] == df_max_date and len(df_clean)>1 and df.iloc[-1]['close']<df_max*0.98 and test_low<df_max*0.85:
        return False



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
    evaluate_result = pd.DataFrame(columns=['code', 'date', 'result'])

    pocket(content)

    # code = '600481'
    # date_str = '20210615'
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
    #     test(content,date_str,code)

    # df=pd.read_csv('/Users/zsmjcaib/Desktop/code/data/result2/result+3.csv',dtype=str)
    #
    # for index,row in df.iterrows():
    #     code = row['code']
    #     date_str = row['date']
    #
    #     test(content,date_str,code,evaluate_result)
    # evaluate_result.to_csv(content['result'] + 'result+24.csv', index=False)
