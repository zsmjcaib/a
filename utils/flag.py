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


def pocket(content):
    rps_50 = pd.read_csv(content['rps_50'], dtype=str)
    rps_250 = pd.read_csv(content['rps_250'], dtype=str)

    evaluate_result = pd.DataFrame(columns=['code', 'date', 'result','remarks','remarks_info','remarks_1','t+3','t+5','t+5_max','t3'])
    for date, codelist in rps_250.iloc[:, -3:].iteritems():
        codelist = rps_50[date].iloc[:500].drop_duplicates()
        # codelist = rps_120[date].iloc[:].append(rps_250[date].iloc[:]).append(rps_50[date].iloc[:]).drop_duplicates()
        for _, code in codelist.iteritems():
            df = pd.read_csv(content['normal'] + code + '.csv')

            # if  code == '000066' :#and date=='2019-04-10'
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
            # if code!='600859':
            #     continue

            rate = df.loc[index, 'rate']
            if rate > 6 :#
                pass
            else:
                continue

            close_10 = df.loc[index - 7:index - 2, 'close'].max()
            open_10 = df.loc[index - 7:index - 2, 'open'].max()
            if close_10 > open_10:
                high_index = df[df["close"] == close_10].index
                high_index = high_index[high_index < index][-1]
            else:
                high_index = df[df["open"] == open_10].index
                high_index = high_index[high_index < index][-1]
            # print(code + ' ' + date)
            # if code=='600859':
            #     print(1)
            if close_10>df.iloc[high_index-3]['close']*1.28 or close_10>df.iloc[high_index-5]['close']*1.35 or close_10>df.iloc[high_index-10]['close']*1.5:
                pass
            else:
                continue
            if df.iloc[index - 1]['close'] == df.iloc[index - 10:index]['close'].max() and df.iloc[index - 1]['close'] > df.iloc[index - 10:index-1]['open'].max():
                continue

            if df.iloc[index]['close'] * 1.05 < max(df.iloc[index - 10:index]['open'].max(),
                                                    df.iloc[index - 10:index]['close'].max()):
                continue
            if max(df.iloc[high_index]['close'],df.iloc[high_index]['open']) < df.iloc[high_index - 40:high_index - 1]['close'].max():
                continue
            if df.iloc[index]['close'] < df.iloc[index]['ma200'] * 1.05 or df.iloc[index]['close'] < df.iloc[index][
                'ma250'] * 1.05 or df.iloc[index]['close'] < df.iloc[index]['ma50'] * 1.1:
                continue
            if df.iloc[high_index]['ma5'] < df.iloc[high_index - 1]['ma5'] * 1.02:
                continue
            if df.iloc[high_index - 5:high_index]['rate'].sum() < 3:
                continue
            if (df.iloc[high_index - 4:high_index + 1]['rate'] > 5).sum() < 3 and (
                        df.iloc[high_index - 9:high_index + 1]['rate'] > 7).sum() < 4:
                continue
            if (df.iloc[index - 1]['rate'] > 6 and (
                    df.iloc[index - 1]['high'] == df.iloc[index - 1]['close'] or df.iloc[index - 2]['open'] * 1.04 <
                    df.iloc[index - 2]['close'])):
                continue

            if (df.iloc[index - 1]['rate'] > 6 and (
                    df.iloc[index - 1]['high'] == df.iloc[index - 1]['close'] or df.iloc[index - 2]['open'] * 1.04 <
                    df.iloc[index - 2]['close'])):
                continue

            if df.iloc[index - 2]['low'] * 1.15 < df.iloc[index - 2]['close'] and df.iloc[index - 2]['rate'] < 11:
                continue
                # if df.iloc[index-2]['close']==df.iloc[index-2]['high'] and df.iloc[index-1]['rate']<4 and df.iloc[index]['close']==df.iloc[index]['high']:
            if df.iloc[index - 2]['low'] * 1.19 < df.iloc[index - 2]['close'] and df.iloc[index - 2]['rate'] > 11:
                continue
            if df.iloc[high_index]['close'] < df.iloc[high_index - 200:high_index - 10]['close'].max() * 0.9 or \
                    df.iloc[high_index]['close'] < df.iloc[high_index - 250:high_index - 10]['close'].max() * 0.8:
                continue
            if df.iloc[index - 2]['rate'] > 0 and df.iloc[index - 2]['open'] > df.iloc[index - 2]['close'] and \
                    df.iloc[index - 1]['rate'] < -3:
                continue
            market_vaule = now[now['股票代码'] == code]['流通市值'].iloc[0]
            now_price = now[now['股票代码'] == code]['最新价'].iloc[0]
            if market_vaule == '-' or now_price == '-':
                continue
            market_vaule = market_vaule / now_price * close
            if df.iloc[index]['vol'] / market_vaule > 0.4:
                continue
            if df.iloc[index]['high'] / df.iloc[index]['close'] > df.iloc[index]['close'] / df.iloc[index][
                'open'] > 1.02:
                continue
            print(code + ' ' + date)

            # evaluate, high_index = evaluates(df, index)
            # remarks, remarks_info = last_check(df, index)
            # remarks_1, remarks_2, t_5, t_5_max = last_check_1(df, index)
            # t3 = -999
            # if high_index != '':
            #     t3 = round((df.iloc[index + 1:high_index]['low'].min() / df.iloc[index + 1]['open'] - 1) * 100, 2)
            # print(code + ' ' + date + ' ' + str(
            #     evaluate) + ' ' + remarks + ' ' + remarks_info + ' ' + remarks_1 + ' ' + remarks_2 + ' ' + t_5 + ' ' + t_5_max)
            # length = evaluate_result.shape[0]
            # evaluate_result.loc[length] = {"code": code, "date": date, "result": evaluate,
            #                                "remarks": remarks, "remarks_info": remarks_info,
            #                                "remarks_1": remarks_1, "t+3": remarks_2, "t+5": t_5,
            #                                "t+5_max": t_5_max, "t3": t3}
            # evaluate_result.to_csv(content['result'] + 'result+2003.csv', index=False)





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



    index = df[df["date"] == date].index[0]
    close = df.loc[index, 'close']
    evaluate = evaluates(df, index)
    remarks = last_check(df, index)
    # remarks_1,remarks_2 = last_check_1(df, index)


    # if df.iloc[index-1]['close'] < df.iloc[index]['ma20']*1.1:
    close_10 = df.loc[index - 7:index - 2, 'close'].max()
    open_10 = df.loc[index - 7:index - 2, 'open'].max()
    if close_10>open_10:
        high_index = df[df["close"] == close_10].index
        high_index = high_index[high_index < index][-1]
    else:
        high_index = df[df["open"] == open_10].index
        high_index = high_index[high_index < index][-1]

    if index>high_index+5:
        evaluate, high_index = evaluates(df, index)
        remarks, remarks_info = last_check(df, index)
        remarks_1, remarks_2, t_5, t_5_max = last_check_1(df, index)
        t3 = -999
        if high_index != '':
            t3 = round((df.iloc[index + 1:high_index]['low'].min() / df.iloc[index + 1]['open'] - 1) * 100, 2)
        print(code + ' ' + date + ' ' + str(
            evaluate) + ' ' + remarks + ' ' + remarks_info + ' ' + remarks_1 + ' ' + remarks_2 + ' ' + t_5 + ' ' + t_5_max)
        length = evaluate_result.shape[0]
        evaluate_result.loc[length] = {"code": code, "date": date, "result": evaluate,
                                       "remarks": remarks, "remarks_info": remarks_info,
                                       "remarks_1": remarks_1, "t+3": remarks_2, "t+5": t_5,
                                       "t+5_max": t_5_max, "t3": t3}


    return




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



if __name__ == '__main__':
    evaluate_result = pd.DataFrame(columns=['code', 'date', 'result','remarks','remarks_1','remarks_2'])

    pocket(content)

    # code = '002272'
    # date_str = '20240311'
    # if date_str!='':
    #     date = datetime.strptime(date_str, '%Y%m%d').date()
    #     date_str = date.strftime('%Y-%m-%d')
    # test(content,date_str,code,evaluate_result)
    # #
    # df=pd.read_csv('/Users/zsmjcaib/Desktop/code/data/result2/result+2120.csv',dtype=str) #24 23
    # for index,row in df.iterrows():
    #     code = row['code']
    #     s = '000000'
    #     num = len(code)
    #     if num>0:
    #         code = s[num:] + code
    #     date = row['date']
    #     try:
    #         date = datetime.strptime(date, '%Y/%m/%d').date()
    #         date_str = date.strftime('%Y-%m-%d')
    #     except:date_str = date
    #     test(content,date_str,code,evaluate_result)
    # evaluate_result.to_csv(content['result'] + 'result+2121.csv', index=False)

