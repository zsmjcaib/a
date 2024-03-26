import os
import pandas as pd
import yaml
import efinance as ef

from datetime import datetime, timedelta

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

def last_check_1(data,index):
    test_index = index+4
    close = data.iloc[index]['close']
    open = data.iloc[index+1]['open']
    test = data.iloc[index+1:test_index]['close'].max()
    test_close = data.iloc[test_index-1]['close']
    vaule = str(round((test_close/open-1)*100,1))
    if close>test:
        return 'wrong',vaule
    return 'right',vaule
def last_check(data,index):
    # if data.iloc[index]['close']>data.iloc[index-250:index]['close'].max():
    result=''
    if data.iloc[index+1]['close']<data.iloc[index+1]['open'] :
        if data.iloc[index+2]['rate']<0:
            result= "wrong"

    else:result= "right"
        # if data.iloc[index+1]['close']<data.iloc[index+1]['open'] :#and data.iloc[index+1]['vol'] > data.iloc[index]['vol'] * 0.7 效果好
        #     # if data.iloc[index+2]['rate']<0:

    # num = (data.iloc[index+1:index+4]['close'] < data.iloc[index+1:index+4]['open']).sum()
    num = (data.iloc[index+1:index+4]['close'] < data.iloc[index+1:index+4]['open']).sum()

    vol='0'
    if data.iloc[index+1]['close']<data.iloc[index+1]['open'] and data.iloc[index]['close']*1.005>data.iloc[index+1]['close']>data.iloc[index]['close'] :
        vol = str(round(data.iloc[index+1]['vol']/data.iloc[index]['vol'],2))
    return result+'-'+str(num)
def evaluates(df, index):
    price = df.iloc[index + 1]['open']

    max = 0
    min = 10000

    for i, row in df.iloc[index + 2:index + 250].iterrows():

        end_price = row['close']

        if end_price / price > 1.8  :
            if df.iloc[index+40:index+120]['close'].max()/price>1.3:
                return 1
            else:return 2
        elif end_price / price < 0.8:
            high = df.iloc[index + 2:i]['close'].max()
            high_index = df.iloc[index + 1:i]['close'].idxmax() - index
            if high / price > 1.6:
                return 3
            if high / price > 1.4:
                return 4
            # if high / price > 1.15:
            #     return 4
            # if high / price > 1.1:
            #     return 5
            # if high / price > 1.05:
            #     return 6
            max_index = i - index
            # for j, roww in df.iloc[i:index + 250].iterrows():
            #     endd_price = roww['close']
            #     if max < endd_price:
            #         max = endd_price
            #         max_index = j - index
            #     if min >= endd_price:
            #         min = endd_price
            #         min_index = j - index
            #         if endd_price / price < 0.89:
            #             if max / price > 1.45:
            #                 return 12
            #             if max / price > 1.35:
            #                 return 22
            #             if max / price > 1.25:
            #                 return 32
            #             if max / price > 1.15:
            #                 return 42
            #             if max / price > 1.1:
            #                 return 52
            #             if max / price > 1.05:
            #                 return 62
            #             for k, rowww in df.iloc[j:index + 40].iterrows():
            #                 enddd_price = rowww['close']
            #                 if max < enddd_price:
            #                     max = enddd_price
            #                     max_index = k - index
            #                 if min >= enddd_price:
            #                     min = enddd_price
            #                     min_index = k - index
            #                     if enddd_price / price < 0.82:
            #                         if max / price > 1.45:
            #                             return 13
            #                         if max / price > 1.35:
            #                             return 23
            #                         if max / price > 1.25:
            #                             return 33
            #                         if max / price > 1.15:
            #                             return 43
            #                         if max / price > 1.1:
            #                             return 53
            #                         if max / price > 1.05:
            #                             return 63
            #                         else:
            #                             return 10
            #             if max / price > 1.45:
            #                 return 13
            #             if max / price > 1.35:
            #                 return 23
            #             if max / price > 1.25:
            #                 return 33
            #             if max / price > 1.15:
            #                 return 43
            #             if max / price > 1.1:
            #                 return 53
            #             if max / price > 1.05:
            #                 return 63
            #             else:
            #                 return 9
            # if max / price > 1.45:
            #     return 12
            # if max / price > 1.35:
            #     return 22
            # if max / price > 1.25:
            #     return 32
            # if max / price > 1.15:
            #     return 42
            # if max / price > 1.1:
            #     return 52
            # if max / price > 1.05:
            #     return 62
            return 8
    return 5

    max_close = df.iloc[index:index + 40]['close'].max()
    if max_close / price > 1.35:
        return 2
    if max_close / price > 1.25:
        return 3
    if max_close / price > 1.15:
        return 4
    if max_close / price > 1.1:
        return 5
    if max_close / price > 1.05:
        return 6
    else:
        return 7

def buttom(content):
    rps_50 = pd.read_csv(content['rps_50'], dtype=str)
    rps_120 = pd.read_csv(content['rps_120'], dtype=str)
    rps_250 = pd.read_csv(content['rps_250'], dtype=str)
    for date, codelist in rps_250.iloc[:, 300:-255].iteritems():
        # print(date)
        codelist = rps_120[date].iloc[:400].append(rps_250[date].iloc[:500]).append(
            rps_50[date].iloc[:300]).drop_duplicates()
        for _, code in codelist.iteritems():
            df = pd.read_csv(content['normal'] + code + '.csv')
            try:
                index = df[df["date"] == date].index[0]
            except:
                # print(date)
                continue
            close = df.iloc[index]['close']

            market_vaule = now[now['股票代码'] == code]['总市值'].iloc[0]
            if market_vaule == '-':
                continue
            market_vaule = market_vaule / df['close'].iloc[-1] * close
            if market_vaule < 6000000000 or df.iloc[index]['close'] < df.iloc[index]['open']:
                continue


            high = df.iloc[index]['high']
            ma20 = df.iloc[index]['ma20']

            ma120 = df.iloc[index]['ma120']
            ma200 = df.iloc[index]['ma200']
            ma250 = df.iloc[index]['ma250']


            a1 = close<df.iloc[index-70:index]['close'].max()

            a2 = df.iloc[index-50:index]['low'].min()>df.iloc[index-200:index]['low'].min()
            a3 = df.iloc[index-30:index]['low'].min()>df.iloc[index-120:index]['low'].min()
            a4 = df.iloc[index - 20:index]['low'].min() > df.iloc[index - 50:index]['low'].min()

            if not (a2 or a3 or a4):
                continue

            b1 = df.iloc[index-11:index]['high'].max()>df.iloc[index-81:index]['high'].max()
            b2 = (close > df.iloc[index-50:index]['close'].max()) or (high > df.iloc[index-50:index]['high'].max())
            if not (b1 or b2):
                continue

            if close < ma20 or close < ma200 or ma120/ma200<0.9:
                continue

            d1= (df.iloc[index-1:index+1]['close'] > df.iloc[index-1:index+1]['ma200']).all() and (df.iloc[index-45:index+1]['close'] <df.iloc[index-45:index+1]['ma200']).any()
            d2 = (df.iloc[index-2:index+1]['close'] > df.iloc[index-2:index+1]['ma200']).all() and (df.iloc[index-45:index+1]['close'] <df.iloc[index-45:index+1]['ma200']).any()
            d3 = (df.iloc[index-2:index+1]['close'] > df.iloc[index-2:index+1]['ma250']).all() and (df.iloc[index-45:index+1]['close'] <df.iloc[index-45:index+1]['ma250']).any()
            if not(d1 or d2 or d3):
                continue

            ma120_list = df.loc[index - 15:index , 'ma120']
            ma120_result = (ma120_list / ma120_list.shift(2) - 0.995)[2:] * 100
            ma200_list = df.loc[index - 15:index , 'ma200']
            ma200_result = (ma200_list / ma200_list.shift(2) - 0.995)[2:] * 100
            e101= (ma120_result>=0).all() and (ma200_result>=0).all()
            e102 =(ma120_result>=0).all() or (ma200_result>=0).all()
            e103 = close>ma200 and e101
            e1 = df.iloc[index-31:index]['high'].max()/df.iloc[index-121:index]['low'].min()<1.5 and e102
            e2 = df.iloc[index-31:index]['high'].max()/df.iloc[index-121:index]['low'].min()<1.55 and e101
            e3 = df.iloc[index-31:index]['high'].max()/df.iloc[index-121:index]['low'].min()<1.65 and e103 and a1
            if not (e1 or e2 or e3):
                continue

            f1 = df.iloc[index-6:index]['high'].max()/df.iloc[index-121:index]['high'].max()>0.85
            f2 = df.iloc[index-6:index]['high'].max()/df.iloc[index-121:index]['high'].max()>0.8 and a1
            f3 = close/df.iloc[index-11:index]['high'].max()>0.9
            if not((f1 or f2) and f3):
                continue

            # data = performance_df[(performance_df['股票代码'] == code) & (performance_df['公告日期'] < date)]
            # data.reset_index(drop=True, inplace=True)
            # test_date = data.sort_values(by='报告期').iloc[-1]['公告日期'][:10]
            # date1 = datetime.strptime(date, "%Y-%m-%d")
            # date2 = datetime.strptime(test_date, "%Y-%m-%d")
            # # 计算日期差值
            # diff = date1 - date2
            # if diff.days>4:
            #     continue

            evaluate = evaluates(df, index)
            remarks = last_check(df, index)

            remarks_1, remarks_2 = last_check_1(df, index)

            date = df.iloc[index]['date']
            print(code + ' ' + date + ' ' + str(
                evaluate) + ' ' + remarks + ' ' + remarks_1 + ' ' + remarks_2)
            length = evaluate_result.shape[0]
            evaluate_result.loc[length] = {"code": code, "date": date, "result": evaluate, "remarks": remarks,
                                           "remarks_1": remarks_1, "remarks_2": remarks_2}


def test(content, date, code,evaluate_result):
            df = pd.read_csv(content['normal'] + code + '.csv')


            data = performance_df[(performance_df['股票代码'] == code) & (performance_df['公告日期'] > date)]
            data.reset_index(drop=True, inplace=True)
            # test_date = data.sort_values(by='报告期').iloc[-1]['公告日期'][:10]
            test_date = data.sort_values(by='报告期').iloc[0]['公告日期'][:10]



            index = df[df["date"] == date].index[0]

            close = df.iloc[index]['close']


            try:
                test_index = df[df["date"] == test_date].index[0]
                # if index - test_index>4:
                #     return
                if df.iloc[test_index]['low']<df.iloc[test_index-1]['close']*1.03:
                    return
                # if df.iloc[test_index:index]['low'].min()<df.iloc[test_index-1]['close']:
                #     return
                if close>df.iloc[test_index-1]['close']:
                    return
                index = test_index
                evaluate = evaluates(df, index)
                remarks = last_check(df, index)
                date = df.iloc[test_index]['date']
                remarks_1, remarks_2 = last_check_1(df, index)
                print(code + ' ' + date + ' ' + str(
                    evaluate) + ' ' + remarks + ' ' + remarks_1 + ' ' + remarks_2)
                length = evaluate_result.shape[0]
                evaluate_result.loc[length] = {"code": code, "date": date, "result": evaluate, "remarks": remarks,
                                               "remarks_1": remarks_1, "remarks_2": remarks_2}

                return
            except:
                return

            # market_vaule = now[now['股票代码'] == code]['总市值'].iloc[0]
            # if market_vaule == '-':
            #     return
            # market_vaule = market_vaule / df['close'].iloc[-1] * close
            # if market_vaule > 12000000000 or df.iloc[index]['close'] < df.iloc[index]['open']:
            #     date = df.iloc[index]['date']
            #
            #     return
            # return
            high = df.iloc[index]['high']
            ma20 = df.iloc[index]['ma20']

            ma120 = df.iloc[index]['ma120']
            ma200 = df.iloc[index]['ma200']
            ma250 = df.iloc[index]['ma250']


            a1 = close<df.iloc[index-70:index]['close'].max()

            a2 = df.iloc[index-50:index]['low'].min()>df.iloc[index-200:index]['low'].min()
            a3 = df.iloc[index-30:index]['low'].min()>df.iloc[index-120:index]['low'].min()
            a4 = df.iloc[index - 20:index]['low'].min() > df.iloc[index - 50:index]['low'].min()

            if not (a2 or a3 or a4):
                return

            b1 = df.iloc[index-11:index]['high'].max()>df.iloc[index-81:index]['high'].max()
            b2 = (close > df.iloc[index-50:index]['close'].max()) or (high > df.iloc[index-50:index]['high'].max())
            if not (b1 or b2):
                return

            if close < ma20 or close < ma200 or ma120/ma200<0.9:
                return

            d1= (df.iloc[index-1:index+1]['close'] > df.iloc[index-1:index+1]['ma200']).all() and (df.iloc[index-45:index+1]['close'] <df.iloc[index-45:index+1]['ma200']).any()
            d2 = (df.iloc[index-2:index+1]['close'] > df.iloc[index-2:index+1]['ma200']).all() and (df.iloc[index-45:index+1]['close'] <df.iloc[index-45:index+1]['ma200']).any()
            d3 = (df.iloc[index-2:index+1]['close'] > df.iloc[index-2:index+1]['ma250']).all() and (df.iloc[index-45:index+1]['close'] <df.iloc[index-45:index+1]['ma250']).any()
            if not(d1 or d2 or d3):
                return

            ma120_list = df.loc[index - 15:index , 'ma120']
            ma120_result = (ma120_list / ma120_list.shift(2) - 0.995)[2:] * 100
            ma200_list = df.loc[index - 15:index , 'ma200']
            ma200_result = (ma200_list / ma200_list.shift(2) - 0.995)[2:] * 100
            e101= (ma120_result>=0).all() and (ma200_result>=0).all()
            e102 =(ma120_result>=0).all() or (ma200_result>=0).all()
            e103 = close>ma200 and e101
            e1 = df.iloc[index-31:index]['high'].max()/df.iloc[index-121:index]['low'].min()<1.5 and e102
            e2 = df.iloc[index-31:index]['high'].max()/df.iloc[index-121:index]['low'].min()<1.55 and e101
            e3 = df.iloc[index-31:index]['high'].max()/df.iloc[index-121:index]['low'].min()<1.65 and e103 and a1
            if not (e1 or e2 or e3):
                return

            f1 = df.iloc[index-6:index]['high'].max()/df.iloc[index-121:index]['high'].max()>0.85
            f2 = df.iloc[index-6:index]['high'].max()/df.iloc[index-121:index]['high'].max()>0.8 and a1
            f3 = close/df.iloc[index-11:index]['high'].max()>0.9
            if not((f1 or f2) and f3):
                return



            date = df.iloc[index]['date']
            print(code + ' ' + date + ' ' + str(
                evaluate) + ' ' + remarks + ' ' + remarks_1 + ' ' + remarks_2)
            length = evaluate_result.shape[0]
            evaluate_result.loc[length] = {"code": code, "date": date, "result": evaluate, "remarks": remarks,
                                           "remarks_1": remarks_1, "remarks_2": remarks_2}







if __name__ == '__main__':
    evaluate_result = pd.DataFrame(columns=['code', 'date', 'result','remarks','remarks_1','remarks_2'])

    # buttom(content)
    #
    # evaluate_result.to_csv(content['result'] + 'result-1.csv', index=False)


    # code = '603868'
    # date_str = '20220428'
    # if date_str!='':
    #     date = datetime.strptime(date_str, '%Y%m%d').date()
    #     date_str = date.strftime('%Y-%m-%d')
    # test(content,date_str,code,evaluate_result)


    # df=pd.read_csv('/Users/zsmjcaib/Desktop/code/data/month.csv',dtype=str) #24 23
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
    # evaluate_result.to_csv(content['result'] + 'result-2.csv', index=False)

    df=pd.read_csv('/Users/zsmjcaib/Desktop/code/data/result2/result-1.csv',dtype=str) #24 23
    for index,row in df.iterrows():
        code = row['code']
        s = '000000'
        num = len(code)
        if num>0:
            code = s[num:] + code
        date = row['date']
        try:
            date = datetime.strptime(date, '%Y%m%d').date()
            date_str = date.strftime('%Y-%m-%d')
        except:date_str = date
        test(content,date_str,code,evaluate_result)
    evaluate_result = evaluate_result.drop_duplicates(subset=['code', 'date'])
    evaluate_result.to_csv(content['result'] + 'result-3.csv', index=False)