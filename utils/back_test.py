import pandas as pd
import yaml
from datetime import datetime

with open('../config.yaml') as f:
    content = yaml.load(f, Loader=yaml.FullLoader)

def evaluates(df, index):

    price = df.iloc[index + 1]['open']

    max = 0
    min = 10000

    for i, row in df.iloc[index+1:index + 40].iterrows():
        end_price = row['close']

        if end_price / price > 1.4:
            return 1
        elif end_price / price < 0.94:
            high = df.iloc[index+1:i]['close'].max()
            if high / price >1.3:
                return 2
            if high / price >1.2:
                return 3
            if high / price >1.1:
                return 4
            if high / price > 1.05:
                return 5
            for j, roww in df.iloc[i:index + 40].iterrows():
                endd_price = roww['close']
                if max<endd_price:
                    max = endd_price
                if min >= endd_price:
                    min = endd_price

                    if endd_price / price < 0.89:
                        if max / price > 1.4:
                            return 12
                        if max / price > 1.3:
                            return 22
                        if max / price > 1.2:
                            return 32
                        if max / price > 1.1:
                            return 42
                        for k, rowww in df.iloc[j:index + 40].iterrows():
                            enddd_price = rowww['close']
                            if max < enddd_price:
                                max = enddd_price
                            if min >= enddd_price:
                                min = enddd_price
                                if enddd_price / price < 0.82:
                                    if max / price > 1.4:
                                        return 13
                                    if max / price > 1.3:
                                        return 23
                                    if max / price > 1.2:
                                        return 33
                                    if max / price > 1.1:
                                        return 43
                                    else:
                                        return 9
                        if max / price > 1.4:
                            return 13
                        if max / price > 1.3:
                            return 23
                        if max / price > 1.2:
                            return 33
                        if max / price > 1.1:
                            return 43

                        else:return 8
            if max / price > 1.4:
                return 12
            if max / price > 1.3:
                return 22
            if max / price > 1.2:
                return 32
            if max / price > 1.1:
                return 42
            return 7


    max_close = df.iloc[index:index + 40]['close'].max()
    if max_close / price > 1.3:
        return 2
    if max_close / price > 1.2:
        return 3
    if max_close / price > 1.1:
        return 4
    if max_close / price > 1.05:
        return 5
    else: return 6

def get_condition_2(data,close,index,end_index):
    test = data.loc[index + 1:end_index]['close'].max()
    if close > test:
        return 'wrong'
    return 'right'

def get_roe(data,index,end_index):
    return  round((data.iloc[end_index]['close'] / data.iloc[index + 1]['open'] - 1) * 100, 1)

def test(content, date, code, evaluate_result):
    data = pd.read_csv(content['normal'] + code + '.csv')
    index = data[data["date"] == date].index[0]
    evaluate = evaluates(data, index)
    close = data.iloc[index]['close']

    end_index = index + 4
    if data.iloc[index + 1]['close'] < data.iloc[index + 1]['open']:#第一天阴
        if data.iloc[index + 2]['rate'] <= 0 or data.iloc[index + 2]['close'] <= data.iloc[index + 2]['open']: #第二天阴或降
            end_index = index + 3
            condition_1 = "wrong"
            condition_2 = get_condition_2(data,close,index,end_index)
            roe = str(get_roe(data,index,index + 2))
            length = evaluate_result.shape[0]
            evaluate_result.loc[length] = {"code": code, "date": date, "result": evaluate, "condition_1": condition_1,
                                           "condition_2": condition_2, "roe": roe}
            print(code + ' ' + date + ' ' + str(evaluate)+' '+condition_1+' ' +condition_2+' ' +roe)

            return


        else:#第一天阴，第二天晴且热
            num = (data.iloc[index + 1:index + 4]['close'] <= data.iloc[index + 1:index + 4]['open']).sum()
            condition_1 = '-'+str(num)
            condition_2 = get_condition_2(data, close, index, end_index)
            roe = str(get_roe(data,index,index + 3))
            length = evaluate_result.shape[0]
            evaluate_result.loc[length] = {"code": code, "date": date, "result": evaluate, "condition_1": condition_1,
                                           "condition_2": condition_2, "roe": roe}
            print(code + ' ' + date + ' ' + str(evaluate)+' '+condition_1+' ' +condition_2+' ' +roe)

            return

    else:#第一天晴
        num = (data.iloc[index + 1:index + 4]['close'] <= data.iloc[index + 1:index + 4]['open']).sum()
        condition_1 = "right-"+str(num)
        condition_2 = get_condition_2(data, close, index, end_index)
        roe = str(get_roe(data, index, index + 3))
        length = evaluate_result.shape[0]
        evaluate_result.loc[length] = {"code": code, "date": date, "result": evaluate, "condition_1": condition_1,
                                           "condition_2": condition_2, "roe": roe}
        print(code + ' ' + date + ' ' + str(evaluate) + ' ' + condition_1 + ' ' + condition_2 + ' ' + roe)

        return

if __name__ == '__main__':
    evaluate_result = pd.DataFrame(columns=['code', 'date', 'result','condition_1','condition_2','roe'])


    # code = '002581'
    # date_str = '20200714'
    # if date_str!='':
    #     date = datetime.strptime(date_str, '%Y%m%d').date()
    #     date_str = date.strftime('%Y-%m-%d')
    # test(content,date_str,code,evaluate_result)




    df=pd.read_csv('/Users/zsmjcaib/Desktop/code/data/result2/result+10.csv',dtype=str) #24 23
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
    evaluate_result.to_csv(content['result'] + 'result+16.csv', index=False)