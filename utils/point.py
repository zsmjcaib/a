import pandas as pd
import os
import yaml
def simpleTrend(df,df_simple):



    #找第一个新增数据
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
    df_simple['date'] = pd.to_datetime(df_simple['date'], format='%Y-%m-%d')

    tem_data=df[df["date"]>df_simple.iloc[-1]["date"]]
    dfSimple = compare(df_simple,tem_data)
    dfSimple.reset_index(drop=True, inplace=True)
    return dfSimple



def compare(dfSimple,tem_data) -> pd.DataFrame:
    for index,row in tem_data.iterrows():
        # if str(row['date']) == '2023-04-21 00:00:00':
        #     print(1)
        dfSimple =calculation(row,dfSimple)
    return dfSimple


def calculation(row,dfSimple) -> pd.DataFrame:
    #右包左
    if(row["high"]>=dfSimple["high"].iloc[-1] and row["low"]<=dfSimple["low"].iloc[-1]):
        dfSimple.iat[-1, 0] = row["date"]
        #上升
        if(dfSimple["high"].iloc[-1]>=dfSimple["high"].iloc[-2]):
            dfSimple.iat[-1, 2] = row["high"]
            #dfSimple.iat[-1, 4] = max(row["close"],dfSimple.at[-1, "low"])
            dfSimple.iat[-1, 4] = max(row["close"],dfSimple.iat[-1, 3])

        #下降
        else:
            dfSimple.iat[-1, 3] = row["low"]
            #dfSimple.iat[-1, 4] = min(row["close"],dfSimple.at[-1, "high"])
            dfSimple.iat[-1, 4] = min(row["close"],dfSimple.iat[-1, 2])

    #左包右
    elif(row["high"]<=dfSimple["high"].iloc[-1] and row["low"]>=dfSimple["low"].iloc[-1]):
        dfSimple.iat[-1, 0] = row["date"]
        #上升
        if(dfSimple["high"].iloc[-1]>=dfSimple["high"].iloc[-2]):
            #dfSimple.iat[-1, 1] = max(row["low"],dfSimple.at[-1, "open"])
            dfSimple.iat[-1, 1] = max(row["low"],dfSimple.iat[-1, 1])
            dfSimple.iat[-1, 3] = row["low"]
        #下降
        else:
            #dfSimple.iat[-1, 1] = min(row["high"],dfSimple.at[-1, "open"])
            dfSimple.iat[-1, 1] = min(row["high"],dfSimple.iat[-1, 1])
            dfSimple.iat[-1, 2] = row["high"]
    else:
        dfSimple = dfSimple.append(row[0:7])
    return dfSimple

if __name__ == '__main__':
    with open('../config.yaml') as f:
        content = yaml.load(f, Loader=yaml.FullLoader)
        f.close()
    path = content['week']
    simple_path = content['week_simple']
    for file in os.listdir(path):
        if file == '.DS_Store': continue
        normal = pd.read_csv(content['week'] + file)
        if not os.path.exists(simple_path + file):
            simple = normal.iloc[0:10,0:7].copy()
        else:
            simple = pd.read_csv(simple_path + file)

        df = simpleTrend(normal, simple)
        df.to_csv(simple_path + file, index=False)











