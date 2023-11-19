the_day = '20210820'  # 截至计算时间
# 先引入后面可能用到的library
import tushare as ts
import efinance as ef
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import time
from statistics import mean


# 使用之前先输入token，可以从个人主页上复制出来，
# 每次调用数据需要先运行该命令
ts.set_token('这里是自己的token码 可以在tushare个人主页找到')
pro = ts.pro_api()

# 正常显示画图时出现的中文和负号
# from pylab import mpl
#
# mpl.rcParams['font.sans-serif'] = ['SimHei']
# mpl.rcParams['axes.unicode_minus'] = False

# 数据来源于tushare，首先使用stock_basic获取当前交易日的所有股票代码。截至2021年8月20日，一共有4444只股票。
##########################################################
df = pro.stock_basic(exchange='', list_status='L',
                     fields='ts_code,symbol,name,area,industry,list_date')
print(len(df))
#########################################################
# 考虑到A股新股上市交易的情况，上市后前一段时间往往会一字涨停，然后出现下跌调整。为了剔除新股次新股的影响，这里选择2020年8月1日前上市的股票作为分析样本，共得到3893只股票。

# 排除掉新股次新股，这里是只考虑2020年8月1日以前上市的股票

###########################################################
# df=df[df['list_date'].apply(int).values<2020801]
df = df[df['list_date'] < '20200801']
print(len(df))
##########################################################
# 输出结果：3893
# 获取当前所有非新股次新股代码和名称
codes = df.ts_code.values
names = df.name.values
# 构建一个字典方便调用
code_name = dict(zip(names, codes))


# 使用tushare获取上述股票周价格数据并转换为周收益率
# 设定默认起始日期为2020年06月01日，结束日期为2021年08月20日
# 日期可以根据需要自己改动
def get_data(code, start='20200601', end='20210820'):
    df = pro.daily(ts_code=code, start_date=start, end_date=end, fields='trade_date,close')
    # 将交易日期设置为索引值
    df.index = pd.to_datetime(df.trade_date)
    df = df.sort_index()
    # 计算收益率
    return df.close


# 构建一个空的dataframe用来装数据
data = pd.DataFrame()

n = 0
for name, code in code_name.items():
    print(n)
    n += 1
    data[name] = get_data(code)
    time.sleep(0.1)  # 避免调用过快 引发调用限制 在这里使用sleep函数

data = data.fillna(method='bfill')  # 补充缺失值


# 计算收益率
def cal_ret(df, w=5):
    '''w:周5;月20;半年：5; 一年250
    '''
    df = df / df.shift(w) - 1
    return df.iloc[w:, :].fillna(0)


ret10 = cal_ret(data, w=10)


# 计算RPS
def get_RPS(ser):
    df = pd.DataFrame(ser.sort_values(ascending=False))
    df['n'] = range(1, len(df) + 1)
    df['rps'] = (1 - df['n'] / len(df)) * 100
    return df


# 计算每个交易日所有股票滚动w日的RPS
def all_RPS(data):
    dates = (data.index).strftime('%Y%m%d')
    RPS = {}
    for i in range(len(data)):
        RPS[dates[i]] = pd.DataFrame(get_RPS(data.iloc[i]).values, columns=['收益率', '排名', 'RPS'],
                                     index=get_RPS(data.iloc[i]).index)
    return RPS


rps10 = all_RPS(ret10)


# 获取所有股票在某个期间的RPS值
def all_data(rps, ret):
    df = pd.DataFrame(np.NaN, columns=ret.columns, index=ret.index)
    for date in ret.index:
        date = date.strftime('%Y%m%d')
        d = rps[date]
        for c in d.index:
            df.loc[date, c] = d.loc[c, 'RPS']
    return df


# 构建一个以前面收益率为基础的空表
df_new = pd.DataFrame(np.NaN, columns=ret10.columns, index=ret10.index)
for date in df_new.index:
    print(date)
    date = date.strftime('%Y%m%d')
    d = rps10[date]
    for c in d.index:
        df_new.loc[date, c] = d.loc[c, 'RPS']


# def plot_rps(stock):
#     plt.subplot(211)
#     data[stock][10:].plot(figsize=(16, 16), color='r')
#     plt.title(stock + '股价走势', fontsize=15)
#     plt.yticks(fontsize=12)
#     plt.xticks([])
#     ax = plt.gca()
#     ax.spines['right'].set_color('none')
#     ax.spines['top'].set_color('none')
#     plt.subplot(212)
#     df_new[stock].plot(figsize=(16, 8), color='b')
#     plt.title(stock + 'RPS相对强度', fontsize=15)
#     my_ticks = pd.date_range('2020-11-1', the_day, freq='m')
#     plt.xticks(my_ticks, fontsize=12)
#     plt.yticks(fontsize=12)
#     ax = plt.gca()
#     ax.spines['right'].set_color('none')
#     ax.spines['top'].set_color('none')
#     plt.show()





