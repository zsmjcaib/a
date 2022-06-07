import struct
import datetime
import os
import utils.getMacd as getMacd

def stock_csv(file, target_file):
    data = []
    with open(file, 'rb') as f:

        file_object = open(target_file, 'w+')
        list=("date"+","+"open"+","+"high"+","+"low"+","+"close"+","+"vol"+","+"amount"+","
                                        +"diff"+","+"dea"+","+"macd"+"\n")
        file_object.writelines(list)
        while True:
            stock_date = f.read(4)
            stock_open = f.read(4)
            stock_high = f.read(4)
            stock_low= f.read(4)
            stock_close = f.read(4)
            stock_amount = f.read(4)
            stock_vol = f.read(4)
            stock_reservation = f.read(4)
            # date,open,high,low,close,amount,vol,reservation

            if not stock_date:
                break
                 # 4字节 如20091229
            stock_date = struct.unpack("l", stock_date)
            stock_open = struct.unpack("l", stock_open)     #开盘价*100
            stock_high = struct.unpack("l", stock_high)     #最高价*100
            stock_low= struct.unpack("l", stock_low)        #最低价*100
            stock_close = struct.unpack("l", stock_close)   #收盘价*100
            stock_amount = struct.unpack("f", stock_amount) #成交额
            stock_vol = struct.unpack("l", stock_vol)       #成交量
            stock_reservation = struct.unpack("l", stock_reservation)  # 保留值
            date_format = datetime.datetime.strptime(str(stock_date[0]), '%Y%M%d')  # 格式化日期
            list= date_format.strftime('%Y-%M-%d')+","+\
                str(stock_open[0]/100)+","+str(stock_high[0]/100.0)+","+str(stock_low[0]/100.0)+","+str(stock_close[0]/100.0)+","+str(stock_vol[0]/100)+","+str(stock_amount[0])+"\n"
            file_object.writelines(list)
        file_object.close()
        # getMacd.stock_macd(target_file)




if __name__ == '__main__':
    list=["sh","sz"]
    #feq = "day"
    dir="/lday/"

    for l in list:
        path = 'D:/app/new_jyplug/vipdoc/'+l+dir
        listfile = os.listdir('D:/app/new_jyplug/vipdoc/'+l+dir)

        for i in listfile:
            if i.startswith("0",2)|i.startswith("30",2)|i.startswith("60",2):
                target =  'D:/project/data/stock/normal/day/' + i[2:-4] + '.csv'
                stock_csv(path + i, target)



