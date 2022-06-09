
def array_ma(normal):
    if normal['ma30'].iloc[-1]>=normal['ma60'].iloc[-1]>=normal['ma120'].iloc[-1] and normal['ma5'].iloc[-1]>=normal['ma120'].iloc[-1]:
        if normal['ma60'].iloc[-2]<normal['ma120'].iloc[-2] or normal['ma30'].iloc[-2]<normal['ma120'].iloc[-2]\
                or normal['ma30'].iloc[-2]<normal['ma60'].iloc[-2]:
            return True
    elif normal['ma30'].iloc[-1]>=normal['ma60'].iloc[-1]>=normal['ma250'].iloc[-1] and normal['ma5'].iloc[-1]>=normal['ma250'].iloc[-1]:
        if normal['ma60'].iloc[-2] < normal['ma250'].iloc[-2] or normal['ma30'].iloc[-2] < normal['ma250'].iloc[-2] \
                or normal['ma30'].iloc[-2] < normal['ma60'].iloc[-2]:
            return True
