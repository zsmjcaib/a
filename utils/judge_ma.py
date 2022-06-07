
def array_ma(normal):
    if normal['ma5'].iloc[-1]>=normal['ma10'].iloc[-1]>=normal['ma20'].iloc[-1]>=normal['ma30'].iloc[-1]>=\
        normal['ma60'].iloc[-1]>=normal['ma120'].iloc[-1]:
        if normal['ma30'].iloc[-2]<normal['ma120'].iloc[-2] or normal['ma60'].iloc[-2]<normal['ma120'].iloc[-2]:
            return True