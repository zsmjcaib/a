import pandas as pd
import talib
def stock_macd(df):

    diff, dea, macd = talib.MACD(df["close"],
                                    fastperiod=12,
                                    slowperiod=26,
                                    signalperiod=9)
    df = df.copy()
    df["diff"]=round(diff*10,2)
    df["dea"] = round(dea*10,2)
    df["macd"] = round(macd*100,2)
    return df
