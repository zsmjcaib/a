import yaml
import pandas as pd
import os

if __name__ == '__main__':
    with open('config.yaml') as f:
        content = yaml.load(f,Loader=yaml.FullLoader)
    path = content['normal']
    # if not os.path.exists(content['loser']):
    #     loser = pd.DataFrame(columns=['date','open','high','low','close','amount','vol','rate','ma5','ma10','ma15','ma20','ma30','ma50','ma120','ma200','ma250'])
    # else:
    #     loser = pd.DataFrame(columns=['date','open','high','low','close','amount','vol','rate','ma5','ma10','ma15','ma20','ma30','ma50','ma120','ma200','ma250'])
    for file in os.listdir(path):
        if file == '.DS_Store': continue
        normal = pd.read_csv(content['normal'] + file)
        if len(normal[normal['date'] == '2023-11-10'])>1:
            print(file)

        # loser = loser.append(data)
    # loser = loser.sort_values('date')
    # loser.reset_index(drop=True)
    # loser.to_csv(content['loser']+'loser.csv' , index=False)
