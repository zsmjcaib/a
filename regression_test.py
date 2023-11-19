import pandas as pd
import yaml
import os
from utils.regression_tool import rps_info


if __name__ == '__main__':
    with open('config.yaml') as f:
        content = yaml.load(f,Loader=yaml.FullLoader)
        path = content['normal']
        f.close()
        i=0
        # for code in os.listdir(path):
        #     if test(code, content):
        #         i+=1
        #         if i%500==0:
        #             print(i)
        test('600031.csv', content)

