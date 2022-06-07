import pandas as pd
import yaml
import os
from utils.regression_tool import test


if __name__ == '__main__':
    with open('config.yaml') as f:
        content = yaml.load(f,Loader=yaml.FullLoader)
        f.close()
        # path = content['path']
        # line_5_path = content['line_5_path']
        # line_30_path = content['line_30_path']
        # deal_5_path = content['deal_5_path']
        # deal_30_path = content['deal_30_path']
        # normal_5_path = content['normal_5_path']
        # normal_30_path = content['normal_30_path']
        # for code in os.listdir(line_5_path)[0:10]:
        #     test(normal_5_path, code,content)
        test('601899.csv', content)
