import pandas as pd
import yaml







if __name__ == '__main__':
    with open('config.yaml') as f:
        content = yaml.load(f,Loader=yaml.FullLoader)
        for j in ['120','250','50']:
            path = content['rpsinfo_'+j]
            df = pd.read_csv(path, index_col=0)
            result = pd.DataFrame({})
            for i,row in df.iterrows():
                row_T = row.T
                if row_T.isna().all():continue
                # 对每列进行排序处理，即按日期排序
                df_sorted = row_T.sort_values(ascending=False)

                # 提取排好序的 code 数据列（第一列）
                code_series = df_sorted.iloc[:600]

                # 构造结果 DataFrame 对象
                result_df = pd.DataFrame({
                    i: code_series.index
                })
                result = pd.concat([result,result_df],axis=1)
            # result.replace('\t', '', regex=True,inplace=True)
            result.to_csv(content['rps_'+j], index=False)
        f.close()


