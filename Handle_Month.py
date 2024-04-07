import pandas as pd
from datetime import datetime
import time
import transbigdata as tbd
import CoordinatesConverter #GPS数据转换库
import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

# 创建一个字典来存储 df2 数据框
df2_dict = {}
i=0
# 初始化全局变量
max_lng = float('-inf')
max_lat = float('-inf')
min_lng = float('inf')
min_lat = float('inf')
def handle_Month(input_path,output_path): #处理月数据
    #导入超大的轨迹数据文件
    print("开始读取大文件了哦~~~~~~~~")
    # df = pd.read_csv("/data/MaoXiaowei/KDD2024/xian_data/20181115_20181131/xianshi_1115_1130.csv",header=None)
    df = pd.read_csv(input_path,header=None)
    print("大文件读取完毕 sir!!!!~~")
    df.columns=['orderid','driverid','trac_list']
    

    #处理精度到小数点后5位
    def round_coordinates(df, decimal_places=5):
        df['Lng'] = df['Lng'].round(decimal_places)
        df['Lat'] = df['Lat'].round(decimal_places)
        return df



    
    def process_element(row):
        global i
        global max_lng, max_lat, min_lng, min_lat
        # 取出两边的引号
        trac_list = row['trac_list'].strip('"[]')

        # 去除方括号并分割为坐标组
        coordinates = [group.split() for group in trac_list.split(', ')]

        # 转换为 DataFrame
        df2 = pd.DataFrame(coordinates, columns=['Lng', 'Lat', 'Timestamp'])

        # 将字符串转换为浮点数
        df2[['Lng', 'Lat', 'Timestamp']] = df2[['Lng', 'Lat', 'Timestamp']].astype(float)

        # 转换时间戳为日期时间格式
        df2['Timestamp'] = df2['Timestamp'].apply(lambda x: datetime.utcfromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S'))
        df2['Lng'], df2['Lat'] = CoordinatesConverter.gcj02towgs84(df2['Lng'], df2['Lat'])  # 将数据点转换为84坐标系下的GPS点
        i=i+1
        
        # 更新全局最大和最小值
        max_lng = max(max_lng, df2['Lng'].max())
        max_lat = max(max_lat, df2['Lat'].max())
        min_lng = min(min_lng, df2['Lng'].min())
        min_lat = min(min_lat, df2['Lat'].min())

        # 添加 CarId 和 OrderId 列
        df2['CarId'] = row['driverid']
        df2['OrderId'] = row['orderid']  # 添加这一行
        df2 = df2[['OrderId', 'CarId', 'Timestamp', 'Lng', 'Lat']]  # 调整列的顺序
        df2 = round_coordinates(df2)  # 舍入到5位小数
        # 获取日期
        date_key = df2['Timestamp'].iloc[0].split()[0]


        # 获取每隔5行的索引 原始数据是3s间隔的轨迹数据
        #进行下采样到15s间隔
        indices = np.arange(0, len(df2), 5)
    #    print(len(df2))
        # 处理不足5行的情况
        if (len(df2)-1) % 5 != 0:
            indices = np.append(indices, len(df2) - 1)
        # 使用 iloc 取样
        df2_downsampled = df2.iloc[indices]

        # 重置索引
        df2_downsampled.reset_index(drop=True, inplace=True)
        df2=df2_downsampled
    #     print(df2)
        if(i%1000==0): 
        #打印显示进度  
        #我已经忘记有多少万行的轨迹数据了似乎是200万行 到达2000左右时处理结束，
        #这个我本想写成多进程处理的。 但是似乎是由于 一条轨迹可能横跨多个文件。还是怎么滴存在一定的难度。
        #希望后来的人可以把这个改成多进程的。
            print(i/1000)
        # 将 df2 存入字典
        if date_key in df2_dict:
            df2_dict[date_key].append(df2)
        else:
            df2_dict[date_key] = [df2]

        return df2


    start_time = time.time()
    # 使用 apply 函数，axis=1 表示按行操作
    df3 = df.apply(process_element, axis=1)

    # 将数据从16G分割成为一天一天的数据 并且下采样到15秒
    for date_key, dfs_list in df2_dict.items():
        merged_df = pd.concat(dfs_list)
        filename = f"merged_{date_key}.csv"
        # merged_df.to_csv("/data/MaoXiaowei/KDD2024/xian_data/20181115_20181131/raw/"+filename, index=False)
        merged_df.to_csv(output_path+filename, index=False)
    end_time = time.time()
    # 打印结果
    print("全局最大经度:", max_lng)
    print("全局最大纬度:", max_lat)
    print("全局最小经度:", min_lng)
    print("全局最小纬度:", min_lat)
    print("程序运行时间:", end_time - start_time, "秒")