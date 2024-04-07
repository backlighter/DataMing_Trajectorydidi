import os
import pandas as pd
from tqdm import tqdm
'''
把数据拆分成不同的文件，提供TPTK读取
'''
def group_by_car_id(input_csv_path, output_directory):
    # 读取CSV文件
    df = pd.read_csv(input_csv_path)

    # 按照CarId分组
    grouped = df.groupby('CarId')

    # 确保输出目录存在，如果不存在则创建
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    # 遍历每个分组，将分组写入单独的CSV文件
    for car_id, group_df in tqdm(grouped, desc=f'Processing {os.path.basename(input_csv_path)}'):
        # 按照Timestamp列进行排序
        group_df = group_df.sort_values(by='Timestamp')
        
        output_file_path = os.path.join(output_directory, f'{car_id}.csv')
        # 写入CSV文件，不包含索引
        group_df.to_csv(output_file_path, index=False, columns=['CarId', 'Timestamp', 'Lng', 'Lat'])

# 示例调用
# input_csv_path = '../data/TenDayData/merged_2018-11-02.csv'
# output_directory = '../data/Split_data_7d/20181102/'
# group_by_car_id(input_csv_path, output_directory)
        

'''
把数据拆分成不同的文件，提供TPTK读取
'''        
def Split_File(input_directory,output_parent_directory): 
    '''
    input_directory = '/data/MaoXiaowei/KDD2024/xian_data/20181101_20181115/raw/'
    output_parent_directory = '/data/MaoXiaowei/KDD2024/xian_data/Split_data_1101_1115/'
    '''
    # Loop through each day's file
    for day in range(1, 16):
        # Generate input and output paths
        input_csv_path = os.path.join(input_directory, f'merged_2018-11-{day:02d}.csv')
        output_directory = os.path.join(output_parent_directory, f'201811{day:02d}')
        print(f"正在处理{day}的数据")
        # Process the file
        group_by_car_id(input_csv_path, output_directory)
