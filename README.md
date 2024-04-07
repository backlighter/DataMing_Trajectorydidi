# 使用方法

## 拆分月文件到每天 并下采样到15s

```
示例：
python main.py --mfi /data/MaoXiaowei/KDD2024/xian_data/20181101_20181115/xianshi_1101_1115.csv --mfo /data/MaoXiaowei/KDD2024/xian_data/20181101_20181115/raw2 --phase month
```

## 把日文件拆分成TPTK 可以使用的以CarID为文件名的代码

```
示例：
python main.py --dfi /data/MaoXiaowei/KDD2024/xian_data/20181101_20181115/raw/ --dfo /data/MaoXiaowei/KDD2024/xian_data/Split_data_1101_1115/ --phase day
```

