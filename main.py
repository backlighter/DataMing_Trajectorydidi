from Handle_Month import handle_Month 
from Split_Day_File import Split_File
import argparse

if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--mfi', help='以月为单位的文件input  type=csv')
    parser.add_argument('--mfo', help='处理为以天为单位的output type=csv (注意 此处下采样到15)')
    parser.add_argument('--dfi', help='将以天为单位的csv作为input 将其分割为以车辆为单位的文件 用于TPTK 清洗后路网匹配')
    parser.add_argument('--dfo', help='清洗后的文件存储的位置')
    parser.add_argument('--phase', help='the preprocessing phase [month,day]')
    opt=parser.parse_args()
    if opt.phase == 'month':
        handle_Month(opt.mfi, opt.mfo)
    elif opt.phase == 'day':
        Split_File(opt.dfi, opt.dfo)
    else:
        raise Exception('unknown phase')