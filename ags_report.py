# -*- coding: utf-8 -*-
from profile.mysql import query,query_list
from profile.mysql_df import query_df
import time
from datetime import datetime,timedelta
import numpy as np
from numpy import mean, ptp, var, std
import pandas as pd
#from  profile.setup import setup_snapshot
from ags_snapshot import analyze_fleet_month_df,analyze_person_month_df

#参数初始化
m_month='2018-07'
m_parameters=['`VRTG_MAX_LD (g)`','`DIST_LDG (feet)`','`ROLL_MAX_BL100 (deg)`','`PITCH_LANDING (deg)`']
ac_type="'73M','73L','73H','738','73E','737','73G','73A'"



def report_v1(name=None,month=''):
    '''
    [数据报表-输出数据报表  TXT格式]######
    函数说明 创建：乔晖 2018/8/10
    [输入Parameters]:
        ac_type:string 机队列表
        month:要分析的月份如'2018-07'
        parameter_list:list 要分析的快照列表，如平飘距离和着陆载荷 ['`VRTG_MAX_LD (g)`','`DIST_LDG (feet)`'] 注意：一定要有``否则会出错

    -------
    [返回值return]：
    返回机队当月指定参数的Q0-Q4值
    
    
    修改说明：
    
    '''
    #机队数值初始化
    df_fleet=analyze_fleet_month_df(ac_type,month,m_parameters)
    #获取个人数值
    df_person=analyze_person_month_df(name,month,m_parameters)

    
    #输出报表
    text_msg='%s，您好：\n' % name
    text_msg=text_msg+'您的%s月QAR飞行品质报告已出炉\n\n' % month
    text_msg=text_msg+'2.【快照数据】\n'
    text_msg=text_msg+'    2.1 起飞阶段\n'
    text_msg=text_msg+'    2.2 着陆阶段\n'
    
    text_msg=text_msg+'        2.2.1 本月着陆载荷数据如下（单位：G）：\n'
    m_loc_x_name='`VRTG_MAX_LD (g)`'
    m_arrow=arrow(df_person.at[m_loc_x_name,'Q2'],df_fleet.at[m_loc_x_name,'Q1'],df_fleet.at[m_loc_x_name,'Q3'])
    text_msg=text_msg+'            本人数值：中位数：%s %s；最大值：%s；最小值：%s；\n' % (round(df_person.at[m_loc_x_name,'Q2'],3),m_arrow,round(df_person.at[m_loc_x_name,'Q4'],3),round(df_person.at[m_loc_x_name,'Q0'],3))
    text_msg=text_msg+'            机队参考值：中位数：%s；机队25%%至75%%区间：%s-%s；\n' % (round(df_fleet.at[m_loc_x_name,'Q2'],3),round(df_person.at[m_loc_x_name,'Q1'],3),round(df_person.at[m_loc_x_name,'Q3'],3))
    text_msg=text_msg+'\n'
    
    text_msg=text_msg+'        2.2.2 本月平飘距离数据如下（单位：英尺）：\n'
    m_loc_x_name='`DIST_LDG (feet)`'
    m_arrow=arrow(df_person.at[m_loc_x_name,'Q2'],df_fleet.at[m_loc_x_name,'Q1'],df_fleet.at[m_loc_x_name,'Q3'])
    text_msg=text_msg+'            本人数值：中位数：%s %s；最大值：%s；最小值：%s；\n' % (int(df_person.at[m_loc_x_name,'Q2']),m_arrow,int(df_person.at[m_loc_x_name,'Q4']),int(df_person.at[m_loc_x_name,'Q0']))
    text_msg=text_msg+'            机队参考值：中位数：%s；机队25%%至75%%区间：%s-%s；\n' % (int(df_fleet.at[m_loc_x_name,'Q2']),int(df_person.at[m_loc_x_name,'Q1']),int(df_person.at[m_loc_x_name,'Q3']))
    text_msg=text_msg+'\n'

    text_msg=text_msg+'        2.2.3 本月着陆坡度（100英尺以下）数据如下（单位：度）：\n'
    m_loc_x_name='`ROLL_MAX_BL100 (deg)`'
    m_arrow=arrow(df_person.at[m_loc_x_name,'Q2'],df_fleet.at[m_loc_x_name,'Q1'],df_fleet.at[m_loc_x_name,'Q3'])
    text_msg=text_msg+'            本人数值：中位数：%s %s；最大值：%s；最小值：%s；\n' % (round(df_person.at[m_loc_x_name,'Q2'],3),m_arrow,round(df_person.at[m_loc_x_name,'Q4'],3),round(df_person.at[m_loc_x_name,'Q0'],3))
    text_msg=text_msg+'            机队参考值：中位数：%s；机队25%%至75%%区间：%s-%s；\n' % (round(df_fleet.at[m_loc_x_name,'Q2'],3),round(df_person.at[m_loc_x_name,'Q1'],3),round(df_person.at[m_loc_x_name,'Q3'],3))
    text_msg=text_msg+'\n'
 
    text_msg=text_msg+'        2.2.4 本月着陆俯仰姿态数据如下（单位：度）：\n'
    m_loc_x_name='`PITCH_LANDING (deg)`'
    m_arrow=arrow(df_person.at[m_loc_x_name,'Q2'],df_fleet.at[m_loc_x_name,'Q1'],df_fleet.at[m_loc_x_name,'Q3'])
    text_msg=text_msg+'            本人数值：中位数：%s %s；最大值：%s；最小值：%s；\n' % (round(df_person.at[m_loc_x_name,'Q2'],3),m_arrow,round(df_person.at[m_loc_x_name,'Q4'],3),round(df_person.at[m_loc_x_name,'Q0'],3))
    text_msg=text_msg+'            机队参考值：中位数：%s；机队25%%至75%%区间：%s-%s；\n' % (round(df_fleet.at[m_loc_x_name,'Q2'],3),round(df_person.at[m_loc_x_name,'Q1'],3),round(df_person.at[m_loc_x_name,'Q3'],3))
    text_msg=text_msg+'\n'   
    print(text_msg)





def arrow(person_q2='',fleet_q1='',fleet_q3=''):
    if person_q2<=fleet_q1:
        return '↓'
    elif person_q2>=fleet_q3:
        return '↑'
    else:
        return ''
        
        
        
report_v1('乔晖',m_month)