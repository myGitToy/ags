# -*- coding: utf-8 -*-

"""
文档说明【模块】：
    QAR月报模块，用于生成QAR品质报告，调用ags_mail完成邮件发送
    引用规范：请使用下列语句
        import ags_mail as mail #主模块引用
        import ags_report as report   #报告生成模块引用
    初始化：
        每月邮件发送列表初始化： report.mail_init(month)
    
重要事项：
    每次导入ags_mail_list后写入month，如果需要发送对应的邮件，还需要对setup中的初始条件进行相应的更改，内容包括month/start_date/end_date，否则邮件发送内容会与实际想发送的内容不符合

基本框架：
    __init__   初始化
    send    发送邮件
    arrow 生成代表数据高低的箭头
    
数据结构：
    ags_mail_list
    +----------+----------+-----------+-----------+------------+-----------+
    mail_id          name        dep_name    email       mobile       valid                  
    +----------+----------+-----------+-----------+------------+-----------+           
    int key     varcha       varchar    varchar                 int 0:invalid 1:valid           


    ags_mail_log            
    +----------+-----------+-----------+------------+
    send_id     mail_id     month       status
    +----------+-----------+-----------+------------+
    int key     int         varchar     int 0:未发送 1:已发送
    
    
数据结构说明：
    1. ags_mail_list
        1.1 本表使用name/dep_name/email做三重约束
        1.2 三重约束的目的是为了防止插入重复数值
        1.3 本表没有采用自增长的id，原因是未来设计每月会重复导入一次，因此数据表中内容会做清除，因此不用id作为primary key
        1.4 未来本表可能的数据变动
            1.4.1 人员调整 所属分部会有变化
            1.4.2 新增加人员 新下队的
            1.4.3 人员重名
版本信息：
    version 0.1
    乔晖 2018/9/23

修改日志：




SQL语句：
    ##获取指定飞行员日期间的QAR信息##
    select lnk.姓名,lnk.机上岗位,lnk.技术授权,DATE_FORMAT(ags.`Flight Date`,"%Y-%m-%d"),ags.`A/C Tail`,ags.`Flight No`,ags.`Departure Airport`,ags.`Arrival Airport`,ags.`Event Short Name`,ags.`Maximum Value`,ags.`Severity Class No`
    from ags_event ags,crew_link lnk 
    where lnk.key_id=ags.key_id and ags.`Event Validity` ='Valid' and ags.`Event Type`='Flight Operation'
    and ags.`Severity Class No`>=2 and lnk.姓名 ='乔晖'
    and ags.`Event Short Name` not in (select `Event Short Name` from ags_event_exception where `Severity Class No`>=2) 
    and ags.`Flight Date` between '2018-8-1' and '2018-8-31' 
    order by lnk.`姓名`,ags.`Event Short Name`


"""
import time
import numpy as np
import pandas as pd
import logging
import ags_report as report
from profile.mysql import query,query_list
from profile.mysql_df import query_df
from ags_snapshot import analyze_fleet_month_df,analyze_person_month_df
from ags_event import export_ags_event_summary
from profile.setup import setup_smtp as setup
from profile.setup import setup_dep_name as dep
from datetime import datetime,timedelta
from numpy import mean, ptp, var, std
#from  profile.setup import setup_snapshot

#参数初始化
logging.basicConfig(filename='logger.log', level = logging.DEBUG, format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
#logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
m_month=setup.month
m_start_date=setup.start_date
m_end_date=setup.end_date
m_parameters=['`VRTG_MAX_LD (g)`','`DIST_LDG (feet)`','`ROLL_MAX_BL100 (deg)`','`PITCH_LANDING (deg)`','`PITCH_LIFTOFF (deg)`','`PITCH_RATAVGTO (deg/s)`']
ac_type="'73M','73L','73H','738','73E','737','73G','73A'"
df_event=None
df_fleet=None


def report_fleet_v1():
    """
    ###【停用】###
    [数据报表-输出数据报表（机队）  TXT格式]######
    函数说明 创建：乔晖 2018/8/10
    [输入Parameters]:
        ac_type:string 机队列表
        month:要分析的月份如'2018-07'
        parameter_list:list 要分析的快照列表，如平飘距离和着陆载荷 ['`VRTG_MAX_LD (g)`','`DIST_LDG (feet)`'] 注意：一定要有``否则会出错

    -------
    [返回值return]：
        返回机队当月指定参数的Q0-Q4值
    """
    #获取人员和事件列表初始化
    df_event=export_ags_event_summary(start_date=m_start_date,end_date=m_end_date,flag_csv=0)
    #机队数值初始化
    df_fleet=analyze_fleet_month_df(ac_type,m_month,m_parameters)
    idx = df_event.index
    name_list = idx.tolist() 
    for name in name_list:
        report_person_v1(name,m_month)

def report_person_v1(name=None,month=''):
    """
    [数据报表-输出数据报表（个人）  TXT格式]######
    函数说明 创建：乔晖 2018/8/10
    [输入Parameters]:
        ac_type:string 机队列表
        month:要分析的月份如'2018-07'
        parameter_list:list 要分析的快照列表，如平飘距离和着陆载荷 ['`VRTG_MAX_LD (g)`','`DIST_LDG (feet)`'] 注意：一定要有``否则会出错

    -------
    [返回值return]：
        返回机队当月指定参数的Q0-Q4值
    
    #df_event=export_ags_event_summary(start_date=m_start_date,end_date=m_end_date,flag_csv=0),df_fleet=analyze_fleet_month_df(ac_type,m_month,m_parameters)
    修改说明：
    
    """
    #获取个人数值
    df_person=analyze_person_month_df(name,month,m_parameters)
    #NA值处理
    df_event.fillna(0, inplace = True)
    df_person.fillna(0, inplace = True)
    
    
    #输出报表
    text_msg='%s，您好：\n' % name
    text_msg=text_msg+'您的%s月QAR飞行品质报告已出炉\n\n' % month
    
    text_msg=text_msg+'1.【汇总数据】\n'
    text_msg=text_msg+'本月航班数：%s\n' % int(df_event.at[name,'航段数'])
    text_msg=text_msg+'二级事件数：%s；二级事件率：%s%%\n' % (int(df_event.at[name,'二级事件数量']),round(df_event.at[name,'二级事件率']*100,2))
    text_msg=text_msg+'三级事件数：%s；三级事件率：%s%%\n' % (int(df_event.at[name,'三级事件数量']),round(df_event.at[name,'三级事件率']*100,2))  
    text_msg=text_msg+'预留字段\n'
    text_msg=text_msg+'\n' 
    text_msg=text_msg+'\n' 
    
    
    
    
    text_msg=text_msg+'2.【快照数据】\n'
    text_msg=text_msg+'    2.1 起飞阶段\n'
    text_msg=text_msg+'        2.1.1 本月起飞俯仰姿态数据如下（单位：度）：\n'
    m_loc_x_name='`PITCH_LIFTOFF (deg)`'
    m_arrow=arrow(df_person.at[m_loc_x_name,'Q2'],df_fleet.at[m_loc_x_name,'Q1'],df_fleet.at[m_loc_x_name,'Q3'])
    text_msg=text_msg+'            本人数值：中位数：%s %s；最大值：%s；最小值：%s；\n' % (round(df_person.at[m_loc_x_name,'Q2'],3),m_arrow,round(df_person.at[m_loc_x_name,'Q4'],3),round(df_person.at[m_loc_x_name,'Q0'],3))
    text_msg=text_msg+'            机队参考值：中位数：%s；机队25%%至75%%区间：%s-%s；\n' % (round(df_fleet.at[m_loc_x_name,'Q2'],3),round(df_person.at[m_loc_x_name,'Q1'],3),round(df_person.at[m_loc_x_name,'Q3'],3))
    text_msg=text_msg+'\n'    
    
    text_msg=text_msg+'        2.1.2 本月起飞平均抬轮速率数据如下（单位：度/秒）：\n'    
    m_loc_x_name='`PITCH_RATAVGTO (deg/s)`'
    m_arrow=arrow(df_person.at[m_loc_x_name,'Q2'],df_fleet.at[m_loc_x_name,'Q1'],df_fleet.at[m_loc_x_name,'Q3'])
    text_msg=text_msg+'            本人数值：中位数：%s %s；最大值：%s；最小值：%s；\n' % (round(df_person.at[m_loc_x_name,'Q2'],3),m_arrow,round(df_person.at[m_loc_x_name,'Q4'],3),round(df_person.at[m_loc_x_name,'Q0'],3))
    text_msg=text_msg+'            机队参考值：中位数：%s；机队25%%至75%%区间：%s-%s；\n' % (round(df_fleet.at[m_loc_x_name,'Q2'],3),round(df_person.at[m_loc_x_name,'Q1'],3),round(df_person.at[m_loc_x_name,'Q3'],3))
    text_msg=text_msg+'\n'  
    
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
    #print('%s 已输出完毕' % name)
    return text_msg

def report_person_v2(self,name=None):
    """
    [数据报表-输出数据报表（个人）  TXT格式]######
    函数说明 创建：乔晖 2018/9/15
    [输入Parameters]:
        ac_type:string 机队列表
        month:要分析的月份如'2018-07'
        parameter_list:list 要分析的快照列表，如平飘距离和着陆载荷 ['`VRTG_MAX_LD (g)`','`DIST_LDG (feet)`'] 注意：一定要有``否则会出错

    -------
    [返回值return]：
        返回机队当月指定参数的Q0-Q4值
    #df_event=export_ags_event_summary(start_date=m_start_date,end_date=m_end_date,flag_csv=0),df_fleet=analyze_fleet_month_df(ac_type,m_month,m_parameters)
    修改说明：
    2018/9/15 创立V2版
    本次修订目的是将report_fleet_v1的输出作为report_person_v2的输入，降低服务器读取机队数据的符合
    按照原先布局，读取每个人数据时都需要读取一遍机队数据，修订后通过引入pandas，将机队数据各维度组成矩阵，因此800名飞行员只需要读取一次数据即可
    
    """
    
    #获取个人数值
    df_person=analyze_person_month_df(name,month,m_parameters)
    #NA值处理
    #print(df_event)
    #df_event=self.df_event
    df_event.fillna(0, inplace = True)
    df_person.fillna(0, inplace = True)
    
    
    #输出报表
    text_msg='%s，您好：\n' % name
    text_msg=text_msg+'您的%s月QAR飞行品质报告已出炉\n\n' % month
    
    text_msg=text_msg+'1.【汇总数据】\n'
    text_msg=text_msg+'本月航班数：%s\n' % int(df_event.at[name,'航段数'])
    text_msg=text_msg+'二级事件数：%s；二级事件率：%s%%\n' % (int(df_event.at[name,'二级事件数量']),round(df_event.at[name,'二级事件率']*100,2))
    text_msg=text_msg+'三级事件数：%s；三级事件率：%s%%\n' % (int(df_event.at[name,'三级事件数量']),round(df_event.at[name,'三级事件率']*100,2))  
    text_msg=text_msg+'预留字段\n'
    text_msg=text_msg+'\n' 
    text_msg=text_msg+'\n' 
    
    
    
    
    text_msg=text_msg+'2.【快照数据】\n'
    text_msg=text_msg+'    2.1 起飞阶段\n'
    text_msg=text_msg+'        2.1.1 本月起飞俯仰姿态数据如下（单位：度）：\n'
    m_loc_x_name='`PITCH_LIFTOFF (deg)`'
    m_arrow=arrow(df_person.at[m_loc_x_name,'Q2'],df_fleet.at[m_loc_x_name,'Q1'],df_fleet.at[m_loc_x_name,'Q3'])
    text_msg=text_msg+'            本人数值：中位数：%s %s；最大值：%s；最小值：%s；\n' % (round(df_person.at[m_loc_x_name,'Q2'],3),m_arrow,round(df_person.at[m_loc_x_name,'Q4'],3),round(df_person.at[m_loc_x_name,'Q0'],3))
    text_msg=text_msg+'            机队参考值：中位数：%s；机队25%%至75%%区间：%s-%s；\n' % (round(df_fleet.at[m_loc_x_name,'Q2'],3),round(df_person.at[m_loc_x_name,'Q1'],3),round(df_person.at[m_loc_x_name,'Q3'],3))
    text_msg=text_msg+'\n'    
    
    text_msg=text_msg+'        2.1.2 本月起飞平均抬轮速率数据如下（单位：度/秒）：\n'    
    m_loc_x_name='`PITCH_RATAVGTO (deg/s)`'
    m_arrow=arrow(df_person.at[m_loc_x_name,'Q2'],df_fleet.at[m_loc_x_name,'Q1'],df_fleet.at[m_loc_x_name,'Q3'])
    text_msg=text_msg+'            本人数值：中位数：%s %s；最大值：%s；最小值：%s；\n' % (round(df_person.at[m_loc_x_name,'Q2'],3),m_arrow,round(df_person.at[m_loc_x_name,'Q4'],3),round(df_person.at[m_loc_x_name,'Q0'],3))
    text_msg=text_msg+'            机队参考值：中位数：%s；机队25%%至75%%区间：%s-%s；\n' % (round(df_fleet.at[m_loc_x_name,'Q2'],3),round(df_person.at[m_loc_x_name,'Q1'],3),round(df_person.at[m_loc_x_name,'Q3'],3))
    text_msg=text_msg+'\n'  
    
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
    #print('%s 已输出完毕' % name)
    return text_msg

def report_person_detail(self,filter_event_level=3,filter_event_valid='valid'):
    """
    [数据报表-输出个人当月全部事件]######
    函数说明 创建：乔晖 2018/9/22
    [输入Parameters]:
        filter_event_level:int QAR事件，比如输入2，则代表输出内容为2级以上事件（含）
        filter_event_valid:string 事件有效性，默认为有效；有效：valid 非有效：invalid
        
    -------
    [返回值return]：
        返回机队当月指定参数的Q0-Q4值   
    """
    pass

def get_mail_list(self,dep_name_list=None,flag_valid=1):
    """
    [得到应发送的邮件列表]######
    函数说明 创建：乔晖 2018/9/22
    [输入Parameters]:
        dep_name_list:list 需要抓取的分部列表，该信息可从setup.setup_dep_name获取。
                            比如要获取737全部分部，则:
                            from profile.setup import setup_dep_name as dep
                            dep.dep_list737
        flag_valid:boolean 邮件列表有效性，默认为有效 1
        
    -------
    [返回值return]：
        返回邮件列表 包含工号 姓名 分部 邮箱列表 手机号码
    """    
    pass

def mail_init(month=None,start_date=None,end_date=None):
    """
    [邮件发送列表初始化]######
    函数说明 创建：乔晖 2018/10/6
    整合每月例行发送的邮件列表
    校验ags_mail_log中是否存在当月发送列表，不存在则写入，存在则跳过
    默认初始化中对status列置为0（0：邮件未发送 1：邮件已发送）
    
    [输入Parameters]:
    month string 需要导入列表的月份 如2018-10
    -------
    [返回值return]：
        返回
    """
    #第一步：校验是否有重复数据
    sql_duplicate=query("select count(send_id) as cnt from ags_mail_log where month='%s'" % (month))
    result=sql_duplicate.fetchone()
    if result['cnt']==0:
        #返回结果为0，进行导入操作
        logging.debug("准备开始导入~")
        sql="insert into ags_mail_log  (mail_id,month,name,start_date,end_date,status) " \
        "select mail_id,'%s',name,'%s','%s',0 " \
        "from ags_mail_list where valid=1"  % (month,start_date,end_date)
        sql_import=query(sql)
        logging.debug("导入完成")
        return True
    else:
        #返回结果不为0，忽略导入操作
        logging.debug("%s邮件发送列表已存在，忽略导入操作！" % (month))
        return False
    
    
    pass

def arrow(person_q2='',fleet_q1='',fleet_q3=''):
    if person_q2<=fleet_q1:
        return '↓'
    elif person_q2>=fleet_q3:
        return '↑'
    else:
        return ''
        
def __init__(self,month=None,start_date=None,end_date=None):
    #本模块未写成类，因此以下代码无效 乔晖 2018/10/16
    #df_event,df_fleet
    #报表初始化，获取前置的一些信息，以免反复读取
    print('report init')
    self.month=month
    self.start_date=start_date
    self.end_date=end_date
   
    
if __name__ == '__main__':
    df_event=export_ags_event_summary(start_date=m_start_date,end_date=m_end_date,flag_csv=0)
    df_fleet=analyze_fleet_month_df(ac_type,m_month,m_parameters) 
    report_person_v1('张颢',m_month)

    #report_fleet_v1()