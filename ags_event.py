# -*- coding: utf-8 -*-
"""
文档说明【模块】：
    QAR1-4级事件处理
    主要完成事件匹配和事件数据输出，为QAR分析提供csv格式的raw_data
    引用规范：请使用下列语句
        import ags_event as event

基本框架：
    match   QAR事件和人名匹配，主要用于被ags.mysql_import_data调用
    export_ags_event_summary    导出月度（指定日期间）QAR事件率汇总表（每人一行，行数等同于飞行员人数）
    export_ags_event_summary_dep    将export_ags_event_summary的结果拆分成分部后导出到~/export 
                                    原计划这个功能将通过邮件发送拆分后的事件给各分部，现在搁置，由于担心分部滥用数据
    export_ags_event_person     导出月度（指定日期间）QAR所有事件含人名的表格（每月约7万条）

版本信息：
    version 0.1
    乔晖 2018/9/16

修改日志：
    2018/9/16
        1. 在老版本的基础上建立基本框架，逐渐按照要求进行移植
    2018/8/13
        1. 【事件】修正二三级事件率统计中的一处错误，事件率为正常的2倍，通过删除排除事件表（exp表）实现
        2. 【事件】二三级事件率输出格式调整为小数点后5位
    2018/7/6
        1. 【事件】修改[export_ags_event_summary_dep]函数的输出路径至profile文件夹
    2018/6/16
        1. 【事件】增加[export_ags_event_summary_dep]函数，将分部数据输出至csv格式文件中
    2018/6/12
        1. 【事件】增加[export_ags_event_person]函数，输出月度所有飞行员各类事件（效仿原segem程序）
        2. 【事件】上述函数的输出结果将每月汇总至QAR_Rawdata_ags中，为各类月报分析提供数据
    2018/6/11
        1. 【事件】完成QAR事件与人员名字的匹配，目前匹配率达到99.6%，基本达到预想目标
        2. 【事件】增加[export_ags_event_summary]函数，输出月度各分部人员QAR二三级事件率（效仿原segem程序）

TODO LIST：
    事件导出需要增加4级事件信息
"""
import time
import xlwt
import os
import sys
import pandas as pd
import numpy as np
from profile.mysql import query,query_list
from profile.mysql_df import query_df
from profile.setup import setup_dep_name as dep
from profile.setup import setup
from datetime import datetime,timedelta
from numpy import mean, ptp, var, std

def match():
    """
    [数据匹配]######
    函数说明 乔晖 2018/6/11
    将每月ags_event导入数据库后执行该程序，程序会进行自动数据匹配，通过和crew_link_chn表进行配合，得到事件和机组姓名的匹配
    每月数据约30000条，使用commercail库，不包含本场和发动机试车数据
    包含master caution数据，后续分析中会使用[ags_event_exception]排除无关数据
    """ 
    #进行匹配，取出所有未进行匹配的数据
    a=query("select event_id,`Flight Date`,`A/C Tail`,`Flight No`,`Departure Airport`,`Arrival Airport` " \
    "from ags_event where `A/C Tail` is not null and ags_valid is null")
    result=a.fetchall()
    count=0
    for row in result:
        #获取数据
        event_id=row['event_id']
        ags_datetime=row['Flight Date']
        ags_reg=row['A/C Tail']
        #print(ags_reg)
        ags_fn=row['Flight No']
        ags_from=row['Departure Airport']
        ags_to=row['Arrival Airport']
        #QAR事件中的时间为实际滑出时间
        #ags_datetime=datetime.strptime(ags_datetime2, "%d/%m/%Y %H:%M:%S")
        #GMT转化为北京时 GMT+8
        ags_datetime=ags_datetime+timedelta(hours = 8)
        #进行mysql语句层面的匹配，规则为取前后三天的航班进行时间间隔相减，如果两者时间间隔小于一小时且数据只有一条，则认为精确匹配
        #sql="select key_id,航班号,timediff('%s' ,实飞) as 时间间隔 from flight_link_chn where 航班日期 between '%s' and '%s' and 机号='%s' having 时间间隔<='-01:30:00'" % (ags_datetime,ags_datetime-timedelta(days = 1),ags_datetime+timedelta(days =1),ags_reg[2:6])
        sql="select key_id,航班号,timediff('%s' ,实飞) as 时间间隔 from flight_link_chn " \
        "where 航班日期 between '%s' and '%s' and 机号='%s' having 时间间隔<='-01:30:00'" % (ags_datetime,ags_datetime-timedelta(days = 2),ags_datetime+timedelta(days =2),ags_reg[2:6])
        b=query(sql)
        rst=b.fetchall()
        #print(sql)
        if b.rowcount==1:
            #精确匹配
            
            count=count+1
            for row2 in rst:
                fn=row2['航班号']
                print('event：%s|机组排版系：%s' % (ags_fn,fn))
                #查询队列置0
                sql_list=[]
                #获取key_id
                key_id=row2['key_id']
                #print(ags_id,key_id)
                #1. 更新[ags_event]，进行以下操作：
                #1.1 ags_valid置为1 有效
                #1.2 匹配模式为1 精确匹配
                #1.3 ags_id和key_id关联
                sql_list.append("update ags_event set ags_valid=1,match_mode=1,key_id='%s' where event_id='%s'" % (key_id,event_id))
                #2. 更新[crew_link]中的ags_id
                #sql_list.append("update crew_link set ags_id='%s' where key_id='%s'" % (ags_id,key_id))
                
            
            #执行数据库更新操作
            query_list(sql_list)
            print('[%s]success,%s' % (event_id,count))

def export_ags_event_summary(start_date='',end_date='',flag_csv=1):
    """
    [导出事件汇总表 按月]######
    函数说明 乔晖 2018/6/11
    将每月ags_event导出成事件汇总表 summary
    包含每个人的航班数量，2、3、4级事件的数量和时间率
    需要排除ags_event_exception
    输入：
    start_date：开始日期
    end_date：结束日期
    flag_csv：是否输出csv格式数据 1为输出 0为不输出
    输出：
        dataframe格式汇总表
        写入 ~/ags/event_summary.csv
    """ 
    ##########姓名 分部 技术授权  航段数
    df_count=query_df("select crew.姓名,dep.部门,dep.航班机型排班授权,count(crew.姓名) as 航段数 from flight_link_chn flt,crew_link crew,flight_dep dep where flt.key_id=crew.key_id and crew.姓名=dep.姓名 and crew.姓名 !='' and flt.航班日期 between '%s' and '%s'  group by crew.姓名 " % (start_date,end_date))
    #重构索引
    df_count.set_index(['姓名'], inplace = True) 

    ##########姓名 二级事件数量
    sql="select lnk.姓名,count(lnk.姓名) as 二级事件数量 " \
        "from ags_event ags,crew_link lnk " \
        "where lnk.key_id=ags.key_id and ags.`Severity Class No`=2 " \
        "and ags.`Flight Date` between '%s' and '%s'" \
        "and ags.`Event Short Name` not in (select `Event Short Name` from ags_event_exception where `Severity Class No`=2) " \
        "group by lnk.姓名 order by ags.`Flight date`" % (start_date,end_date)
    df_event2=query_df(sql)
    #重构索引
    df_event2.set_index(['姓名'], inplace = True) 
    #获取二级事件率
    df_event2['二级事件率']=round(df_event2['二级事件数量']/df_count['航段数'],5)

    ##########姓名 三级事件数量
    sql="select lnk.姓名,count(lnk.姓名) as 三级事件数量 " \
        "from ags_event ags,crew_link lnk " \
        "where lnk.key_id=ags.key_id and ags.`Severity Class No`=3 " \
        "and ags.`Flight Date` between '%s' and '%s'" \
        "and ags.`Event Short Name` not in (select `Event Short Name` from ags_event_exception where `Severity Class No`=3) " \
        "group by lnk.姓名 order by ags.`Flight date`" % (start_date,end_date)
    df_event3=query_df(sql)
    #重构索引
    df_event3.set_index(['姓名'], inplace = True) 
    #获取二级事件率
    df_event3['三级事件率']=round(df_event3['三级事件数量']/df_count['航段数'],5)
    
    #两个dataframe合并
    df_new=pd.concat([df_count,df_event2,df_event3], axis=1)
    #df_new.set_index(['姓名'], inplace = True) 
    #检查去重(如果去重会少很多行，总数应该811行，去重后为739行)
    #df_new = df_new.drop_duplicates() 
    #按照索引[日期]进行排序，升序
    #print(df_new.sort_index(ascending = True))
    #重命名列
    
    df_new.columns=['部门','航班机型排班授权','航段数','二级事件数量','二级事件率','三级事件数量','三级事件率']
    #df_new=df_new.rename(index={1: '姓名'})
    #print(df_new)
    #结果输出至excel
    if flag_csv==1:
       df_new.to_csv('~/environment/ags/%s_%s_event_summary.csv' % (start_date,end_date),encoding='utf_8_sig') 
    
    return df_new

def export_ags_event_summary_dep(start_date='',end_date='',month_string=''):
    """
    [导出事件汇总表 按月按分部]######
    函数说明 乔晖 2018/6/16
    将每月ags_event导出成事件汇总表 summary 拆分成各分部
    写入路径 /ags/export/%sQAR_monthly_summary_%s.csv
    包含每个人的航班数量，2、3、4级事件的数量和事件率
    需要排除ags_event_exception
    
    输入：
        month_string：导出当月的月份或其他任意值，用于拼接文件名的，例如201808
    """
    #获取事件
    df= export_ags_event_summary(start_date,end_date,flag_csv=0)
    #按部门筛选并输出，历遍词典
    dep_list=dep.dep_list737_dict
    for lst in dep_list:
        dep_name=lst
        dep_abbr=dep_list[lst]
        df_dep=df[(df['部门']==dep_name)]
        #print(df_dep)
        df_dep.to_csv('%s/ags/export/%sQAR_monthly_summary_%s.csv' % (os.getcwd(),dep_abbr,month_string),encoding='utf_8_sig')
        #生成EXCEL格式文件
        #file_path ='~/environment/RDS/QAR_monthly_summary_%s.xlsx' % (month_string)
        #writer = pd.ExcelWriter(file_path)
        #df_dep.to_excel(writer,  index=False,encoding='utf-8',sheet_name='Sheet')
        #writer.save()

def export_ags_event_person(start_date='',end_date='',flag_csv=1):
    """
    [导出事件详细数据 按月]######
    函数说明 乔晖 2018/6/12
    将每月ags_event导出成详细表格 event_detail
    不排除ags_event_exception
    每月数据约8万条
    输入：
    start_date：开始日期
    end_date：结束日期
    flag_csv：是否输出csv格式数据 1为输出 0为不输出
    """ 
    sql="select ags.event_id,lnk.key_id,lnk.ags_id,lnk.姓名,lnk.机上岗位,lnk.技术授权,lnk.责任机长标识,DATE_FORMAT(ags.`Flight Date`, '%%Y-%%m-%%d') as 航班日期,ags.`A/C Type`,ags.`A/C Tail`,ags.`Flight No`,ags.`Departure Airport`,ags.`Arrival Airport`,ags.`Event Short Name`,ags.`Maximum Value`,ags.`Severity Class No`,ags.`Event Validity` " \
        "from ags_event ags,crew_link lnk " \
        "where lnk.key_id=ags.key_id and ags.`Flight Date` between '%s' and '%s'" % (start_date,end_date)
    df_detail=query_df(sql)
    #重构索引
    df_detail.set_index(['event_id'], inplace = True) 
     #结果输出至excel
    if flag_csv==1:
        df_detail.to_csv('%s/ags/%s_%s_event_person.csv' % (os.getcwd(),start_date,end_date),encoding='utf_8_sig')


#路径设置
#print(os.getcwd())
#print(sys.path[0])
def __init__(self):
    print("ags event init")
    pass

if __name__ == '__main__':
    pass