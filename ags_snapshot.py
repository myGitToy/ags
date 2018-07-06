# -*- coding: utf-8 -*-
from mysql import query,query_list
import time
from datetime import datetime,timedelta
import numpy as np
from numpy import mean, ptp, var, std
import pandas as pd
def clean():
    '''
    [数据清洗]######
    函数说明 乔晖 2018/3/30
    通过检查[ags_snapshot]
    
    '''
    #去除非有效数据 2018/4/3 增加双条件判断，预估每月可以排除200条左右的非有效数据
    a=query("update ags_snapshot set ags_valid=0,match_mode=0 where (`From` is null and `To` is null) or (`From` ='' and `To` ='')")
    print("共删除%d条非有效数据：" % a.rowcount)

def match():
    '''
    [数据匹配]######
    函数说明 乔晖 2018/4/2
    ''' 
    #进行匹配，取出所有未进行匹配的数据
    a=query("select ags_id,`Date & Time`,`A/C Tail`,`Flight No`,`From`,`To` from ags_snapshot where  `A/C Tail` is not null and ags_valid is null")
    result=a.fetchall()
    count=0
    for row in result:
        #获取数据
        ags_id=row['ags_id']
        ags_datetime2=row['Date & Time']
        ags_reg=row['A/C Tail']
        #print(ags_reg)
        ags_fn=row['Flight No']
        ags_from=row['From']
        ags_to=row['To']
        #QAR数据的上传时间GMT
        ags_datetime=datetime.strptime(ags_datetime2, "%d/%m/%Y %H:%M:%S")
        #GMT转化为北京时 GMT+8
        ags_datetime=ags_datetime+timedelta(hours = 8)
        #进行mysql语句层面的匹配，规则为取前后三天的航班进行时间间隔相减，如果两者时间间隔小于一小时且数据只有一条，则认为精确匹配
        sql="select key_id,timediff('%s' ,实到) as 时间间隔 from flight_link_chn where 航班日期 between '%s' and '%s' and 机号='%s' having 时间间隔<='01:30:00' and 时间间隔>='00:00:00'" % (ags_datetime,ags_datetime-timedelta(days = 2),ags_datetime+timedelta(days = 2),ags_reg[2:6])
        b=query(sql)
        rst=b.fetchall()
        #print(sql)
        if b.rowcount==1:
            #精确匹配
            count=count+1
            for row2 in rst:
                #查询队列置0
                sql_list=[]
                #获取key_id
                key_id=row2['key_id']
                #print(ags_id,key_id)
                #1. 更新[ags_snapshot]，进行以下操作：
                #1.1 ags_valid置为1 有效
                #1.2 匹配模式为1 精确匹配
                #1.3 ags_id和key_id关联
                sql_list.append("update ags_snapshot set ags_valid=1,match_mode=1,key_id='%s' where ags_id='%s'" % (key_id,ags_id))
                #2. 更新[crew_link]中的ags_id
                sql_list.append("update crew_link set ags_id='%s' where key_id='%s'" % (ags_id,key_id))
                
            
            #执行数据库更新操作
            query_list(sql_list)
            print('[%s]success,%s' % (ags_id,count))
        
def analyze():
    '''
    [数据统计-]######
    函数说明 乔晖 2018/3/30
    列出每月指定机型的某项快照数据
    
    '''
    #查询737机型某月单项数据
    #B737=query("SELECT lnk.key_id,lnk.航班日期,ags.`RETARD_ALT (FT)` as 数据 FROM `flight_link_chn` lnk,`ags_snapshot` ags where ags.key_id=lnk.key_id and lnk.航班日期 between '2018/1/1' and '2018/3/31' and lnk.机型 IN ('73M','73L','73H','738','73E','737','73G','73A')")
    B737=query("SELECT lnk.key_id,lnk.航班日期,ags.`VRTG_MAX_LD (g)` as 数据 FROM `flight_link_chn` lnk,`ags_snapshot` ags where ags.key_id=lnk.key_id and lnk.航班日期 between '2018/1/1' and '2018/1/31' and lnk.机型 IN ('73L','73H','738','73E','737','73G','73A')")
    
    
    #B737=query("select flnk.`航班日期`,ags.`From`,ags.`To`,ags.`DIST_LDG (feet)` as 数据 from crew_link lnk,ags_snapshot ags,flight_link_chn flnk where ags.key_id=lnk.key_id and flnk.key_id=lnk.key_id and flnk.key_id=ags.key_id and lnk.姓名='乔晖'  and flnk.航班日期 between '2018/2/1' and '2018/2/28'   order by 航班日期")
    B737_result=B737.fetchall()
    B737_list=[]
    for B737_row in B737_result:
        B737_list.append(float(B737_row['数据']))
    print("B737 3月份平飘距离Q3指标为：",np.percentile(B737_list,75))
    #平均值
    print("平均值：",mean(B737_list))
    #标准差
    print("标准差：",std(B737_list,ddof=1))
    #变异系数
    print("变异系数：",std(B737_list,ddof=1)/mean(B737_list))
    print("中位数，Q3，标准差，变异系数：",mean(B737_list),np.percentile(B737_list,75),std(B737_list,ddof=1),std(B737_list,ddof=1)/mean(B737_list))
    a=query("select flnk.`航班日期`,ags.`From`,ags.`To`,ags.`DIST_LDG (feet)` as 数据 from crew_link lnk,ags_snapshot ags,flight_link_chn flnk where ags.key_id=lnk.key_id and flnk.key_id=lnk.key_id and flnk.key_id=ags.key_id and lnk.姓名='章磊'  and date_format(flnk.航班日期,'%Y-%m')='2018-03'")
    print("共%s条有效数据：" % a.rowcount)
    result=a.fetchall()
    list=[]
    #print(result)
    #print(result)
    #print(11)
    #print(np.percentile(result,50,axis = 1))
    for row in result:
        #data=row['数据']
        #数据库里是字符串，这里必须转换成浮点数
        list.append(float(row['数据']))
    #print(list)
    #d=np.array([list])
    #print(d)
    #print(np.percentile(d,50))
    #print(list)
    #ls=[10, 7, 4, 3, 2, 1]
    
    #d = np.array([list])
    print("某人的中位数为：",np.percentile(list,50))
    #print(list)
    #print(ls)
    #array=np.array(list)
    #print(array)
    #arr=np.array((list))
    #c=np.array([[30,65,70],[80,95,10],[50,90,60]])  
    #print(np.median(c))
    #quartile_75=np.median([list])
    #print(quartile_75)
    #np.percentile(list,95)
    
    #print(np.percentile(a['数据'], 25))
def analyze_person_month(name=None,start='',end='',column=''):
    '''
    [数据统计-列出某人指定日期间的某项快照数据全部结果]######
    函数说明 乔晖 2018/4/22
    [输入Parameters]:
        name:string 姓名 e.g. 乔晖
        start:string 开始日期 format：YYYY/MM/DD
        end:string 结束日期 format：YYYY/MM/DD
        column:string 具体分析某一个快照，如平飘距离 `DIST_LDG (feet)`
    -------
    [返回值return]：
    DataFrame
          date 交易日期 (index)
          open 开盘价
          high  最高价
          close 收盘价
          low 最低价
          volume 成交量
          amount 成交额
          turnoverratio 换手率
          code 股票代码
    '''
def analyze_person_monthlist(name=None,monthlist='',column=''):
    '''
    [数据统计-列出某人按月分分布的某项快照数据全部结果]######
    函数说明 乔晖 2018/4/22
    [输入Parameters]:
        name:string 姓名 e.g. 乔晖
        monthlist:list 要分析的月份列表 如['2018-01','2018-02','2018-03']
        column:string 具体分析某一个快照，如平飘距离 `DIST_LDG (feet)`
    -------
    [返回值return]：

    ''' 
    #打印表头
    #print("姓名,月度,航班量,中位数,Q3值,标准差,变异系数")
    for month in monthlist:
        a=query("select flnk.`航班日期`,ags.`From`,ags.`To`,ags.%s as 数据 from crew_link lnk,ags_snapshot ags,flight_link_chn flnk where ags.key_id=lnk.key_id and flnk.key_id=lnk.key_id and flnk.key_id=ags.key_id and lnk.姓名='%s'  and date_format(flnk.航班日期,'%%Y-%%m')='%s'" % (column,name,month))
        #print("共%s条有效数据：" % a.rowcount)
        if a.rowcount>0:
            
            result=a.fetchall()
            list=[]
            for row in result:
            #data=row['数据']
            #数据库里是字符串，这里必须转换成浮点数
                list.append(float(row['数据'])) 
            #输出结果
            Q2=np.percentile(list,50)
            Q3=np.percentile(list,75)
            m_std=std(list,ddof=1)
            m_cv=std(list,ddof=1)/mean(list)
            #print(name,month,Q2,Q3,m_std,m_cv)
            print("%s,%s,%d,%f,%f,%f,%f" % (name,month,len(list),Q2,Q3,m_std,m_cv))
        else:
            pass
#数据清洗
clean()
#数据匹配
match()
analyze()
analyze_person_monthlist(name='唐驰',monthlist=['2018-01','2018-02','2018-03','2018-04'],column='`DIST_LDG (feet)`')
#analyze_person_monthlist(name='刘富元',monthlist=['2018-01','2018-02','2018-03'],column='`DIST_LDG (feet)`')
#analyze_person_monthlist(name='刘长家',monthlist=['2018-01','2018-02','2018-03'],column='`DIST_LDG (feet)`')
#analyze_person_monthlist(name='张T',monthlist=['2018-01','2018-02','2018-03'],column='`DIST_LDG (feet)`')
