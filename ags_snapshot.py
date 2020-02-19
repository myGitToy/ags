# -*- coding: utf-8 -*-
from profile.mysql import query,query_list
from profile.mysql_df import query_df
import time
from datetime import datetime,timedelta
import numpy as np
from numpy import mean, ptp, var, std
import pandas as pd
from  profile.setup import setup_snapshot
#from ags_mail import mail
def clean():
    """
    [数据清洗]######
    函数说明 乔晖 2018/3/30
    通过检查[ags_snapshot]
    
    """
    #去除非有效数据 2018/4/3 增加双条件判断，预估每月可以排除200条左右的非有效数据
    a=query("update ags_snapshot set ags_valid=0,match_mode=0 where (`From` is null and `To` is null) or (`From` ='' and `To` ='')")
    print("共删除%d条非有效数据：" % a.rowcount)

def match():
    """
    [数据匹配]######
    函数说明 乔晖 2018/4/2
    """ 
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
        
def query2(ac_type='',monthlist='',column=''):
    """
    [数据统计-列出某人指定日期间的某项快照数据全部结果]######
    函数说明 乔晖 2018/4/22
    [输入Parameters]:
        ac_type:string 机型 e.g. 可以是机型列表'B737-700','B737-800'
        start:string 开始日期 format：YYYY/MM/DD
        end:string 结束日期 format：YYYY/MM/DD
        column:string 具体分析某一个快照，如平飘距离 `DIST_LDG (feet)`
    -------
    [返回值return]
    [数据统计-]######
    函数说明 乔晖 2018/3/30
    列出每月指定机型的某项快照数据
    
    """
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
def analyze_person_month_df(name=None,month='',parameter_list=''):
    """
    [数据统计-列出某人某月指定快照数据全部结果]######
    函数说明 乔晖 2018/8/9
    [输入Parameters]:
        name:姓名
        ac_type:string 机队列表
        month:要分析的月份如'2018-07'
        parameter_list:list 要分析的快照列表，如平飘距离和着陆载荷 ['`VRTG_MAX_LD (g)`','`DIST_LDG (feet)`'] 注意：一定要有``否则会出错

    -------
    [返回值return]：
    返回本人当月参数的Q0-Q4值
        修改说明：
    2018/8/9
    1、函数初始化
    2、赋值方式由set_value更改为at，原因是未来pandas可能不再支持set_value，提前做好准备
    """ 
    #初始化，赋空值
    df2=pd.DataFrame(index=parameter_list,columns=['count','Q0','Q1','Q2','Q3','Q4'])
    for column in parameter_list:
        sql="select flnk.`航班日期`,ags.`From`,ags.`To`,ags.%s as 数据 " \
        "from crew_link lnk,ags_snapshot ags,flight_link_chn flnk " \
        "where ags.key_id=lnk.key_id and flnk.key_id=lnk.key_id and flnk.key_id=ags.key_id " \
        "and lnk.姓名='%s' and date_format(flnk.航班日期,'%%Y-%%m')='%s'" % (column,name,month)
        df=query_df(sql)
        #数据类型转换 text->float
        df_float=df['数据'].astype('float')
        count=df_float.count()
        Q0=df_float.quantile(0)
        Q1=df_float.quantile(0.25)
        Q2=df_float.quantile(0.50)
        Q3=df_float.quantile(0.75)
        Q4=df_float.quantile(1.0)

        #将参数写回dataframe
        df2.at[column,'count']=count
        df2.at[column,'Q0']=Q0
        df2.at[column,'Q1']=Q1
        df2.at[column,'Q2']=Q2
        df2.at[column,'Q3']=Q3
        df2.at[column,'Q4']=Q4
        
    return df2
    
def analyze_person_statics(month=None,parameter=None):
    """
    [数据统计-列出某月指定快照数据全部结果 汇总后进行统计学描述分析]######
    函数说明 乔晖 2018/12/21
    [输入Parameters]:
        month:要分析的月份如'2018-07'
        parameter_list:list 要分析的快照列表，如平飘距离和着陆载荷 ['`VRTG_MAX_LD (g)`','`DIST_LDG (feet)`'] 注意：一定要有``否则会出错

    -------
    [返回值return]：
    返回本人当月参数的Q0-Q4值
        修改说明：
    2018/12/21
    1、函数初始化
    2、赋值方式由set_value更改为at，原因是未来pandas可能不再支持set_value，提前做好准备
    """ 
    #初始化，赋空值
    #df2=pd.DataFrame(index=parameter_list,columns=['count','Q0','Q1','Q2','Q3','Q4'])

    sql="select flnk.`航班日期`,lnk.姓名,ags.`From`,ags.`To`,ags.%s as 数据 " \
    "from crew_link lnk,ags_snapshot ags,flight_link_chn flnk " \
    "where ags.key_id=lnk.key_id and flnk.key_id=lnk.key_id and flnk.key_id=ags.key_id " \
    "and date_format(flnk.航班日期,'%%Y-%%m')='%s'" % (parameter,month)
    df=query_df(sql)
    print(df)
    #数据类型转换 text->float
    df['数据'] = df['数据'].astype('float')
    #数据聚合
    df=df.groupby('姓名').mean()
    #筛选
    df=df.loc[df['数据']>=2000]
    #排序
    print(df.sort_values(by=['数据'],ascending=False))
    #df_name=df['数据'].groupby(df['姓名'])
    #mean=df_name.mean
    #print(mean)
    

    #数据聚合

    return df

def analyze_person_statics_2(month=None):
    """
    [数据统计-列出某月指定快照数据全部结果 汇总后进行统计学描述分析]######
    函数说明 乔晖 2018/12/21
    [输入Parameters]:
        month:要分析的月份如'2018-07'
        parameter_list:list 要分析的快照列表，如平飘距离和着陆载荷 ['`VRTG_MAX_LD (g)`','`DIST_LDG (feet)`'] 注意：一定要有``否则会出错

    -------
    [返回值return]：
    返回本人当月参数的Q0-Q4值
        修改说明：
    2018/12/21
    1、函数初始化
    2、赋值方式由set_value更改为at，原因是未来pandas可能不再支持set_value，提前做好准备
    """ 
    #初始化，赋空值
    #df2=pd.DataFrame(index=parameter_list,columns=['count','Q0','Q1','Q2','Q3','Q4'])

    sql="select flnk.`航班日期`,lnk.姓名,ags.`From`,ags.`To`,ags.`DIST_LDG (feet)` as 着陆距离,ags.`VRTG_MAX_LD (g)` as 着陆载荷 " \
    "from crew_link lnk,ags_snapshot ags,flight_link_chn flnk " \
    "where ags.key_id=lnk.key_id and flnk.key_id=lnk.key_id and flnk.key_id=ags.key_id " \
    "and date_format(flnk.航班日期,'%%Y-%%m')='%s'" % (month)
    df=query_df(sql)
    #print(df)
    #数据类型转换 text->float
    df['着陆距离'] = df['着陆距离'].astype('float')
    df['着陆载荷'] = df['着陆载荷'].astype('float')
    #数据聚合
    df=df.groupby('姓名').mean()
    #筛选
    #df=df.loc[df['着陆距离']>=2000]
    #排序
    print(df.sort_values(by=['着陆载荷'],ascending=False))
    #df_name=df['数据'].groupby(df['姓名'])
    #mean=df_name.mean
    #print(df)
    

    #数据聚合

    return df

def analyze_qar_download_status(tail_list='',start='',end='',fleet_list='73M'):
    """
    [数据统计-列出每月航班数和快照数的差值，以筛选QAR源头未导入的航班]######
    函数说明 乔晖 2018/7/21
    目前主要用于统计MAX航班
    [输入Parameters]:
        max_tail_list:list 机尾号 如'B-1379','B-1381','B-1382','B-1259','B-1260','B-1261'
        fleet_list:list 机型 如'73M'
        start:string 开始日期 format：YYYY/MM/DD
        end:string 结束日期 format：YYYY/MM/DD
    -------
    [返回值return]：
    dataframe 符合条件的航班列表
    """
    sql="select key_id,联线号,航班,航程,航班日期,航班号,机型,机号,起飞机场,降落机场 " \
    "from flight_link_chn where key_id not in " \
    "(select key_id from ags_snapshot where `A/C Tail` IN "\
    "(%s) and key_id is not null) and " \
    "航班日期 between '%s' and '%s' and 机型='%s' and 航班号 not in ('FMMAX')" % (tail_list,start,end,fleet_list)
    df=query_df(sql)
    df.to_csv('~/environment/ags/export/QAR_notmatched.csv',encoding='utf_8_sig')
    return df

def analyze_fleet_monthlist_df(ac_type='',monthlist='',columnlist='',print_head=True):
    """
    [数据统计-列出机队按月分布的快照数据结果]######
    函数说明 创建：乔晖 2018/7/8
    [输入Parameters]:
        ac_type:string 机队列表
        monthlist:list 要分析的月份列表 如['2018-01','2018-02','2018-03']
        columnlist:list 要分析的快照列表，如平飘距离和着陆载荷 ['`VRTG_MAX_LD (g)`','`DIST_LDG (feet)`'] 注意：一定要有``否则会出错
        print_head:boolean 是否打印表头，默认为True
    -------
    [返回值return]：
    将结果打印出来，无返回值
    
    
    修改说明：
    2018/7/9
    1. 修正sql语句，原先移植版本从事件人员角度分析，会有重复，因此采用distinct消除重复。目前版本删除crew_link表，取消人员查询，因此月度查询速度从40秒优化至4.3秒
    
    2018/7/12
    1.修改输入函数，原先一次只能处理一种快照数据，现输入项为快照列表，可以输入多种快照数据
    2.数据处理过程使用df，理论上速度更快
    
    """ 
    #打印表头
    if print_head==True:
        print("机队,字段,月份,航班快照量,Q1值,中位数,Q3值,Q90,标准差,变异系数,平均值")
    
    for column in columnlist:
        for month in monthlist:
            sql="select ags.ags_id,flnk.`航班日期`,ags.`From`,ags.`To`,ags.%s as 数据 from ags_snapshot" \
            " ags,flight_link_chn flnk where flnk.key_id=ags.key_id and " \
            "date_format(flnk.航班日期,'%%Y-%%m')='%s' and flnk.机型 IN (%s)" % (column,month,ac_type)
            df=query_df(sql)
            #数据类型转换 text->float
            df_float=df['数据'].astype('float')
            count=df_float.count()
            Q1=df_float.quantile(0.25)
            Q2=df_float.quantile(0.50)
            Q3=df_float.quantile(0.75)
            #注：Q90在个人数据中没有意义，不做统计
            Q90=df_float.quantile(0.9)
            m_std=df_float.std()
            #m_std=std(list,ddof=1)
            m_mean=df_float.mean()
            m_cv=m_std/m_mean
            print("%s,%s,%s,%d,%f,%f,%f,%f,%f,%f,%f" % ("B737机队",column,month,count,Q1,Q2,Q3,Q90,m_std,m_cv,m_mean))
            #print(stats1(df_float))
            
def analyze_fleet_month_df(ac_type='',month='',parameter_list=''):
    """
    [数据统计-列出机队按月分布的快照数据结果 dataframe格式]######
    函数说明 创建：乔晖 2018/8/9
    [输入Parameters]:
        ac_type:string 机队列表
        month:要分析的月份如'2018-07'
        parameter_list:list 要分析的快照列表，如平飘距离和着陆载荷 ['`VRTG_MAX_LD (g)`','`DIST_LDG (feet)`'] 注意：一定要有``否则会出错

    -------
    [返回值return]：
    返回机队当月指定参数的Q0-Q4值
    
    
    修改说明：
    2018/8/9
    1、函数初始化
    2、赋值方式由set_value更改为at，原因是未来pandas可能不再支持set_value，提前做好准备
    """ 
    #初始化，赋空值
    df2=pd.DataFrame(index=parameter_list,columns=['count','Q0','Q1','Q2','Q3','Q4'])
    for column in parameter_list:
        sql="select ags.ags_id,flnk.`航班日期`,ags.`From`,ags.`To`,ags.%s as 数据 from ags_snapshot" \
        " ags,flight_link_chn flnk where flnk.key_id=ags.key_id and " \
        "date_format(flnk.航班日期,'%%Y-%%m')='%s' and flnk.机型 IN (%s)" % (column,month,ac_type)
        df=query_df(sql)
        #数据类型转换 text->float
        df_float=df['数据'].astype('float')
        count=df_float.count()
        Q0=df_float.quantile(0)
        Q1=df_float.quantile(0.25)
        Q2=df_float.quantile(0.50)
        Q3=df_float.quantile(0.75)
        Q4=df_float.quantile(1.0)

        #将参数写回dataframe
        df2.at[column,'count']=count
        df2.at[column,'Q0']=Q0
        df2.at[column,'Q1']=Q1
        df2.at[column,'Q2']=Q2
        df2.at[column,'Q3']=Q3
        df2.at[column,'Q4']=Q4
        
    return df2


            
def stats1(x):
    return pd.Series([x.count(),x.min(),x.idxmin(),
               x.quantile(.25),x.median(),
               x.quantile(.75),x.mean(),
               x.max(),x.idxmax(),
               x.mad(),x.var(),
               x.std(),x.skew(),x.kurt()],
              index = ['Count','Min','Whicn_Min',
                       'Q1','Median','Q3','Mean',
                       'Max','Which_Max','Mad',
                       'Var','Std','Skew','Kurt'])
def analyze_fleet_monthlist_CL(ac_type='',monthlist='',columnlist=''):
    """
    [数据统计-列出机队按月分布的快照数据结果 【经典分析方法】######
    函数说明 创建：乔晖 2018/7/8
    此为第一版统计学分析，使用经典的mysql导出数据列表，将字段信息写入list列表，再通过对list进行统计学分析得到结果
    后续已被analyze_fleet_monthlist_df替换
    【本版本已不再维护】
    
    [输入Parameters]:
        ac_type:string 机队列表
        monthlist:list 要分析的月份列表 如['2018-01','2018-02','2018-03']
        columnlist:list 要分析的快照列表，如平飘距离和着陆载荷 ['`VRTG_MAX_LD (g)`','`DIST_LDG (feet)`'] 注意：一定要有``否则会出错
    -------
    [返回值return]：
    修改说明：
    2018/7/9
    1. 修正sql语句，原先移植版本从事件人员角度分析，会有重复，因此采用distinct消除重复。目前版本删除crew_link表，取消人员查询，因此月度查询速度从40秒优化至4.3秒
    
    2018/7/12
    1.修改输入函数，原先一次只能处理一种快照数据，现输入项为快照列表，可以输入多���快照数据
    
    """ 
    #打印表头
    print("机队,字段,月份,航班快照量,Q1值,中位数,Q3值,Q90,标准差,变异系数,平均值")
    for column in columnlist:
        for month in monthlist:
            sql="select ags.ags_id,flnk.`航班日期`,ags.`From`,ags.`To`,ags.%s as 数据 from " \
            "ags_snapshot ags,flight_link_chn flnk "\
            "where flnk.key_id=ags.key_id and "\
            "date_format(flnk.航班日期,'%%Y-%%m')='%s' and flnk.机型 IN (%s)" % (column,month,ac_type)
            #print(sql)
            a=query(sql)
            if a.rowcount>0:
                result=a.fetchall()
                list=[]
                for row in result:
                #data=row['数据']
                #数据库里是字符串，这里必须转换成浮点数
                    list.append(float(row['数据'])) 
                #输出结果
                print(len(list))
                Q1=np.percentile(list,25)
                Q2=np.percentile(list,50)
                Q3=np.percentile(list,75)
                #注：Q90在个人数据中没有意义，不做统计
                Q90=np.percentile(list,90)
                m_std=std(list,ddof=1)
                m_mean=mean(list)
                m_cv=std(list,ddof=1)/m_mean
                #print(name,month,Q2,Q3,m_std,m_cv)
                #清空list列表
                list=[]
                print("%s,%s,%s,%d,%f,%f,%f,%f,%f,%f,%f" % ("B737机队",column,month,len(list),Q1,Q2,Q3,Q90,m_std,m_cv,m_mean))
            else:
                pass
        pass
    """
    [数据统计-列出机队按月分布的快照数据结果]######
    函数说明 创建：乔晖 2018/7/8

    
    [输入Parameters]:
        ac_type:string 机队列表
        monthlist:list 要分析的月份列表 如['2018-01','2018-02','2018-03']
        columnlist:list 要分析的快照列表，如平飘距离和着陆载荷 ['`VRTG_MAX_LD (g)`','`DIST_LDG (feet)`'] 注意：一定要有``否则会出错
    -------
    [返回值return]：
    修改说明：
    2018/7/9
    1. 修正sql语句，原先移植版本从事件人员角度分析，会有重复，因此采用distinct消除重复。目前版本删除crew_link表，取消人员查询，因此月度查询速度从40秒优化至4.3秒
    
    2018/7/12
    1.修改输入函数，原先一次只能处理一种快照数据，现输入项为快照列表，可以输入多种快照数据
    2.数据处理过程使用df，理论上速度更快
    
    """ 
    #打印表头
    print("机队,字段,月份,航班快照量,Q1值,中位数,Q3值,Q90,标准差,变异系数,平均值")
    for column in columnlist:
        for month in monthlist:
            sql="select ags.ags_id,flnk.`航班日期`,ags.`From`,ags.`To`,ags.%s as 数据 " \
            "from ags_snapshot ags,flight_link_chn flnk " \
            "where flnk.key_id=ags.key_id and " \
            "date_format(flnk.航班日期,'%%Y-%%m')='%s' and flnk.机型 IN (%s)" % (column,month,ac_type)
            df=query_df(sql)
            #数据类型转换 text->float
            df_float=df['数据'].astype('float')
            count=df_float.count()
            Q1=df_float.quantile(0.25)
            Q2=df_float.quantile(0.50)
            Q3=df_float.quantile(0.75)
            #注：Q90在个人数据中没有意义，不做统计
            Q90=df_float.quantile(0.9)
            m_std=df_float.std()
            #m_std=std(list,ddof=1)
            m_mean=df_float.mean()
            m_cv=m_std/m_mean
            print("%s,%s,%s,%d,%f,%f,%f,%f,%f,%f,%f" % ("B737机队",column,month,count,Q1,Q2,Q3,Q90,m_std,m_cv,m_mean))

#数据清洗
#clean()
#数据匹配
#match()
#analyze()

#数据分析
#初始化月度列表
m_list=['2018-01','2018-02','2018-03','2018-04','2018-05','2018-06','2018-07','2018-08']
#m_list=['2018-06']
#初始化落地监控项目
m_landing=setup_snapshot.event_name_landing
#初始化SOP监控项目
m_event_name_sop=setup_snapshot.event_name_sop
ac_type="'73M','73L','73H','738','73E','737','73G','73A'"

#分析机队落地快照
#analyze_fleet_monthlist_df(ac_type,m_list,m_landing,print_head=True)

#analyze_fleet_month_dfdf(ac_type,'2018-07',m_landing)
#print(analyze_person_month_df('乔晖','2018-07',m_landing))

#analyze_fleet_monthlist(ac_type,m_list,['`ROLL_MAX_BL100 (deg)`'])
#分析机队SOP快照
#analyze_fleet_monthlist(ac_type,m_list,m_event_name_sop)



"""



analyze_person_monthlist(name='唐驰',monthlist=m_list,column='`DIST_LDG (feet)`')
analyze_person_monthlist(name='虞斌华',monthlist=m_list,column='`DIST_LDG (feet)`')
analyze_person_monthlist(name='乔晖',monthlist=m_list,column='`DIST_LDG (feet)`')
analyze_person_monthlist(name='刘富元',monthlist=m_list,column='`DIST_LDG (feet)`')
analyze_person_month_df(name='刘长家',monthlist=m_list,column='`DIST_LDG (feet)`')
analyze_person_monthlist(name='章磊',monthlist=m_list,column='`DIST_LDG (feet)`')
"""
#b=mail(name='乔晖',month='2018-08',email='g.huiqiao@aliyun.com',start_date='2018-08-01',end_date='2018-08-31')
#print(b.username)
#print(b.send_test())
#b.send()

def __init__(self):
    pass

if __name__ == '__main__':
    analyze_person_statics_2(month='2018-11')
    pass