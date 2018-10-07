# -*- coding: utf-8 -*-
from profile.mysql import query
import time
from datetime import datetime

def import_data():
    #导入数据
    #通过比较flight_delay和flight_link_chn两张表的数据异同，将[flight_link_chn]中的新增数据导入[flight_delay]
    #新导入的数据默认valid为null，因此在caculate_time函数中，通过检查valid值来确定哪些数据需要计算，哪些已经计算过了
    sql="insert into flight_delay (key_id) (select A.key_id from flight_link_chn A where not exists (select B.key_id from flight_delay B where B.key_id=A.key_id))"
    a=query(sql)
    
    """
    #这里的数据是先前的测试内容，注释掉
    with profile.mysql() as cursor:
        # 左连接查询
        r = cursor.execute("SELECT * FROM flight_link_chn where 航班日期='2017-10-1'")
        result = cursor.fetchall()
        print(result)
        for rows in result:
            print(rows['航班日期'],rows['航程'])
    """
            
def caculate_time():
    ######【计算飞行时间】######
    sql="SELECT dly.key_id,lnk.`航班日期`,lnk.`航班号`,lnk.`计飞`,lnk.`实飞`,lnk.`计到`,lnk.`实到`,lnk.`飞行时间` FROM flight_delay AS dly ,flight_link_chn AS lnk WHERE dly.key_id = lnk.key_id AND dly.`valid` IS NULL"
    a=query(sql)
    print("本次查询总共返回结果数：",a.rowcount)
    result=a.fetchall()
    for row in result:
        #下面两行被注释的原因是从数据库内相关字段获取的时间字符串直接就是时间格式，strptime只接受字符串形式，所以转换失败。字段可以直接拿来做时间使用
        #d1=datetime.strptime(row['计飞'], "%Y-%m-%d %H:%M:%S")
        #d2=datetime.strptime(row['实飞'], "%Y-%m-%d %H:%M:%S")
        #获取数据
        d1=row['计飞']
        d2=row['实飞']
        a1=row['计到']
        a2=row['实到']
        flt_time_str=row['飞行时间']
        key_id=row['key_id']
        ##判断数据是否有效
        if d1 is None or d2 is None or a1 is None or a2 is None:
            #四个时间段其中有一个为NULL，所以数据无效，设置valid为0，其余延误时间和飞行时间不做处理
            query("update flight_delay set valid='%s' where key_id='%d'" % (0,key_id))
            print("无效数据[",key_id,'] valid属性置0')
        else:
            #定义和计算起飞延误时间
            dep_delay=(d2-d1).total_seconds()//60
            #定义和计算落地延误时间
            arr_delay=(a2-a1).total_seconds()//60
            flt_time=flt_time_str.split(':')
            #将[03:52]的时间格式转换成分钟
            time_min=int(flt_time[0])*60+int(flt_time[1])
            #将结果返回数据库
            query("update flight_delay set 起飞延误='%d',落地延误='%d',飞行时间='%d',valid=1 where key_id='%d'" % (dep_delay,arr_delay,time_min,key_id))         
            #print(row['key_id'],row['航班号'],dep_delay)
    print("结果输出完毕！")

#将新增航班添加到延误数据库
import_data
#计算延误时间
caculate_time()