# -*- coding: utf-8 -*-
from profile.mysql import query,query_list
import time
from datetime import datetime


def import_data():
    ######[导入数据]######
    #函数说明 乔晖 2018/3/30
    #通过检查[flight_link_chn]中的机长、航线机长、一副、二副这四个四段，把相应的机组成员导入到[crew_link]表中
    #新导入的数据默认valid为null，因此在caculate_time函数中，通过检查valid值来确定哪些数据需要计算，哪些已经计算过了
    #######第一步：从[flight_link_chn]中取出所有没有做过导入的数据
    sql_flight_link_chn_not_imput="select key_id,联线号,机长,航线机长,一副,二副 from flight_link_chn where 机组拆分标识 is null"
    a=query(sql_flight_link_chn_not_imput)
    print("本次查询总共返回结果数：",a.rowcount)
    result=a.fetchall()
    for row in result:
        #获取数据
        key_id=row['key_id']
        link_id=row['联线号']
        cap=row['机长']
        crz=row['航线机长']
        f1=row['一副']
        f2=row['二副']
        #初始化sql查询语句
        sql_list=[]
        #初始化责任机长标识
        cap_flag_loop=0
        #######第二步：拆分和导入[机长]字段
        if len(cap)>0:
            #字段内有内容，拆分
            cap_list=cap.split("," )
            for n in cap_list:
                num=len(n)
                name=n[0:num-2]
                tech=n[num-2:num]
                if cap_flag_loop==0:
                    cap_flag=1
                else:
                    cap_flag=0
                cap_flag_loop=cap_flag_loop+1
                print("航班连线：%s|姓名：%s，|技术授权：%s,|责任机长标识=%d" % (link_id,name,tech,cap_flag))
                #将sql查询语句加载到多语句列表中
                sql_list.append("insert into crew_link (key_id,link_id,姓名,机上岗位,技术授权,责任机长标识) values ('%s','%s','%s','机长','%s','%s')" % (key_id,link_id,name,tech,cap_flag))
        else:
            #字段内无内容，跳过
            pass
        #######第三步：拆分和导入[航线机长]字段
        if len(crz)>0:
            #字段内有内容，拆分
            crz_list=crz.split("," )
            for n in crz_list:
                num=len(n)
                name=n[0:num-2]
                tech=n[num-2:num]
                #航线机长中直接将机长标识置为0
                cap_flag=0
                print("航班连线：%s|姓名：%s，|技术授权：%s,|责任机长标识=%d" % (link_id,name,tech,cap_flag))
                #将sql查询语句加载到多语句列表中
                sql_list.append("insert into crew_link (key_id,link_id,姓名,机上岗位,技术授权,责任机长标识) values ('%s','%s','%s','航线机长','%s','%s')" % (key_id,link_id,name,tech,cap_flag))
        else:
            #字段内无内容，跳过
            pass
        
        #######第四步：拆分和导入[一副]字段
        if len(f1)>0:
            #字段内有内容，拆分
            f1_list=f1.split("," )
            for n in f1_list:
                num=len(n)
                name=n[0:num-2]
                tech=n[num-2:num]
                #航线机长中直接将机长标识置为0
                cap_flag=0
                print("航班连线：%s|姓名：%s，|技术授权：%s,|责任机长标识=%d" % (link_id,name,tech,cap_flag))
                #将sql查询语句加载到多语句列表中
                sql_list.append("insert into crew_link (key_id,link_id,姓名,机上岗位,技术授权,责任机长标识) values ('%s','%s','%s','一副','%s','%s')" % (key_id,link_id,name,tech,cap_flag))
        else:
            #字段内无内容，跳过
            pass        
        
        #######第五步：拆分和导入[二副]字段
        if len(f2)>0:
            #字段内有内容，拆分
            f2_list=f2.split("," )
            for n in f2_list:
                num=len(n)
                name=n[0:num-2]
                tech=n[num-2:num]
                #航线机长中直接将机长标识置为0
                cap_flag=0
                print("航班连线：%s|姓名：%s，|技术授权：%s,|责任机长标识=%d" % (link_id,name,tech,cap_flag))
                #将sql查询语句加载到多语句列表中
                sql_list.append("insert into crew_link (key_id,link_id,姓名,机上岗位,技术授权,责任机长标识) values ('%s','%s','%s','二副','%s','%s')" % (key_id,link_id,name,tech,cap_flag))
        else:
            #字段内无内容，跳过
            pass 
        
        #######第六步：[flight_link_chn]中的机组拆分标识 置为1
        sql_list.append("update flight_link_chn set 机组拆分标识='1' where key_id='%d'" % (key_id))
        #######第七步：执行sql语句
        query_list(sql_list)

import_data()