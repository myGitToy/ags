# -*- coding: utf-8 -*-
"""
文档说明：

"""
import os
import sys
import time
import numpy as np
import pandas as pd
from datetime import datetime,timedelta
from numpy import mean, ptp, var, std
#from ags.ags.profile.mysql import query,query_list
#from profile.mysql_df import query_df
#from ags.ags.profile.setup import setup_snapshot
#from ags.event.event import event
from profile.mysql_df import query_df


class event():
    '''
    用于处理ags等级事件的类
    '''
    def __init__(self):
        pass
    
    def get_event(self , start = None , end = None , filter_event_list = "" , filter_pilot_rank = "'D1','D2','D3'" , export_csv = False):
        '''
        获取事件列表
        参数：
            start 开始日期
            end 结束日期
            filter_event_list 需要排除的事件列表
            filter_pilot_rank 需要排除的飞行员排班授权 主要用于剔除二副等
            export_csv 是否导出 默认否 False
        '''
        sql="select ags.event_id,lnk.key_id,lnk.ags_id,lnk.姓名,lnk.机上岗位,lnk.技术授权,lnk.责任机长标识,DATE_FORMAT(ags.`Flight Date`, '%%Y-%%m-%%d') as 航班日期,ags.`A/C Type`,ags.`A/C Tail`,ags.`Flight No`,ags.`Departure Airport`,ags.`Arrival Airport`,ags.`Event Short Name`,ags.`Maximum Value`,ags.`Severity Class No`,ags.`Event Validity` " \
        "from ags_event ags,crew_link lnk " \
        "where lnk.key_id=ags.key_id and lnk.技术授权 not in ('%s') and ags.`Event Short Name` not in ('%s') and ags.`Flight Date` between '%s' and '%s'" % (filter_pilot_rank , filter_event_list , start , end)
        df_detail = query_df(sql)
        #重构索引
        df_detail.set_index(['event_id'], inplace = True) 
        #结果输出至excel
        if export_csv==True:
            df_detail.to_csv('%s/ags/%s_%s_event_person.csv' % (os.getcwd(),start,end),encoding='utf_8_sig')

if __name__ == '__main__':
    a = event()
    df = a.get_event(start = '2019/1/1' , end = '2019/12/31')
    print(df)