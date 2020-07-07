# -*- coding: utf-8 -*-   
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

#删除晃盘文件导入错误引起的虚假事件信息
df_count=query_df("select *  from ags_event where `Flight Date` is null")

#重构索引
print(df_count)
df=query_df("select flnk.`航班日期`,flnk.机型,lnk.姓名,dep.航班机型排班授权,dep.部门,ags.`From`,ags.`To`,ags.`RETARD_ALT (FT)` as 收油门高度,ags.`VRTG_MAX_LD (g)` as 着陆载荷,ags.`DIST_LDG (feet)` as 着陆距离,ags.`ROLL_MAX_BL20 (deg)` as 着陆坡度, ags.`PITCH_MAX_LD (deg)` as 着陆姿态, ags.`BOUNCE` as 着陆弹跳 from crew_link lnk,ags_snapshot ags,flight_link_chn flnk,flight_dep dep where ags.key_id=lnk.key_id and flnk.key_id=lnk.key_id and flnk.key_id=ags.key_id and lnk.姓名=dep.姓名  and flnk.机型 IN ('73M','73L','73H','738','73E','737','73G','73A') and flnk.航班日期 between '2019/1/1' and '2019/12/31' and lnk.`责任机长标识`=1")
df.to_csv('%s/ags/ags2019.csv' % (os.getcwd()),encoding='utf_8_sig')