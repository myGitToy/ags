# -*- coding: utf-8 -*-
#本模块专门进行数据初始化工作
#from ags_event import match,export_ags_event_summary,export_ags_event_person
import ags_event
import ags_snapshot
import flight_delay
import crew_link_line


#########导入航线信息：拆分机组  这是所有分析的基础和前提
#数据导入
crew_link_line.import_data()

#########导入ags_event
#月度事件匹配
ags_event.match()
#数据导出
ags_event.export_ags_event_summary(start_date='2018-6-1',end_date='2018-6-30')
ags_event.export_ags_event_person(start_date='2018-6-1',end_date='2018-6-30')

#########导入ags_snapshot
#数据清洗
ags_snapshot.clean()
#数据匹配
ags_snapshot.match()

#########导入flight_delay
#将新增航班添加到延误数据库
flight_delay.import_data
#计算延误时间
flight_delay.caculate_time()
