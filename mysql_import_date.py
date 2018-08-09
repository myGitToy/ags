# -*- coding: utf-8 -*-
#本模块专门进行数据初始化工作
#from ags_event import match,export_ags_event_summary,export_ags_event_person
import ags_event
from ags_snapshot import match,clean
import flight_delay
import crew_link_line

#####定义
#开始日期
start='2018-7-1'
#结束日期
end='2018-7-31'
#max机尾号
max_tail_list="'B-1379','B-1381','B-1382','B-1259','B-1260','B-1261','B-1263'"
#MAX机型
max_type='73M'

#########导入航线信息：拆分机组  这是所有分析的基础和前提
#数据导入
crew_link_line.import_data()
print("crew link line process complete")

#########导入ags_event
#月度事件匹配
ags_event.match()
#数据导出
ags_event.export_ags_event_summary(start_date=start,end_date=end)
ags_event.export_ags_event_person(start_date=start,end_date=end)
print("ags event process complete")

#########导入ags_snapshot
#数据清洗
ags_snapshot.clean()
#数据匹配
ags_snapshot.match()
print("ags snapshot process complete")


#########导入flight_delay
#将新增航班添加到延误数据库
flight_delay.import_data
#计算延误时间
flight_delay.caculate_time()
print("flight delay process complete")

#########计算每个月MAX没有匹配的航班列表
print('导出每个月MAX没有匹配的航班列表，数据详情请下载\export\QAR_notmatched.csv')
print(ags_snapshot.analyze_qar_download_status(max_tail_list,start,end,max_type))