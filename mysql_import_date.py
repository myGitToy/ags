# -*- coding: utf-8 -*-
"""
文档说明：
    本模块专门进行数据初始化工作
    引用规范：不推荐被引用
    
版本信息：
    version 0.1
    乔晖 2018/7/7

修改日志：
    2018/9/16
        1. 调整代码规范，运行正常，无bug
"""
import ags_event as event
import ags_snapshot as snapshot
import flight_delay as delay
import crew_link_line as link
import ags_report as report
from profile.setup import setup as setup
from profile.setup import setup_ac_type as ac_type

#####定义
#月份
month=setup.month
#开始日期
start=setup.start_date
#结束日期
end=setup.end_date

#下列注释代码已被配置文件取代，可删除
#max机尾号
#max_tail_list="'B-1379','B-1381','B-1382','B-1259','B-1260','B-1261','B-1263'"
#MAX机型
#max_type='73M'

#########导入航线信息：拆分机组  这是所有分析的基础和前提
#数据导入
link.import_data()
print("crew link line process complete")

#########导入ags_event
#月度事件匹配
event.match()
#数据导出
event.export_ags_event_summary(start_date=start,end_date=end)
event.export_ags_event_person(start_date=start,end_date=end)
print("ags event process complete")

#########导入ags_snapshot
#数据清洗
snapshot.clean()
#数据匹配
snapshot.match()
print("ags snapshot process complete")

#########导入flight_delay
#将新增航班添加到延误数据库
delay.import_data
#计算延误时间
delay.caculate_time()
print("flight delay process complete")

#########计算每个月MAX没有匹配的航班列表
print(snapshot.analyze_qar_download_status(ac_type.B737MAX_tail_list,start,end,ac_type.B737MAX))
print('导出每个月MAX没有匹配的航班列表，数据详情请下载\export\QAR_notmatched.csv')

#########导入邮件发送列表
#初始化邮件发送列表
report.mail_init(month=month,start_date=start,end_date=end)