3. 【事件】二三级事件率统计中存在数据输出不完整的情况，未修正
4. 【报表】备份航班计入到航班数中
 
#2020/2/16
1. 【事件】[export_ags_event_person]中排除第二副驾驶信息、排除自定义时间的导出，因为excel最大为140万行，一年数据可能会超这个限制
2. 【事件】增加[export_ags_event_maintenance]函数，用于导出用户自定义的事件

#2018/10/6
1. 【邮件】增加[mail_init]函数：初始化每月发送列表功能
2. 【邮件】增加[send]：邮件群发功能
3. 【系统】增加setup_report类，用以配置最大邮件发送数量（阿里云限制）
4. 【系统】增加月度导入模块中对于邮件列表导入的初始化

#2018/9/16
1. 【配置】按照代码规范调整配置文件

#2018/9/12
1. 【报表】规范报表代码，上传git
2. 【git】git merge from cloud9->master

#2018/9/5
1. 【邮件】增加ags_mail.py，添加邮件发送功能

#2018/8/13
1. 【事件】修正二三级事件率统计中的一处错误，事件率为正常的2倍，通过删除排除事件表（exp表）实现
2. 【事件】二三级事件率输出格式调整为小数点后5位

#2018/8/10
1. 【报表】增加数据报表功能，远期规划三大块内容：【汇总数据】、【快照数据】、【事件数据】，第一期首先解决【快照数据】的数据输出功能

#2018/8/9
1. 【快照】增加[analyze_person_month_df]函数：列出某人某月指定快照数据全部结果，后续用于飞行品质报告
2. 【快照】增加[analyze_fleet_month_df]函数：列出机队按月分布的快照数据结果，后续用于飞行品质报告


#2018/7/21
1. 【快照】增加一项新功能：分析每月航班数和快照数的差值，以筛选QAR源头未导入的航班
2. 【系统】每月导入结合模块中加入上述内容，并将结果保存至export文件夹中
3. 【系统】增加事件和快照中部分代码的文本缩进，增加代码的可读性

#2018/7/12
1. 【快照】完成机队月度快照数据的输出函数，一个传统格式的输出，另一个dataframe格式的输出（analyze_fleet_monthlist_df/analyze_fleet_monthlist_cl）

#2018/7/7
1. 【系统】将零散的数据导入工作进行汇总。每月月初excel数据导入mysql后，需要再ags_event/ags_snapshot/crew_link_line/flight_delay _
四个文件夹中分别执行一次导入工作，工作完成后再将内容注释掉，现在将这部分内容进行合并，归档到mysql_import_data中进行统一执行

#2018/7/6
1. 【系统】主要文件已完成迁移工作
2. 【系统】增加git ignore配置
3. 【系统】新增export文件夹，用于数据导出
4. 【系统】新增profile文件夹，用于配置文件存放。目前包含数据库连接文件和程序配置文件
5. 【事件】修改[export_ags_event_summary_dep]函数的输出路径至profile文件夹


#2018/6/16
1. 【事件】增加[export_ags_event_summary_dep]函数，将分部数据输出至csv格式文件中

#2018/6/12
1. 【事件】增加[export_ags_event_person]函数，输出月度所有飞行员各类事件（效仿原segem程序）
2. 【事件】上述函数的输出结果将每月汇总至QAR_Rawdata_ags中，为各类月报分析提供数据

#2018/6/11
1. 【事件】完成QAR事件与人员名字的匹配，目前匹配率达到99.6%，基本达到预想目标
2. 【事件】增加[export_ags_event_summary]函数，输出月度各分部人员QAR二三级事件率（效仿原segem程序）

#2018/5/10
1. 【系统】将原有ags内容迁移至git

#2018/4/22
1. 【快照】完成个人快照数据的输出函数（query2），目前仅针对单人输出，没有进行循环输出处理。

#2018/3/10
1. 【排班信息】北京过夜期间完成对于机组排班信息的导入和处理，将excel导入mysql后将机组进行拆分，方便后续QAR数据的匹配
2. 【排班信息】增加机组排班中对于责任机长的标识，好处是对于双机长的航班不会进行重复统计 _
3. 【排班信息】一个航班只存在一名责任机长，只要筛选责任机长标识为1，则拆分后的各类二三级事件数和原始数据将保持一致，不会有重复统计的情况发生