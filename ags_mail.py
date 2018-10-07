# -*- coding: utf-8 -*-
"""
文档说明【类】：
    邮件处理模块，用于发送QAR品质报告
    主要完成事件匹配和事件数据输出，为QAR分析提供csv格式的raw_data
    --->smtp模式默认为25或80端口，实测建议使用80端口，25并不稳定
    --->ssl模式请使用465端口
    引用规范：请使用下列语句
        from  ags_mail import mail as mail #主模块引用
        from profile.setup import setup_smtp as setup   #邮件配置文件引用
    初始化：
        ml=mail(name,month,email,start_date,end_date,text_msg) 注：实际代码中目前并没有使用start_date和end_date
    发送邮件：
        ml.send()

基本框架：
    __init__   初始化
    send    发送邮件

版本信息：
    version 0.1
    乔晖 2018/9/22

修改日志：
    #2018/9/5
    1. 【邮件】增加ags_mail.py，添加邮件发送功能

TODO LIST：
    目前邮件发送是通过本模块调用ags_report.xxx来获取邮件正文的，未来可能计划采用引入参数的模式，需要修改__init__
"""
import smtplib
import email
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.base import MIMEBase
from email.mime.application import MIMEApplication
from email.header import Header
from profile.setup import setup_smtp
import ags_report
#from ags_report import report_person_v1
#import ags_report as rpt

class mail(object):
    def __init__(self,name,month,email,start_date,end_date,text):
        """
        [数据初始化]######
        函数说明 创建：乔晖 2018/9/5
        [输入Parameters]:
        name：姓名 如乔晖
        month：月份 如2018-08
        email：邮件地址 直接从数据库获取，不进行校验
        start_date：开始日期 和月份匹配，如2018-08-01 目前不确定是否使用，因为调用ags_snapshot的相关代码
        end_date：结束日期 如2018-08-31 规则如上
        -------
        [返回值return]：
        无
        """
        #类创建时需要输入的参数
        self.c_name=name
        self.m_month=month
        self.rcptto=email
        self.start_date=start_date
        self.end_date=end_date
        self.text=text
        
        #其他系统默认参数
        # 发件人地址，通过控制台创建的发件人地址
        self.username = setup_smtp.smtp_username
        # 发件人密码，通过控制台创建的发件人密码
        self.password = setup_smtp.smtp_password
        # 自定义的回复地址
        self.replyto = setup_smtp.smtp_replyto
        # 收件人地址或是地址列表，支持多个收件人，最多30个
        #注：目前邮件列表测试失败，只能单一邮件地址进行发送
        #rcptto = ['***', '***']
        #self.rcptto ='g.huiqiao@aliyun.com'
        print("初始化%s" % self.rcptto)

    def send(self):
        """
        [发送邮件(SMTP核心代码)]######
        函数说明 创建：乔晖 2018/9/12
        [输入Parameters]:
        无
        使用t.send()调用
        -------
        [返回值return]：
        True:邮件发送成功
        Flase:邮件发送失败
        """
        # 构建alternative结构
        msg = MIMEMultipart('alternative')
        msg['Subject'] = Header('【测试】QAR个人月度分析材料')
        msg['From'] = '%s <%s>' % (Header('QAR月报'), self.username)
        msg['To'] = self.rcptto
        msg['Reply-to'] = self.replyto
        msg['Message-id'] = email.utils.make_msgid()
        msg['Date'] = email.utils.formatdate() 
        # 构建alternative的text/plain部分
        #text_msg=ags_report.report_person_v1(self.c_name,self.m_month)
        text_msg=self.text
        
        textplain = MIMEText(text_msg, _subtype='plain', _charset='UTF-8')
        msg.attach(textplain)
        # 构建alternative的text/html部分
        #texthtml = MIMEText('自定义HTML超文本部分', _subtype='html', _charset='UTF-8')
        #msg.attach(texthtml)
        # 构建alternative的附件部分
        
        # 构造MIMEBase对象做为文件附件内容并附加到根容器
        contype = 'application/octet-stream'
        maintype, subtype = contype.split('/', 1)
        """
        ## 读入文件内容并格式化
        #with open('~/environment/TuShare/data/today_all.csv', mode='w', encoding='UTF-8', errors='strict', buffering=1) as file:
        #    file.write(result)
        file_attach = open('~/environment/TuShare/data/today_all.csv','rb')
        #print(file_attach)
        file_msg = MIMEBase(maintype, subtype)
        file_msg.set_payload(data.read( ))
        data.close( )
        email.Encoders.encode_base64(file_msg)
         
        ## 设置附件头
        basename = os.path.basename(file_name)
        file_msg.add_header('Content-Disposition',
         'attachment', filename = basename)
        main_msg.attach(file_msg)
        """
        
        # 发送邮件
        try:
            client = smtplib.SMTP()
            #python 2.7以上版本，若需要使用SSL，可以这样创建client
            #client = smtplib.SMTP_SSL()
            #SMTP普通端口为25或80
            client.connect('smtpdm.aliyun.com', 80)
            #开启DEBUG模式 0:静默 1:详细信息
            client.set_debuglevel(0)
            client.login(self.username, self.password)
            #发件人和认证地址必须一致
            #备注：若想取到DATA命令返回值,可参考smtplib的sendmaili封装方法:
            #      使用SMTP.mail/SMTP.rcpt/SMTP.data方法
            client.sendmail(self.username, self.rcptto, msg.as_string())
            client.quit()
            print('邮件发送成功！')
            return True
        except smtplib.SMTPConnectError as e:
            print('邮件发送失败，连接失败:', e.smtp_code, e.smtp_error)
            return False
        except smtplib.SMTPAuthenticationError as e:
            print('邮件发送失败，认证错误:', e.smtp_code, e.smtp_error)
            return False
        except smtplib.SMTPSenderRefused as e:
            print('邮件发送失败，发件人被拒绝:', e.smtp_code, e.smtp_error)
            return False
        except smtplib.SMTPRecipientsRefused as e:
            print('邮件发送失败，收件人被拒绝:', e.smtp_code, e.smtp_error)
            return False
        except smtplib.SMTPDataError as e:
            print('邮件发送失败，数据接收拒绝:', e.smtp_code, e.smtp_error)
            return False
        except smtplib.SMTPException as e:
            print('邮件发送失败, ', e.message)
            return False
        except Exception as e:
            print('邮件发送异常, ', str(e))
            return False



