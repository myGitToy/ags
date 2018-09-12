#import ags_mail as mail
#import mail as ml
from ags_mail import mail as mt
from ags_report import report_person_v1

if __name__ == '__main__':  
    ml=mt(name='刘羽2',month='2018-08',email='g.huiqiao@aliyun.com',start_date='2018-08-01',end_date='2018-08-31')
    status=ml.send()
    print(status)

def __init__(self,month,start_date,end_date):
    #初始化
    month='2018-07'
    start_date='2018-08-01'
    end_date='2018-08-31'

'''
def main(self):
    pass
'''
    
#main()
