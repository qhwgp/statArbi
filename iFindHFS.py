#http://quantapi.10jqka.com.cn/?page=sample

from iFinDPy import *
#用户在使用时请修改成自己的账号和密码
thsLogin = THS_iFinDLogin('zszq5850', '555285')
if(thsLogin == 0 or thsLogin == -201):

	#利用数据池函数获取上证50在2016-11-27的成分股
    #数据池函数格式为THS_DataPool('modelName','inputParams','outputParams')
    #modelName不可以为空，且一次只能输入一个
    #inputParams用英文半角分号隔开，如inputParam1;inputParam2;inputParam3
    #outputParams用英文半角冒号赋值，用英文半角逗号分隔，Y表示该字段输出，N表示该字段不输出，如果不写则默认为Y,如outputParam1:Y,outputParam2:Y,outputParam3:N
    #THS_DataPool('modelName','inputParam1;inputParam2;inputParam3','outputParam1,outputParam2,outputParams3')
   
    #【001005260】是板块ID或者可以直接输入“上证50”，目前板块ID可以在【iFinD终端-工具-数据接口-板块ID查询工具】查看
    thsDataDataPool  = THS_DataPool('block','2019-12-09;001005260','date:Y,thscode:Y,security_name:Y')
    thsCodes = thsDataDataPool['tables'][0]['table']['THSCODE']
    f = open("DataPoolandHighFrequenceSequence.txt","a+")
    f.write("THSCode\tTime\tOPEN\tHIGH\tLOW\tCLOSE\n")
    for i in range(0,len(thsCodes)-1):
	
		#高频序列函数格式为THS_HighFrequenceSequence('thsCodes','indicators','params','startTime','endTime')
		#thsCodes不可以为空，且支持多个输入，当有多个thsCodes则用英文半角逗号分隔，如thsCode1,thsCode2,thsCode3
		#indicators不可以为空，且支持多个输入，当有多个indicators则用英文半角分号分隔，如indicator1;indicator2;indicator3
		#params不可以为空，且支持多个输入，当使用默认的参数时可以使用'default'表示，当用户只对其中某个指标设定而其他参数保持默认时，只需要输入设定的参数即可，如'Interval:5'
		#startDate的日期输入格式为YYYY-MM-DD HH:MM:SS
		#endDate的日期输入格式为YYYY-MM-DD HH:MM:SS
		#THS_HighFrequenceSequence('thsCode1,thsCode2,thsCode3','indicator1;indicator2;indicator3','param1,param2,param3','startTime','endTime')
        thsDataHighFrequenceSequence = THS_HighFrequenceSequence(thsCodes[i],"open;high;low;close","CPS:0,MaxPoints:50000,Fill:Previous,Interval:1","2019-11-25 09:35:00","2019-11-25 10:35:00")
        open_= thsDataHighFrequenceSequence['tables'][0]['table']['open'][0]
        if(open_==''):
            open_=0.00
            open_=float(open_)
        high_= thsDataHighFrequenceSequence['tables'][0]['table']['high'][0]
        if(high_==''):
            high_=0.00
            high_=float(high_)
        low_= thsDataHighFrequenceSequence['tables'][0]['table']['low'][0]
        if(low_==''):
            low_=0.0000
            low_=float(low_)
        close_= thsDataHighFrequenceSequence['tables'][0]['table']['close'][0]
        if(close_==''):
            close_=0.0000
            close_=float(close_)
        thscode_ = thsDataHighFrequenceSequence['tables'][0]['thscode']
        time_    = thsDataHighFrequenceSequence['tables'][0]['time'][0]
        f.write("%s\t%s\t%f\t%f\t%f\t%f\n"%(thscode_,time_,open_,high_,low_,close_))
    f.close()
else:
    print("登录失败")
