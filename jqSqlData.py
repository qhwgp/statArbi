# -*- coding: utf-8 -*-
"""
Created on Fri Aug  7 17:10:46 2020

@author: WAP
"""

import jqdatasdk as jq
import dtcfg as cfg
import time as ti
import pandas as pd
from os import path
from datetime import timedelta
listJQID= ['000016.XSHG', '000300.XSHG', '000905.XSHG', '000986.XSHG', '000987.XSHG', '000988.XSHG', '000989.XSHG',
       '000990.XSHG', '000991.XSHG', '000992.XSHG', '000993.XSHG', '000994.XSHG', '000995.XSHG']

listY= ['000016.XSHG', '000300.XSHG', '000905.XSHG']

def syncListTradeDay(sdate, edate):
    listDate= jq.get_trade_days(start_date= sdate, end_date= edate, count=None)
    #filePath= path.join(cfg.jqDataPath, 'trade_days.csv')
    listDate= pd.Series(listDate)
    #pd.Series(listDate).to_csv(filePath,header= 0, index= 0)
    listDate= listDate.map(lambda x:x.strftime("%Y%m%d"))
    return list(listDate)

def stimeToStr(stime):
    return stime.strftime('%Y-%m-%d %H:%M:%S')

if __name__ == '__main__':
    t0 = ti.time()
    jq.auth(cfg.jqUser, cfg.jqPwd)
    jq.get_query_count()
    localSQL= cfg.MSSQL(cfg.host, cfg.user, cfg.pwd, cfg.lcdb)
    localSQL.Connect()
    if not localSQL.isConnect:
        print(localSQL.host + ' not connect')
    else:
        sdate= '20200806'
        jqID= listJQID[0]
        
        edate= cfg.getStrToday()
        listDate= syncListTradeDay('20190101', edate)
        for sdate in listDate:
            print('deal date: '+ sdate)
            for jqID in listJQID:
                data= jq.get_ticks(jqID, start_dt= sdate, end_dt= cfg.getStrNextDay(sdate), count= None)
                data['tickret']= (data['current']-data['current'].shift(1))/data['current'].shift(1)*10000
                data['amntw']= data['money'].diff()/ 10000
                data[['tickret', 'amntw']]= data[['tickret', 'amntw']].round(2)
                data= data.iloc[10:-10]
                data= data.drop(['current', 'high', 'low', 'volume', 'money'], axis= 1)
                data.columns= ['mtime', 'tickret', 'amntw']
                #data['mtime']= data['mtime'].map(stimeToStr)
                data['jqID']= jqID
                data['tdate']= int(sdate)
                data.to_sql('jqIndexDataDT', con= localSQL.engine, if_exists= 'append', index= False)
        
        cfg.timeEnd(t0)
        """
        data= tickPdData.drop(['high', 'low', 'volume'], axis= 1)
        data['amntw']= data['money'].diff()/ 10000
        data['amntw']= data['amntw'].round(2)
        data= data.iloc[1:]
        data= data.drop(['money'], axis= 1)
        data['tdate']= int(sdate)
        data['icode']= jqID[:6]
        data=data[['icode', 'tdate', 'time', 'current', 'amntw']]
        """
        
        