# -*- coding: utf-8 -*-
"""
Created on Wed Jun 24 20:51:57 2020

@author: WAP
"""

import jqdatasdk as jq
import myjqcfg as cfg
import time as ti
import pandas as pd
from os import path
from datetime import date, datetime, timedelta

def getStrToday(tback= 0):
    return (date.today()-timedelta(days= tback)).strftime("%Y%m%d")

def getStrNextDay(strDate):
    dt= datetime.strptime(strDate,'%Y%m%d')
    dt= dt+ timedelta(days=1)
    return dt.strftime("%Y%m%d")

def syncTickData(jqID, sdate):
    strSDate= sdate.strftime("%Y%m%d")
    filePath= path.join(cfg.jqDataPath,'tickData', jqID+ strSDate+ '.csv')
    if not path.exists(filePath):
        edate= sdate+ timedelta(days=1)
        tickPdData= jq.get_ticks(jqID, start_dt= strSDate, end_dt= edate.strftime("%Y%m%d"), count= None)
        tickPdData.to_csv(filePath, index= 0)
    else:
        tickPdData= pd.read_csv(filePath)
    return tickPdData

def syncListTickData(listJQID, listDate):
    for sdate in listDate:
        for jqID in listJQID:
            filePath= path.join(cfg.jqDataPath,'tickData' , jqID+ sdate+ '.csv')
            if not path.exists(filePath):
                tickPdData= jq.get_ticks(jqID, start_dt= sdate, end_dt= getStrNextDay(sdate), count= None)
                if len(tickPdData)>1:
                    tickPdData.to_csv(filePath, index= 0)
    
def syncListTradeDay(sdate, edate):
    listDate= jq.get_trade_days(start_date= sdate, end_date= edate, count=None)
    filePath= path.join(cfg.jqDataPath, 'trade_days.csv')
    listDate= pd.Series(listDate)
    pd.Series(listDate).to_csv(filePath,header= 0, index= 0)
    listDate= listDate.map(lambda x:x.strftime("%Y%m%d"))
    return list(listDate)

if __name__ == '__main__':
    t0 = ti.time()
    jq.auth(cfg.jqUser, cfg.jqPwd)
    
    """
    #jqFutureID= 'IC2006.CCFX'
    #jqETFID= '510500.XSHG'
    #sdate= date(2020,6,1)
    
    #tickFutureData= syncTickData(jqFutureID, sdate)
    #tickETFData= syncTickData(jqETFID, sdate)

    df = jq.get_all_securities(['etf'])
    #codelist= ['510500','510300','510330','IF2007','IF2008','IF2009','IC2007','IC2008','IC2009']
    codelist= ['510050','IH2003','IH2006','IH2009','IF2005']
    #jq.get_security_info(jqIDList[-1]).type
    #jq.get_query_count()
    
    codelist=[]
    for y in range(3):
        sym= 'IF'+ str(17+ y)
        for i in range(12):
            codelist.append(sym+ str(i+1).zfill(2))
    for i in range(7):
        codelist.append('IF20'+ str(i+1).zfill(2))
    """
    codelist= ['510500','510300','510330','IF2007','IF2008','IF2009','IC2007','IC2008','IC2009']
    edate= getStrToday()
    listJQID= jq.normalize_code(codelist)
    listDate= syncListTradeDay('20190101', edate)
    
    #syncListTickData(listJQID, [edate])
    syncListTickData(listJQID, listDate)
    
    jq.logout()
           
    print('All done, time elapsed: %.2f min' %  ((ti.time() - t0)/60))
    







