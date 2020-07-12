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
    jq.get_query_count()
    codelist= ['510500','510300','510330','IF2007','IF2008','IF2009','IC2007','IC2008','IC2009']
    edate= cfg.getStrToday()
    listJQID= jq.normalize_code(codelist)
    listDate= syncListTradeDay('20190101', edate)
    syncListTickData(listJQID, listDate)
    jq.logout()
    print('All done, time elapsed: %.2f min' %  ((ti.time() - t0)/60))
    