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

if __name__ == '__main__':
    t0 = ti.time()
    jq.auth(cfg.jqUser, cfg.jqPwd)
    jq.get_query_count()
    localSQL= cfg.MSSQL(cfg.host, cfg.user, cfg.pwd, cfg.lcdb)
    localSQL.Connect()
    if not localSQL.isConnect:
        print(localSQL.host + ' not connect')
    else:
        sdate= '20200807'
        jqID= cfg.listIndex[0]
        tickPdData= jq.get_ticks(jqID, start_dt= sdate, end_dt= cfg.getStrNextDay(sdate), count= None)
        data= tickPdData.drop(['high', 'low', 'volume'], axis= 1)
        data['amntw']= data['money'].diff()/ 10000
        data['amntw']= data['amntw'].round(2)
        data= data.iloc[1:]
        data.drop([])
    
    