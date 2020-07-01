# -*- coding: utf-8 -*-
"""
Created on Mon Jun 22 09:05:50 2020

@author: WAP
"""

import warnings, myjqcfg
warnings.filterwarnings('ignore')
from pymssql import connect
from datetime import date, timedelta
import numpy as np
import pandas as pd
import time as ti
import jqdatasdk as jq
from sqlalchemy import create_engine

class myDataAPI:

    def __init__(self,cfg):
        self.cfg = cfg

    def connect(self):
        try:
            self.conn = connect(host=self.cfg.host,user=self.cfg.user,password=self.cfg.pwd,database=self.cfg.db,charset="UTF-8")
            self.conn.autocommit(True)
            self.cur = self.conn.cursor()
            if not self.cur:
                return False
            jq.auth(self.cfg.jqUser, self.cfg.jqPwd)
            return jq.is_auth()
        except:
            return False
                
    def close(self):
        try:
            self.cur.close()
            self.conn.close()
            jq.logout()
        except:
            pass
        
    def getDate(self, startDate):
        sql="SELECT distinct id+CONVERT(varchar(10), tm, 112) as tdate FROM jqtickdata"
        self.cur.execute(sql)
        return list(map(lambda x:int(x[0]),self.cur.fetchall()))
        
    def getDateData(self, jqID, intDate):
        dataTable= self.cfg.dictDataTable[jq.get_security_info(jqID).type]
        sqlword= "SELECT * FROM %s where id='%s' and CONVERT(varchar(10), tm, 112)='%d'"%(dataTable, jqID, intDate)
        data= pd.read_sql(sqlword, con=self.conn) 
        if len(data)>0:
            return data
        data= getJQTickData(jqID, intDate)
        updateTickData(data, dataTable)
        return data
        
def createTickDataBase(tickDTName):
    sqlword = """
    CREATE TABLE %s (
    id VARCHAR(30) NOT NULL,
    tm datetime,
    clast numeric(10,3),
    high    numeric(10,3),
    low     numeric(10,3),
    volume  bigint,
    amnt   bigint,
    a1_p    numeric(10,3),
    a1_v    int,
    a2_p    numeric(10,3),
    a2_v    int,
    a3_p    numeric(10,3),
    a3_v    int,
    a4_p    numeric(10,3),
    a4_v    int,
    a5_p    numeric(10,3),
    a5_v    int,
    b1_p    numeric(10,3),
    b1_v    int,
    b2_p    numeric(10,3),
    b2_v    int,
    b3_p    numeric(10,3),
    b3_v    int,
    b4_p    numeric(10,3),
    b4_v    int,
    b5_p    numeric(10,3),
    b5_v    int,
    PRIMARY KEY(id,tm)
    )
    """% tickDTName
    myapi.cur.execute(sqlword)
    
    futureTickDT= 'jqFutureTickData'
    sqlword = """
    CREATE TABLE %s (
    id VARCHAR(30) NOT NULL,
    tm datetime,
    clast numeric(10,3),
    high    numeric(10,3),
    low     numeric(10,3),
    volume  bigint,
    amnt   bigint,
    position bigint,
    a1_p    numeric(10,3),
    a1_v    int,
    b1_p    numeric(10,3),
    b1_v    int,
    PRIMARY KEY(id,tm)
    )
    """%futureTickDT
    myapi.cur.execute(sqlword)
    
    indexTickDT= 'jqIndexTickData'
    sqlword = """
    CREATE TABLE %s (
    id VARCHAR(30) NOT NULL,
    tm datetime NOT NULL,
    clast numeric(10,3),
    high    numeric(10,3),
    low     numeric(10,3),
    volume  bigint,
    amnt   bigint,
    PRIMARY KEY(id,tm)
    )
    """%indexTickDT
    myapi.cur.execute(sqlword)
    
def nextIntDay(intDate):
    y= int(intDate/10000)
    m= int((intDate-y*10000)/100)
    d= intDate- y*10000 - m*100
    dt= date(y, m, d)+ timedelta(days=1)
    return dt.year*10000+ dt.month*100 +dt.day

def getJQTickData(jqID, intSDate, intEDate= 0):
    if not jq.is_auth():
        return None
    if intEDate== 0:
        intEDate= nextIntDay(intSDate)
    tickPdData= jq.get_ticks(jqID, start_dt= str(intSDate), end_dt= str(intEDate), count= None)
    tickPdData.rename(columns={'time':'tm', 'current':'clast', 'money':'amnt'}, inplace = True)
    tickPdData.insert(0, 'id' , jqID)
    return tickPdData
    
def updateTickData(tickPdData, dataTable):
    strEngine= "mssql+pymssql://%s:%s@%s:1433/%s"% (myjqcfg.user, myjqcfg.pwd, myjqcfg.host, myjqcfg.db)
    engine = create_engine(strEngine)
    tickPdData.to_sql(dataTable, con= engine, if_exists= 'append', index= False)
    
    
if __name__ == '__main__':
    t0 = ti.time()

    myapi=myDataAPI(myjqcfg)
    if not myapi.connect():
        print('login error! please check.')
    
    #jq.get_query_count()
    #createTickDataBase(myjqcfg.tickDT)
    #qhJQID= jq.get_all_securities(['futures'])
    #JQindID = jq.get_all_securities(['index'])
    """
    securityID = '000001.XSHG'
    jqID= jq.normalize_code(securityID)
    jq.get_security_info(jqID).type
    intDate= 20200624
    tickPdData= myapi.getDateData(jqID, intDate)
    
    futureID= 'IC2009.CCFX'
    jqID= jq.normalize_code(futureID)
    dataTable= myjqcfg.futureTickDT
    tickPdData= myapi.getDateData(jqID, intDate)
    
    indexID= '000001.XSHG'
    jq.get_security_info(indexID).type
    
    
    
    """

    
    codelist= ['510500','510300','510330','IF2003','IF2006','IF2009','IC2003','IC2006','IC2009']
    codelist= ['000300.SH','000905.SH']
    jqIDList= jq.normalize_code(codelist)
    ld= jq.get_trade_days(start_date='20200101', end_date='20200623', count=None)
    for dt in ld:
        intDate= int(dt.strftime("%Y%m%d"))
        print('dealing %d, time elapsed: %.2f min' % (intDate, (ti.time() - t0)/60)) 
        for jqID in jqIDList:
            tickPdData= myapi.getDateData(jqID, intDate)
            
    myapi.close()        
    print('All done, time elapsed: %.2f min' %  ((ti.time() - t0)/60))
    
    
    
    