# -*- coding: utf-8 -*-
"""
Created on Mon Aug 10 22:41:15 2020

@author: sysadmin
"""

from pymssql import connect
from sqlalchemy import create_engine
import time as ti
import pandas as pd
from datetime import date, datetime, timedelta

jqUser= '18665883365'
jqPwd= 'Hu12345678'

listJQID= ['000016.XSHG', '000300.XSHG', '000905.XSHG', '000986.XSHG', '000987.XSHG', '000988.XSHG', '000989.XSHG',
       '000990.XSHG', '000991.XSHG', '000992.XSHG', '000993.XSHG', '000994.XSHG', '000995.XSHG']

listY= ['000016.XSHG', '000300.XSHG', '000905.XSHG']

host='172.21.6.152'
user='wanggp'
pwd='Wanggp@0511'
lcdb='Wangprivate'

def timeStart():
    return ti.time()
    
def timeEnd(t0):
    print('All done, time elapsed: %.2f min' %  ((ti.time() - t0)/60))
    
def getStrToday(tback= 0):
    return (date.today()-timedelta(days= tback)).strftime("%Y%m%d")

def getStrNextDay(strDate):
    dt= datetime.strptime(strDate,'%Y%m%d')
    dt= dt+ timedelta(days=1)
    return dt.strftime("%Y%m%d")
  
class MSSQL:
    def __init__(self,host,user,pwd,db):
        self.host = host
        self.user = user
        self.pwd = pwd
        self.db = db
        self.engine = create_engine("mssql+pymssql://%s:%s@%s:1433/%s"%(user, pwd, host, db))
        self.isConnect= False

    def Connect(self):
        try:
            self.conn = connect(host=self.host,user=self.user,password=self.pwd,database=self.db,charset="UTF-8")
            self.conn.autocommit(True)
            self.cur = self.conn.cursor()
            if not self.cur:
                self.isConnect= False
            else:
                self.isConnect= True
        except:
            self.isConnect= False
        
    def Close(self):
        self.cur.close()
        self.conn.close()
        
    def getTradeDays(self):
        sql="select distinct tdate from jqIndexDataDT order by tdate" 
        data= pd.read_sql(sql, con=self.conn)
        return list(data['tdate'])
    
    def getDateData(self, jqID, tDate):
        sql= "select mtime,mlast,amntw from jqIndexDataDT where tdate=%d and jqID='%s' order by mtime"% (tDate, jqID)
        data= pd.read_sql(sql, con=self.conn)
        return data
    
    def getYDateData(self, jqID, tDate):
        sql= "select mtime,mlast from jqIndexDataDT where tdate=%d and jqID='%s' order by mtime"% (tDate, jqID)
        data= pd.read_sql(sql, con=self.conn)
        return data
    
    def getParams(self, jqID, tDate, sdate):
        sql= "select stdev(tickret),avg(amntw) from jqIndexDataDT where tdate>=%d and tdate<%d and jqID='%s'"% (sdate, tDate, jqID)
        data= pd.read_sql(sql, con=self.conn)
        return list(data.iloc[0].values)
    
    def getYParams(self, jqID, tDate, sdate):
        sql= "select stdev(tickret),avg(amntw) from jqIndexDataDT where tdate>=%d and tdate<%d and jqID='%s'"% (sdate, tDate, jqID)
        data= pd.read_sql(sql, con=self.conn)
        return data.iloc[0,0]
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    