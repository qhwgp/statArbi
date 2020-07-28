# -*- coding: utf-8 -*-
"""
Created on Sat Jul 18 00:27:39 2020

@author: WAP
"""
from os import path
import pandas as pd
import time as ti
import warnings
from pymssql import connect
from sqlalchemy import create_engine
warnings.filterwarnings('ignore')

def deStrCode(strData):
    try:
        return str.strip(strData.encode('latin1').decode('GB2312'))
    except:
        return strData

class MSSQL:

    def __init__(self,host,user,pwd,db):
        self.host = host
        self.user = user
        self.pwd = pwd
        self.db = db
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
        
    def getDate(self, startDate= 0):
        sql="select distinct busi_date from TFDT where busi_date >= %d order by busi_date" % startDate
        data= pd.read_sql(sql, con=self.conn)
        return list(data['busi_date'])
        
    def getDateData(self,tDate):
        sql= 'select * from TFDT where busi_date=%d'% tDate
        data= pd.read_sql(sql, con=self.conn)
        data['business_name']= data['business_name'].map(deStrCode)
        return data
    
    def getUnmarkedData(self,tDate):
        sql= "select * from TFDT where busi_date<=%d and mark='unmarked'"% tDate
        data= pd.read_sql(sql, con=self.conn)
        data['business_name']= data['business_name'].map(deStrCode)
        return data
    
    def getData(self, sql):
        data= pd.read_sql(sql, con=self.conn)
        return data
    
if __name__ == '__main__':
    t0 = ti.time()
    localSQL= MSSQL('127.0.0.1', 'sa', '123', 'markedTF71')
    localSQL.Connect()
    if not localSQL.isConnect:
        print(localSQL.host + ' not connect')
    else:
        engine = create_engine("mssql+pymssql://sa:123@127.0.0.1:1433/markedTF71")
        listDate= localSQL.getDate()

        tdate= listDate[0]
        data= localSQL.getDateData(tdate)
        subdata= data[data['business_name']== 'ETF申购']
        lsdata= data[data['rpt_contract_no']!= '']


    print('All done, time elapsed: %.2f min' % ((ti.time() - t0)/60))
    
    