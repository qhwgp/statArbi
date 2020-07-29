# -*- coding: utf-8 -*-
"""
Created on Wed Jul 15 16:35:27 2020

@author: WAP
"""

import warnings
warnings.filterwarnings('ignore')
from pymssql import connect
from sqlalchemy import create_engine
from datetime import date
from os import path
import numpy as np
import pandas as pd
import time as ti

def deStrCode(strData):
    try:
        return str.strip(strData.encode('latin1').decode('GB2312'))
    except:
        return strData

def myStr(myStr):
    try:
        resultData= int(myStr)
    except:
        resultData= myStr
    return str(resultData)

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
        
    def getDate(self, startDate):
        sql="select distinct substring(流水号, 1, 8) as tdate from wtzc40histv1..ut_local_journal_acct where\
            substring(流水号, 1, 8)>%d group by 流水号 having COUNT(0) > 0 order by tdate DESC" % startDate
        self.cur.execute(sql)
        return list(map(lambda x:int(x[0]),self.cur.fetchall()))
        
    def getDateData(self,tDate):
        sql="select 列名=name from syscolumns where id=object_id(N'ut_local_journal_acct')"
        self.cur.execute(sql)
        cols= list(map(lambda x:x[0],self.cur.fetchall()))
        #sql="select * from ut_local_journal_acct where fsrq = '" + tDate + "'"
        sql="select * from ut_local_journal_acct where fsrq = '%d'" % tDate
        self.cur.execute(sql)
        #data= pd.DataFrame(self.cur.fetchall())
        data= pd.read_sql(sql, con=self.conn) 
        data.columns= cols
        data.rename(columns={'fsrq':'发生日期'}, inplace = True)
        data.index= data['流水号'].map(lambda x:x[8:])
        data= data.drop(['serial_no','nbzjzh','yyb_zjzh','流水号'], axis=1)
        return data
    
    def getData(self, sql):
        self.cur.execute(sql)
        data= pd.read_sql(sql, con=self.conn) 
        #data['business_name']= data['business_name'].map(deStrCode)
        return data

def rebuildDT(localSQL, dataTable):
    try:
        localSQL.cur.execute('drop table %s'% dataTable)
    except:
        pass
    sqlword = """
    CREATE TABLE  %s (
    serial_no VARCHAR(30) NOT NULL,
    busi_date int,
    business_name  VARCHAR(30),
    fund_chg     float,
    sec_code  VARCHAR(30),
    sec_type   VARCHAR(30),
    sec_chg float,
    done_amt    float,
    contract_no    VARCHAR(30),
    rpt_contract_no    VARCHAR(30),
    done_no    VARCHAR(30),
    done_date VARCHAR(30),
    relative_code VARCHAR(30),
    mark VARCHAR(30),
    code_type VARCHAR(30),
    PRIMARY KEY(serial_no,busi_date)
    )
    """% dataTable
    localSQL.cur.execute(sqlword)
    
def syncDateData(clsSQL, localSQL, engine, tdate):
    sql= "select serial_no, busi_date, business_name, fund_chg, sec_code, sec_type, sec_chg, done_amt, contract_no,\
            rpt_contract_no, done_no, done_date, relative_code from uv_tcl_his_fund_stock_chg_71 where busi_date= %s order by serial_no" % tdate
    data= clsSQL.getData(sql)
    data['business_name']= data['business_name'].map(deStrCode)
    data['busi_date']= data['busi_date'].map(int)
    data['done_no']= data['done_no'].map(myStr)
    data['sec_code']= data['sec_code'].map(str.strip)
    data['contract_no']= data['contract_no'].map(str.strip)
    data['relative_code']= data['relative_code'].map(str.strip)
    data['busi_date']= tdate
    data['mark']='unmarked'
    data.to_sql('TFDT', con= engine, if_exists= 'append', index= False)
    
def syncData(clsSQL, localSQL, engine, intSDate= 0):
    sql= "select distinct busi_date from uv_tcl_his_fund_stock_chg_71 order by busi_date"
    pdDate= clsSQL.getData(sql)
    listDate= pdDate['busi_date'].values.tolist()
    
    sql= "select distinct busi_date from TFDT"
    localDate= localSQL.getData(sql)
    listLocalDate= localDate['busi_date'].map(lambda x:str(x)[:8]).values.tolist()
    
    for dt in listDate:
        if dt< intSDate:
            continue
        tdate= str(dt)[:8]
        if tdate in listLocalDate:
            continue
        print('sync date: '+ tdate)
        syncDateData(clsSQL, localSQL, engine, tdate)
    print('last date: '+ tdate)
    
def updateData(localSQL, engine, data):
    rebuildDT('tempDT')
    data.to_sql('tempDT', con= engine, if_exists= 'append', index= False)
    sql="UPDATE TFDT SET mark=t.mark FROM (SELECT * from tempDT) AS t WHERE TFDT.serial_no=t.serial_no and TFDT.busi_date=t.busi_date"
    localSQL.cur.execute(sql)
    
if __name__ == '__main__':
    t0 = ti.time()
    host='172.21.6.152'
    user='wanggp'
    pwd='Wanggp@0511'
    db='data_ceneter_all'
    clsSQL= MSSQL(host,user,pwd,db)
    clsSQL.Connect()
    localSQL= MSSQL('127.0.0.1', 'sa', '123', 'markedTF71')
    localSQL.Connect()
    if not clsSQL.isConnect:
        print(clsSQL.host + ' not connect')
    elif not localSQL.isConnect:
        print(localSQL.host + ' not connect')
    else:
        
        engine = create_engine("mssql+pymssql://sa:123@127.0.0.1:1433/markedTF71")
        #tdate= '20200707' #= pdDate.iloc[-1,0]
        
        syncData(clsSQL, localSQL, engine)
        
        #rebuildDT(localSQL, 'TFDT')
        
    print('All done, time elapsed: %.2f min' %  ((ti.time() - t0)/60))    
        
    
    """  
lsdata=data[data['business_name']=='ETF赎回']
    

sql= "select  serial_no, busi_date, business_name, fund_chg, sec_code, sec_type, sec_chg, done_amt, contract_no,\
            rpt_contract_no, done_no, done_date, relative_code from uv_tcl_his_fund_stock_chg_71 where done_no= '12273179' or rpt_contract_no='5900058069'"
testdata= clsSQL.getData(sql)    
testdata['business_name']= testdata['business_name'].map(deStrCode)    
testdata['busi_date']= testdata['busi_date'].map(lambda x:str(x)[:8])    
testdata['done_no']= testdata['done_no'].map(lambda x:str(int(x)))


    myapi.cur.execute(sqlword)

sql='select * from TFDT where busi_date= 20200716'
data=localSQL.getData(sql)
data.to_csv('data.csv')
    
    """
    
    
    
      