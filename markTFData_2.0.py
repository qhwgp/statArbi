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
    
    def addData(self, engine, data):
        data.to_sql('TFDT', con= engine, if_exists= 'append', index= False)

    def updateMarkData(self, engine, data):
        try:
            self.cur.execute('drop table tempDT')
        except:
            pass
        sqlword = """
        CREATE TABLE  tempDT (
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
        PRIMARY KEY(serial_no,busi_date)
        )
        """
        self.cur.execute(sqlword)
        data.to_sql('tempDT', con= engine, if_exists= 'append', index= False)
        sql="UPDATE TFDT SET mark=t.mark FROM (SELECT * from tempDT) AS t WHERE TFDT.serial_no=t.serial_no and TFDT.busi_date=t.busi_date"
        self.cur.execute(sql)

def normETFCode(code):
    code=str(code)
    if code[0]=='5':
        return code[:5]+'0'
    elif code[0]=='1':
        return code
    else:
        return 'unmarked'
    
def codeStr(strData):
    try:
        res= str(int(strData)).zfill(6)
    except:
        res= str(strData)
    return res

def myInt(myStr):
    try:
        resultData= str(int(myStr))
    except:
        resultData= myStr
    return resultData
    
def codeType(myStr):
    try:
        if myStr[0]== '1' or myStr[0]== '5':
            return 'etf'
        elif myStr[0]== '0' or myStr[0]== '3' or myStr[0]== '6':
            return 'stock'
        else:
            return 'other'
    except:
        return 'other'
    
def getMarkData(data, tdate):
    data['codeType']= data['sec_code'].map(codeType)
    lsdata= data[data['codeType']=='etf']
    data.loc[lsdata.index, 'mark']= lsdata['sec_code'].map(normETFCode)
    lsdata= data[data['codeType']=='other']
    data.loc[lsdata.index, 'mark']= 'cash'
    data= data.drop(['codeType'], axis=1)
    undoData= data[data['mark']=='unmarked']

    listStockBN= ['申购赎回过入', '申购赎回过出', '证券买入', '证券卖出']
    lsdata= undoData[(undoData['business_name'].isin(listStockBN)==False)|(undoData['sec_chg']==0)]
    data.loc[lsdata.index, 'mark']= lsdata['relative_code'].map(normETFCode)
    undoData= data[data['mark']=='unmarked']
    

    #undoData.iloc[0]
    lsdata= undoData.groupby('sec_code')['serial_no'].count()
    for scode in lsdata.index:
        #scode= lsdata.index[-1]
        
        sdata= data[(data['sec_code']== scode)&((data['business_name']== '证券买入')|(data['business_name']== '申购赎回过出'))]
        nLsData= len(sdata)
        for iRow in range(nLsData):
            if sdata.iloc[iRow]['business_name']=='申购赎回过出':
                cRow= 0
                qty= -sdata.iloc[iRow]['sec_chg']
                while cRow<nLsData:
                    if sdata.iloc[cRow]['business_name']=='证券买入' and sdata.iloc[cRow]['sec_chg']==qty and sdata.iloc[cRow]['mark']=='unmarked':
                        sdata.iloc[cRow,13]= sdata.iloc[iRow,12]
                        sdata.iloc[iRow,13]= sdata.iloc[iRow,12]
                        break
                    else:
                        cRow+=1
        data.loc[sdata.index, 'mark']= sdata['mark']
                        
        sdata= data[(data['sec_code']== scode)&((data['business_name']== '证券卖出')|(data['business_name']== '申购赎回过入'))]
        nLsData= len(sdata)
        for iRow in range(nLsData):
            if sdata.iloc[iRow]['business_name']=='申购赎回过入':
                cRow= 0
                qty= -sdata.iloc[iRow]['sec_chg']
                while cRow<nLsData:
                    if sdata.iloc[cRow]['business_name']=='证券卖出' and sdata.iloc[cRow]['sec_chg']==qty and sdata.iloc[cRow]['mark']=='unmarked':
                        sdata.iloc[cRow,13]= sdata.iloc[iRow,12]
                        sdata.iloc[iRow,13]= sdata.iloc[iRow,12]
                        break
                    else:
                        cRow+=1
        data.loc[sdata.index, 'mark']= sdata['mark']
        
    undoData= data[data['mark']=='unmarked']
    nadp= 0
    nUndo= len(undoData)
    for i in range(nUndo):
        #inde= undoData.index[0]
        if undoData.iloc[i, 13]!= 'unmarked':
            continue
        scode= undoData.iloc[i, 4]
        #scode='000988'
        #qty= undoData.iloc[i, 6]
        lsdata= undoData[(undoData['sec_code']== scode)&(undoData['mark']== 'unmarked')]
        #wap
        gdata= lsdata.groupby(['business_name','relative_code'])['fund_chg','sec_chg','done_amt'].sum()
        if undoData.iloc[i, 2]== '证券买入':
            qty= gdata.loc[('证券买入', scode), 'sec_chg']
            if '申购赎回过出' in gdata.index:
                for rcode in gdata.loc['申购赎回过出','sec_chg'].index:
                    #rcode= gdata.loc['申购赎回过出','sec_chg'].index[1]
                    lsdata= undoData[(undoData['sec_code']== scode)&(undoData['mark']== 'unmarked')]
                    outqty= gdata.loc[('申购赎回过出', rcode), 'sec_chg']
                    if -outqty== qty:
                        sdata= lsdata[((lsdata['relative_code']== rcode)|(lsdata['business_name']== '证券买入'))&(lsdata['mark']== 'unmarked')]
                        undoData.loc[sdata.index, 'mark']= rcode
                        #data.loc[sdata.index, 'mark']= rcode

                    elif -outqty< qty:
                        sdata= lsdata[((lsdata['relative_code']== rcode)|(lsdata['business_name']== '证券买入'))&(lsdata['mark']== 'unmarked')]
                        undoData.loc[sdata.index, 'mark']= rcode
                        #data.loc[sdata.index, 'mark']= rcode
                        apd= undoData.iloc[i]
                        nadp+= 1
                        seqno= str(tdate)+ str(nadp).zfill(4)
                        apd.name= seqno
                        apd.serial_no= seqno
                        apd.sec_chg= -qty- outqty
                        apd.fund_chg= -(qty+ outqty)/qty* gdata.loc[('证券买入', scode), 'fund_chg']
                        apd.done_amt= -(qty+ outqty)/qty* gdata.loc[('证券买入', scode), 'done_amt']
                        undoData= undoData.append(apd)
                        
                        nadp+= 1
                        seqno= str(tdate)+ str(nadp).zfill(4)
                        apd.name= seqno
                        apd.serial_no= seqno
                        apd.sec_chg= -apd.sec_chg
                        apd.fund_chg= -apd.fund_chg
                        apd.done_amt= -apd.done_amt
                        apd.mark= 'unmarked'
                        undoData= undoData.append(apd)
                        qty+= outqty
                    else:
                        pass
        elif undoData.iloc[i, 2]== '证券卖出':
            qty= gdata.loc[('证券卖出', scode), 'sec_chg']
            if '申购赎回过入' in gdata.index:
                for rcode in gdata.loc['申购赎回过入','sec_chg'].index:
                    outqty= gdata.loc[('申购赎回过入', rcode), 'sec_chg']
                    if outqty== -qty:
                        sdata= lsdata[(lsdata['relative_code']== rcode)|(lsdata['business_name']== '证券卖出')]
                        undoData.loc[sdata.index, 'mark']= rcode
                        #data.loc[sdata.index, 'mark']= rcode
                    else:
                        pass            
    data.loc[undoData.index[:nUndo], 'mark']= undoData['mark']
    if len(undoData)> nUndo:
        apdata= undoData.iloc[nUndo:]
    else:
        apdata= pd.DataFrame()
    #data= pd.concat([data, undoData.index[:nUndo]], axis= 0)
    
    #undoData= data[data['mark']=='unmarked']

    return data, apdata
    
if __name__ == '__main__':
    t0 = ti.time()
    localSQL= MSSQL('127.0.0.1', 'sa', '123', 'markedTF71')
    localSQL.Connect()
    if not localSQL.isConnect:
        print(localSQL.host + ' not connect')
    else:
        engine = create_engine("mssql+pymssql://sa:123@127.0.0.1:1433/markedTF71")
        listDate= localSQL.getDate()
        for tdate in listDate:
            print('deal data date: %d'% tdate)
            #tdate= 20200630
            data= localSQL.getUnmarkedData(tdate)  
            data, apdata= getMarkData(data, tdate)
            localSQL.updateMarkData(engine, data)
            localSQL.addData(engine, apdata)
    
    print('All done, time elapsed: %.2f min' % ((ti.time() - t0)/60))
    
    