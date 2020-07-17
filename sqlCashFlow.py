# -*- coding: utf-8 -*-
"""
Created on Fri Jan 17 18:20:00 2020

@author: WGP
"""

import warnings
warnings.filterwarnings('ignore')
from pymssql import connect
from datetime import date
from os import path
import numpy as np
import pandas as pd
import time as ti

def deStrCode(strData):
    try:
        return strData.encode('latin1').decode('GB2312')
    except:
        return strData

def myFloat(strData):
    try:
        resultData= float(strData)
    except ValueError:
        resultData= 0.0
    return resultData

def mySubStr(strData):
    dt= str(strData).split("(")[0]
    return dt.encode('latin1').decode('GB2312')
    
def codeStr(strData):
    return str(strData).zfill(6)

def codeType(myStr):
    if myStr[0]== '1' or myStr[0]== '5':
        return 'etf'
    elif myStr[0]== '0' or myStr[0]== '3' or myStr[0]== '6':
        return 'stock'
    else:
        return 'other'
    
def normETFCode(code):
    code=str(code)
    if code[0]=='5':
        return code[:5]+'0'
    else:
        return code
    
    
def myInt(myStr):
    try:
        resultData= int(myStr)
    except:
        resultData= 0
    return resultData

def getSRList(data, nRow):
    if data.iloc[nRow].买卖类别!= '转' or data.iloc[nRow].成交编号<1:
        return []
    SRList= [data.iloc[nRow].name]
    nData= len(data)
    firstRow= nRow
    lastRow= nRow
    while firstRow> 0:
        if abs(data.iloc[firstRow- 1].成交编号- data.iloc[firstRow].成交编号)<90 and data.iloc[firstRow- 1].买卖类别== '转' and not 'ETF' in data.iloc[firstRow- 1].摘要:
            firstRow-= 1
            SRList.append(data.iloc[firstRow].name)
        else:
            break
    while lastRow< nData:
        if abs(data.iloc[lastRow+ 1].成交编号- data.iloc[lastRow].成交编号)<90 and data.iloc[lastRow+ 1].买卖类别== '转' and not 'ETF' in data.iloc[lastRow+ 1].摘要:
            lastRow+= 1
            SRList.append(data.iloc[lastRow].name)
        else:
            break
    hth=data.iloc[nRow].合同号
    if hth>1:
        setHth=set(data[data['合同号']==hth].index)
        if len(setHth)>len(SRList):
            return list(set.union(setHth,set(SRList)))
    return SRList

def dealNAData(naData):
    etfData=naData[naData['realCode']==naData['realCode']].groupby('realCode')['成交数'].sum()
    stockData=naData[naData['realCode']!=naData['realCode']]
    if len(stockData)==0:
                return stockData
    if stockData.iloc[0]['成交数']>0:
        leftData=stockData.sort_values(by='成交数',ascending=False)
    else:
        leftData=stockData.sort_values(by='成交数',ascending=True)
    nETFData=len(etfData)
    runAgainList=[]
    if nETFData!=0:
        for iETFRow in range(len(etfData)):
            if len(leftData)==0:
                return stockData
            bound=abs(etfData[iETFRow])
            fillList,weight=fillBag(leftData,bound)
            if weight==bound:
                stockData.loc[fillList,'realCode']=etfData.index[iETFRow]
                leftData=leftData.drop(fillList)
            else:
                runAgainList.append(iETFRow)
        for iETFRow in runAgainList:
            if len(leftData)==0:
                return stockData
            bound=abs(etfData[iETFRow])
            fillList,weight=fillBag(leftData,bound)
            stockData.loc[fillList,'realCode']=etfData.index[iETFRow]
            leftData=leftData.drop(fillList)
    return stockData

def fillBag(stockData,bound):
    item=abs(stockData.iloc[0].成交数)
    if bound<=0:
        return [],0
    if len(stockData)==1:
        if item>bound:
            return [],0
        else:
            return [stockData.index[0]],item
    elif item>bound:
        return fillBag(stockData.drop(stockData.index[0]),bound)
    else:
        putList,putWeight=fillBag(stockData.drop(stockData.index[0]),bound-item)
        if putWeight==bound-item:
            return [stockData.index[0]]+putList,bound
        notPutList,notPutWeight=fillBag(stockData.drop(stockData.index[0]),bound)
        if notPutWeight>putWeight+item:
            return notPutList,notPutWeight
        else:
            return [stockData.index[0]]+putList,item+putWeight

def getData(data,delList):
    #t0 = ti.time()
    #print('dealing data: '+ str(dataName))
    #data = pd.read_excel(dataName+'.xlsx', index_col=10)
    #data= pd.read_csv('./dailyData/'+str(dataName)+'.csv', index_col=0, encoding='GB18030')
    data=data[data.index==data.index]
    #data.index=data.index.map(normIndex)
    data= data.drop(['本次余额','股东帐号','市场名称','库存数','摘要代码',
                      '成交金额','市场代码','信用交易信息','银行代码'], axis=1)
    for bscode in delList:
        data= data[data['业务代码']!=str(bscode)]
    data['摘要']= data['摘要'].map(mySubStr)
    data['买卖类别']= data['买卖类别'].map(deStrCode)
    data['证券代码']= data['证券代码'].map(codeStr)
    data['成交编号']= data['成交编号'].map(myInt)
    data['合同号']= data['合同号'].map(myInt)
    data['关联合同号']= data['关联合同号'].map(myInt)
    data['realCode']= np.nan
    data['codeType']= data['证券代码'].map(codeType)
    data= data.sort_index()
    data['nRow']=range(len(data))
    #etf
    lsData= data[data['codeType']=='etf']
    data.loc[lsData.index, 'realCode']= lsData['证券代码'].map(normETFCode)
    data.loc[lsData.index, '摘要']= lsData['摘要'].map(lambda x:x.replace('证券','ETF') )
    #otherTpye
    lsData= data[data['codeType']=='other']
    data.loc[lsData.index, 'realCode']= lsData['证券代码']
    #trustTrans
    lsData= data[(data['摘要']=='托管转入')|(data['摘要']=='托管转出')]
    data.loc[lsData.index, 'realCode']= 'trust'
    #corContract
    lsData= data[(data['关联合同号']>500000)&(data['关联合同号']<599999)]
    data.loc[lsData.index, 'realCode']= lsData['关联合同号'].map(normETFCode)
    #transStock
    lsData= data[(data['摘要']=='ETF申购')|(data['摘要']=='ETF赎回')]
    for i in range(len(lsData)):
        data.loc[getSRList(data, lsData.iloc[i]['nRow']),'realCode']= lsData.iloc[i]['证券代码']
    #tradeStock
    setCode= set(data[data['codeType']=='stock']['证券代码'])
    #nCode= '000063'
    for nCode in setCode:
        #sub
        lsData=data[(data['证券代码']==nCode)&(data['成交数']!=0)&((data['摘要']=='证券买入')|(data['摘要']=='申购赎回过出'))]
        #lsData=lsData.fillna(method='bfill')
        #idata=idata[data['摘要']=='证券买入']
        
        #wap
        
        nLsData= len(lsData)
        lsData['isMatched']=False
        if nCode[0]=='0':
            for iRow in range(nLsData):
                if lsData.iloc[iRow]['摘要']=='申购赎回过出':
                    cRow= 0
                    qty= -lsData.iloc[iRow]['成交数']
                    while cRow<nLsData:
                        if lsData.iloc[cRow]['摘要']=='证券买入' and lsData.iloc[cRow]['成交数']==qty and lsData.iloc[cRow]['isMatched']==False:
                            lsData.iloc[cRow,17]=lsData.iloc[iRow]['realCode']
                            lsData.iloc[cRow,20]=True
                            lsData.iloc[iRow,20]=True
                            break
                        else:
                            cRow+=1
        else:
            for iRow in range(nLsData):
                if lsData.iloc[iRow]['摘要']=='申购赎回过出':
                    cRow= iRow
                    qty= -lsData.iloc[iRow]['成交数']
                    while cRow>=0:
                        if lsData.iloc[cRow]['摘要']=='证券买入' and lsData.iloc[cRow]['成交数']==qty and lsData.iloc[cRow]['isMatched']==False:
                            lsData.iloc[cRow,17]=lsData.iloc[iRow]['realCode']
                            lsData.iloc[cRow,20]=True
                            lsData.iloc[iRow,20]=True
                            break
                        else:
                            cRow-=1
        naData=lsData[lsData['isMatched']==False].loc[:,['成交数','realCode']]
        if len(naData)>0:
            naData=dealNAData(naData)
            lsData.loc[naData.index,'realCode']=naData['realCode']
        data.loc[lsData.index,'realCode']=lsData['realCode']
        #redeem
        lsData=data[(data['证券代码']==nCode)&((data['摘要']=='申购赎回过入')|(data['摘要']=='证券卖出'))]
        nLsData= len(lsData)
        lsData['isMatched']= False
        if nCode[0]== '6':
            for iRow in range(nLsData):
                if lsData.iloc[iRow]['摘要']=='申购赎回过入':
                    cRow= iRow
                    qty= -lsData.iloc[iRow]['成交数']
                    while cRow<nLsData:
                        if lsData.iloc[cRow]['摘要']=='证券卖出' and lsData.iloc[cRow]['成交数']==qty and lsData.iloc[cRow]['isMatched']==False:
                            lsData.iloc[cRow,17]=lsData.iloc[iRow]['realCode']
                            lsData.iloc[cRow,20]=True
                            lsData.iloc[iRow,20]=True
                            break
                        else:
                            cRow+=1  
        else:
            for iRow in range(nLsData):
                if lsData.iloc[iRow]['摘要']=='申购赎回过入':
                    cRow= iRow
                    qty= -lsData.iloc[iRow]['成交数']
                    while cRow>=0:
                        if lsData.iloc[cRow]['摘要']=='证券卖出' and lsData.iloc[cRow]['成交数']==qty and lsData.iloc[cRow]['isMatched']==False:
                            lsData.iloc[cRow,17]=lsData.iloc[iRow]['realCode']
                            lsData.iloc[cRow,20]=True
                            lsData.iloc[iRow,20]=True
                            break
                        else:
                            cRow-=1
        naData=lsData[lsData['isMatched']==False]
        if len(naData)>0:
            naData=dealNAData(naData)
            lsData.loc[naData.index,'realCode']=naData['realCode']
        data.loc[lsData.index,'realCode']=lsData['realCode']
    #noRealCode
    lsData=data[data['realCode']!=data['realCode']]
    data.loc[lsData.index, 'realCode']='noCode'
    #data.to_csv('./dailyMarkData/mark'+str(dataName)+'.csv', encoding='GB18030')
    #print('%d done, time elapsed: %.2f min' % (dataName,(ti.time() - t0)/60)) 
    return data

def normIndex(myIndex):
    try:
        if myIndex> 1e15 or myIndex< 1e12:
            return myIndex
        else:
            return myIndex[8:]
    except:
        return myIndex

class MSSQL:

    def __init__(self,host,user,pwd,db):
        self.host = host
        self.user = user
        self.pwd = pwd
        self.db = db

    def Connect(self):
        try:
            self.conn = connect(host=self.host,user=self.user,password=self.pwd,database=self.db,charset="UTF-8")
            self.conn.autocommit(True)
            self.cur = self.conn.cursor()
            if not self.cur:
                return False
            else:
                return True
        except:
            return False
        
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
        
if __name__ == '__main__':
    t0 = ti.time()
    host='127.0.0.1'
    user='sa'
    pwd='123'
    db='wtzc40histv1'
    sql=MSSQL(host,user,pwd,db)
    sql.Connect()
    
    endDate= int(date.today().strftime("%Y%m%d"))
    startDate= endDate- 100
    listDate=sql.getDate(startDate)
    
    delList= [4196,4197,4198,4199,4200,4201]
    for dataName in listDate:
        if dataName< startDate-0.1 or dataName> endDate+ 0.1:
            continue
        fileName= './dailyMarkData/mark%d.csv' % dataName
        if path.isfile(fileName):
            continue
        print('dealing data: %d' % dataName)
        data= sql.getDateData(dataName)
        data= getData(data, delList)
        data.to_csv(fileName, encoding='GB18030')
        print('%d done, time elapsed: %.2f min' % (dataName,(ti.time() - t0)/60)) 
        
    sql.Close()
    print('All done, last file date: %d, time elapsed: %.2f min' % (listDate[0], (ti.time() - t0)/60)) 
    
    
    

    
    """
    
    sqlword="select top 10 * from ut_local_journal_acct where fsrq = '20200117'" 
    sqlword="select 列名=name from syscolumns where id=object_id(N'ut_local_journal_acct') "
    
    检查数据是否有重复
    sqlword="select distinct substring(流水号, 1, 8) as tdate from wtzc40histv1..ut_local_journal_acct group by 流水号 having COUNT(0) > 1 order by tdate"
    sql.cur.execute(sqlword)
    row = sql.cur.fetchall()
    tt=list(map(lambda x:x[0],row))
    
    totalStockData= pd.DataFrame(row)
    totalStockData.columns=tt
    """
    
    