# -*- coding: utf-8 -*-
"""
Created on Tue Jun 23 10:05:31 2020

@author: WAP
"""
from os import path
import pandas as pd
import matplotlib.pyplot as plt
from datetime import date, datetime, timedelta


host='127.0.0.1'
user='sa'
pwd='123'
db='myJQData'
tickDT= 'jqtickdata'
dictDataTable= {
    'stock': 'jqtickdata',
    'etf': 'jqtickdata',
    'futures': 'jqFutureTickData',
    'index': 'jqIndexTickData'
    }
futureTickDT= 'jqFutureTickData'
indexTickDT= 'jqIndexTickData'

jqUser= '18665883365'
jqPwd= 'Hu12345678'

jqDataPath= 'F:\\LenovoSync\\jqData'
#jqDataPath= 'C:\\Users\\WAP\\Documents\\wap\\jqData'
#jqDataPath= "H:\jqData"

#trade params
tradeParam= {'jqETFID': '510300.XSHG',
            'future': 'IF',
            'tradeThreshold': 1,
            'positionThreshold': 5,
            'levelThreshold': 8,
            'nTradeSilence': 3,
            'nParamDay': 4}

def getStrToday(tback= 0):
    return (date.today()-timedelta(days= tback)).strftime("%Y%m%d")

def getStrNextDay(strDate):
    dt= datetime.strptime(strDate,'%Y%m%d')
    dt= dt+ timedelta(days=1)
    return dt.strftime("%Y%m%d")

def getFutureMultiply(jqFutureID):
    f2= jqFutureID[0:2]
    if f2=='IC':
        return 200
    elif f2=='IF' or f2=='IH':
        return 300
    else:
        return 0
    

def getTickData(jqID, sdate):
    #strSDate= sdate.strftime("%Y%m%d")
    filePath= path.join(jqDataPath,'tickData', jqID+ sdate+ '.csv')
    if not path.exists(filePath):
        return None
    else:
        tickPdData= pd.read_csv(filePath, index_col= 0)
    return tickPdData

def getTradeDays():
    filePath= path.join(jqDataPath, 'trade_days.csv')
    data= pd.read_csv(filePath,header= None)
    return list(data[0].map(lambda x:x.replace('-','')))

def getMergeData(jqETFID, jqFutureID, sdate, nDrop= 10):
    filePath= path.join(jqDataPath,'midData', 'merge_%s_%s_%s.csv'% (jqETFID, jqFutureID, sdate))
    if not path.exists(filePath):
        tickFutureData= getTickData(jqFutureID, sdate)
        tickETFData= getTickData(jqETFID, sdate)
        tickFutureData.columns= tickFutureData.columns.map(lambda x:'f'+x)
        mdata= pd.merge(tickETFData, tickFutureData, left_index=True, right_index=True, how='outer')
        mdata= mdata.fillna(method='ffill')
        mdata= mdata[mdata.index.isin(tickETFData.index)]
        mdata.drop(mdata.index[:nDrop], inplace= True)
        mdata.drop(mdata.index[-nDrop:], inplace= True)
        mdata.to_csv(filePath)
    else:
        mdata= pd.read_csv(filePath, index_col= 0)
    return mdata
    
def drawFigure(data, fname, ftitle, secondY):
    filePath= path.join(jqDataPath, 'figure', fname)
    data.plot(title= ftitle, fontsize= 20, figsize=(40,30), secondary_y= secondY).get_figure().savefig(filePath)
    plt.close('all')
    
def saveResult(pdData, saveName):
    filePath= path.join(jqDataPath,'modelResult', saveName)
    pdData.to_csv(filePath)
    