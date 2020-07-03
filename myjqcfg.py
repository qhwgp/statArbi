# -*- coding: utf-8 -*-
"""
Created on Tue Jun 23 10:05:31 2020

@author: WAP
"""
from os import path
import pandas as pd

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

#trade params
tradeThreshold= 1
positionThreshold= 10
levelThreshold= 5
nTradeSilence= 10
nParamDay= 5

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
    
    
    
    
    
    
    
    
    