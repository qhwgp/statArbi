# -*- coding: utf-8 -*-
"""
Created on Fri Aug  7 23:49:26 2020

@author: WAP
"""

import myjqcfg as cfg
import pandas as pd
import numpy as np
import scipy.stats as stats
import time as ti
from tensorflow.keras import models,backend,callbacks
from tensorflow.keras.layers import GRU

def normjqData(data):
    data['tickret']= (data['current']-data['current'].shift(1))/data['current'].shift(1)*10000
    data['amntw']= data['money'].diff()/ 10000
    #data[['tickret', 'amntw']]= data[['tickret', 'amntw']].round(2)
    data= data.iloc[100:-50]
    data= data.drop(['current', 'high', 'low', 'volume', 'money'], axis= 1)
    return data

def normYdata(data, smove, std= 1):
    data['tickret']= (data['current'].shift(-smove)- data['current'])/ data['current']* 10000/ std
    #data['tickret']= data['tickret'].map(lambda x:stats.norm.cdf(x)*2-1)
    data= data.iloc[100:-50]
    data= data.drop(['current', 'high', 'low', 'volume', 'money'], axis= 1)
    return data

def getNormData(data, params):
    data['tickret']= data['tickret']/ params[0]
    data['tickret']= data['tickret'].map(lambda x:stats.norm.cdf(x)*2-1)
    data['amntw']= data['amntw']/ params[1]
    data['amntw']= data['amntw'].map(lambda x:1- 2*np.exp(-x))
    data=data.resample('5S').ffill()
    return data

def getNormX(listTradeDay, listJQID, nday):
    amdata= pd.DataFrame()
    pmdata= pd.DataFrame()
    for jqID in listJQID:
        hisdata= pd.DataFrame(columns= ['tickret', 'amntw'])
        for i in range(5):
            ldate= listTradeDay[nday-i-1]
            hisdata= pd.concat([hisdata, normjqData(cfg.getTickData(jqID, ldate))])
        params= (hisdata['tickret'].std(), hisdata['amntw'].mean())
        jqdata= cfg.getTickData(jqID, listTradeDay[nday])
        data= normjqData(jqdata)
        data= getNormData(data, params)
        data.columns=[jqID[:6]+'r', jqID[:6]+'a']
        strdate= data.index[0].date().strftime('%Y-%m-%d')
        amdata= pd.concat([amdata, data.loc[strdate+' 09:40:00':strdate+' 11:20:00']], axis= 1)
        pmdata= pd.concat([pmdata, data.loc[strdate+' 13:10:00':strdate+' 14:50:00']], axis= 1)
    return [amdata, pmdata]

def getNormY(listTradeDay, listY, nday, smove):
    amdata= pd.DataFrame()
    pmdata= pd.DataFrame()
    for jqID in listY:
        hisdata= pd.DataFrame(columns= ['tickret'])
        for i in range(5):
            ldate= listTradeDay[nday-i-1]
            #data= normjqData(cfg.getTickData(jqID, ldate), -smove)
            hisdata= pd.concat([hisdata, normYdata(cfg.getTickData(jqID, ldate), smove)])
        #std= hisdata['tickret'].std()
        jqdata= cfg.getTickData(jqID, listTradeDay[nday])
        data= normYdata(jqdata, smove, hisdata['tickret'].std())
        data['tickret']= data['tickret'].map(lambda x:stats.norm.cdf(x)*2-1)
        #data= data.drop(['amntw'], axis= 1)
        data.columns=[jqID[:6]]
        strdate= data.index[0].date().strftime('%Y-%m-%d')
        amdata= pd.concat([amdata, data.loc[strdate+' 09:40:00':strdate+' 11:20:00']], axis= 1)
        pmdata= pd.concat([pmdata, data.loc[strdate+' 13:10:00':strdate+' 14:50:00']], axis= 1)
    return [amdata, pmdata]

def getTensorData(xNormData,yNormData,nx,ny):
    lenDData= len(xNormData[0])- ny
    nday= len(xNormData)
    xData=[]
    yData=[]
    for iday in range(nday):
        xdayData= xNormData[iday]
        ydayData= yNormData[iday]
        for i in range(nx,lenDData):
            xData.append(xdayData.iloc[(i-nx):i].values)
            yData.append(ydayData.iloc[i].values)
    xData=np.array(xData)
    yData=np.array(yData)
    return (xData,yData)

def myLoss(y_true, y_pred):
    return backend.mean(backend.abs((y_pred - y_true)*y_true), axis=-1)

def myMetric(y_true, y_pred):
    return backend.mean(y_pred*y_true, axis=-1)*10

def buildRNNModel(xShape, doRate= 0.23):
    model = models.Sequential()
    model.add(GRU(3,input_shape=xShape,dropout=doRate))

    #model.add(Dense(1))
    model.compile(loss=myLoss,optimizer= 'nadam',metrics=[myMetric])
    return model

#Basic 5:
def trainRNNModel(model,xNormData,nDailyData,nx,ny,iy,xTest,yTest,batchSize=10000,nRepeat=5):

    eStop=callbacks.EarlyStopping(monitor='val_loss',patience=nRepeat,
                                  mode='min', restore_best_weights=True)
    return model.fit(xData, yData, callbacks=[eStop],epochs=nRepeat*nRepeat).history
  
def generateTrainData(xNormData,nDailyData,nx,ny,iy,geneR,nRepeat,batchSize):
    xData=[]
    yData=[]
    for nrpt in range(nRepeat*nRepeat):
        r = np.random.permutation(geneR)
        i=0
        for n in r:
            i+=1
            xData.append(xNormData[(n-nx):n,:-len(ny)].reshape((2,nx,int((xNormData.shape[1]-len(ny))/2))))
            yData.append(xNormData[n,iy-len(ny)])
            if i%batchSize==batchSize-1:
                xData=np.array(xData)
                yData=np.array(yData)
                yield (xData,yData)
                xData=[]
                yData=[]

if __name__ == '__main__':
    t0 = ti.time()
    listTradeDay= cfg.getTradeDays()
    n= 500
    smove= 60
    xNormData= []
    yNormData= []
    for i in range(10):
        nday= n+ i
        xNormData+= getNormX(listTradeDay, cfg.listJQID, nday)
        yNormData+= getNormY(listTradeDay, cfg.listY, nday, smove)
    
    (xData,yData)= getTensorData(xNormData,yNormData, 60, 60)
    """
    jqID= cfg.listY[0]
    
    """
    model = models.Sequential()
    model.add(GRU(3,input_shape=[60,26],dropout=0.2))
    model.compile(loss=myLoss,optimizer= 'nadam',metrics=[myMetric])
    eStop=callbacks.EarlyStopping(monitor='loss',patience= 3, mode='min', restore_best_weights=True)
    model.fit(xData, yData, callbacks=[eStop], batch_size=50, epochs= 20)
    
    ypre= model.predict(xData[0:1])
    
    
    
    
    