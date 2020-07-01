# -*- coding: utf-8 -*-
"""
Created on Thu Jun 25 23:11:27 2020

@author: WAP@qqq
"""

import myjqcfg as cfg
from os import path
import pandas as pd
import numpy as np
#global etfOrderVolumns

    
def getFutureMultiply(jqFutureID):
    f2= jqFutureID[0:2]
    if f2=='IC':
        return 200
    elif f2=='IF' or f2=='IH':
        return 300
    else:
        return 0

def getTickData(jqID, strSDate):
    #strSDate= sdate.strftime("%Y%m%d")
    filePath= path.join(cfg.jqDataPath,'tickData', jqID+ strSDate+ '.csv')
    if not path.exists(filePath):
        return None
    else:
        tickPdData= pd.read_csv(filePath, index_col= 0)
    return tickPdData

def getTradeDays():
    filePath= path.join(cfg.jqDataPath, 'trade_days.csv')
    data= pd.read_csv(filePath,header= None)
    return data[0].map(lambda x:x.replace('-',''))

def getMDPrice(p1, p2, p3, p4, p5, v1, v2, v3, v4, v5, etfOrderVolumns):
    lp= np.array((p1, p2, p3, p4, p5))
    nv= np.array((v1, v2, v3, v4, v5))
    nv= nv/ etfOrderVolumns
    if np.sum(nv)< 1:
        return lp[-1]
    lv= 1
    for i in range(5):
        if nv[i]>= lv:
            nv[i]= lv
            if i< 4:
                nv[i+1:]= 0
            break
        else:
            lv-= nv[i]
    return lp.dot(nv)
        
def getDailyPairStat(jqETFID, jqFutureID, sdate, etfOrderVolumns):
    fMultiply= getFutureMultiply(jqFutureID)
    tickFutureData= getTickData(jqFutureID, sdate)
    tickETFData= getTickData(jqETFID, sdate)
    tickFutureData.columns= tickFutureData.columns.map(lambda x:'f'+x)
    mdata= pd.merge(tickETFData, tickFutureData, left_index=True, right_index=True, how='outer')
    mdata= mdata.fillna(method='ffill')
    mdata= mdata[mdata.index.isin(tickETFData.index)]
    inid= mdata.mean()
    dataOV= inid['fcurrent']* fMultiply/ inid['current']
    if etfOrderVolumns< 0.1:
        etfOrderVolumns= dataOV
    mdata['bidp']= mdata.apply(lambda x:getMDPrice(x.b1_p, x.b2_p, x.b3_p, x.b4_p, x.b5_p, x.b1_v, x.b2_v, x.b3_v, x.b4_v, x.b5_v, etfOrderVolumns), axis = 1)
    mdata['askp']= mdata.apply(lambda x:getMDPrice(x.a1_p, x.a2_p, x.a3_p, x.a4_p, x.a5_p, x.a1_v, x.a2_v, x.a3_v, x.a4_v, x.a5_v, etfOrderVolumns), axis = 1)
    mdata= mdata[['bidp', 'askp', 'fb1_p', 'fa1_p']]
    mdata.drop(mdata.index[:10], inplace= True)
    mdata.drop(mdata.index[-10:], inplace= True)
    #avgPrice= mdata.mean()
    #dataOV= np.round( (avgPrice.fb1_p+ avgPrice.fa1_p)/ (avgPrice.bidp+ avgPrice.askp)* fMultiply, -2)
    mdata['lesf']= mdata['askp']*etfOrderVolumns- mdata['fb1_p']* fMultiply
    mdata['self']= mdata['bidp']*etfOrderVolumns- mdata['fa1_p']* fMultiply
    mdata['mid']= (mdata['lesf']+ mdata['self'])/ 2
    mdata['spread']= mdata['lesf']- mdata['self']
    return dataOV, mdata#['mid'].mean(), mdata['mid'].std(), mdata['spread'].mean()

def checkTrade(row, runParams, tradeThreshold , positionThreshold, position):
    midPoint= runParams[0]- position*runParams[1]
    if row['lesf']< midPoint- tradeThreshold*runParams[1] and position< positionThreshold:
        return 'buy'
    elif row['self']> midPoint+ tradeThreshold*runParams[1] and position> -positionThreshold:
        return 'sell'
    else:
        return 'hold'        
    

def simuDay(mdata, runParams, tradeThreshold , positionThreshold, nTradeSilence, iniPosition):
    position= iniPosition
    nNearTrade= 0
    tradeData =pd.DataFrame(columns=('time', 'market', 'volume', 'price'))
    tradeSignal= 'hold'
    for time, row in mdata.iterrows():
        #check if near trade
        if nNearTrade> 0.1:
            nNearTrade-= 1
        elif tradeSignal!= 'hold':#check tradeSignal
            nNearTrade= nTradeSilence
            if tradeSignal== 'buy':
                tradeData= tradeData.append({'time': time, 'market': 'etf', 'volume': runParams[2], 'price': row['askp']}, ignore_index=True)
                tradeData= tradeData.append({'time': time, 'market': 'future', 'volume': -1, 'price': row['fb1_p']}, ignore_index=True)
                position+= 1
            elif tradeSignal== 'sell':
                tradeData= tradeData.append({'time': time, 'market': 'etf', 'volume': -runParams[2], 'price': row['bidp']}, ignore_index=True)
                tradeData= tradeData.append({'time': time, 'market': 'future', 'volume': 1, 'price': row['fa1_p']}, ignore_index=True)
                position-= 1
            tradeSignal= 'hold'
        else:#check trade
            tradeSignal= checkTrade(row, runParams, tradeThreshold , positionThreshold, position)
    return tradeData, position
    

def PairTradeStrategy(jqETFID, jqFutureID, sdate, nParamDay, tradeThreshold , positionThreshold, nTestDay):
    ls= getTradeDays()
    fMultiply= getFutureMultiply(jqFutureID)
    nindex= ls[ls==sdate].index[0]
    nloop= min(nTestDay, len(ls)- nindex)
    nparams= np.array([])
    etfPosition= 0
    futurePosition= 0
    lastEtfClose= 0
    lastFutureClose= 0
    pdDTradeInfo= pd.DataFrame(columns=('etfClose','futureClose','etfPosition','futurePosition', 'nintraDayTrade',
                                        'midMean', 'midStd', 'etfOrderVolumns', 'PL'))
    etfOrderVolumns= 0
    listTradeData=[]
    position= 0
    for i in range(nloop):
        ndate= ls[nindex+ i]
        dataOV, mdata= getDailyPairStat(jqETFID, jqFutureID, ndate, etfOrderVolumns)
        #countParams= len(nparams)
        if i== 0:
            nparams= np.array([[mdata['mid'].mean(), mdata['mid'].std(), dataOV]])
            etfOrderVolumns= np.round( dataOV, -2)
            continue
        elif i< 5:
            nparams= np.r_[nparams, np.array([[mdata['mid'].mean(), mdata['mid'].std(), dataOV]])]
            etfOrderVolumns= np.round( nparams[:,2].mean(), -2)
            continue
        else:
            runParams= np.round( nparams.mean(axis= 0), -2)
            etfOrderVolumns= runParams[2]
            #run simulation
            tradeData, position= simuDay(mdata, runParams, tradeThreshold , positionThreshold, nTradeSilence, position)
            listTradeData.append(tradeData)
            
            ld= mdata.iloc[-1]
            tradeDate= ld.name[:10]
            etfClose= (ld.bidp+ ld.askp) / 2
            futureClose= (ld.fb1_p+ ld.fa1_p) / 2
            PL= etfPosition* (etfClose- lastEtfClose)+ futurePosition* (futureClose- lastFutureClose)* fMultiply
            lastEtfClose= etfClose
            lastFutureClose= futureClose
            midMean= mdata['mid'].mean()
            midStd= mdata['mid'].std()
            gdata= tradeData.groupby(['market']).agg({'volume':'sum'})
            etfPosition+= gdata.loc['etf', 'volume']
            futurePosition+= gdata.loc['future', 'volume']
            gdata= tradeData.groupby(['market', 'volume']).agg({'price':'mean','time':'count'})
            nintraDayTrade= gdata['time'].min()
            for index, row in gdata.iterrows():
                if index[0]== 'etf':
                    PL+= index[1]* row.time* (etfClose- row.price)
                elif index[0]== 'future':
                    PL+= index[1]* row.time* (futureClose- row.price)* fMultiply
            nparams= np.r_[nparams, np.array([[midMean, midStd, dataOV]])]
            nparams= nparams[1:,:]
            pdDTradeInfo.loc[tradeDate]= [etfClose,futureClose,etfPosition,futurePosition, nintraDayTrade,
                                        midMean, midStd, dataOV, PL]
        
    return listTradeData, position, pdDTradeInfo
        

if __name__ == '__main__':
    jqFutureID= 'IC2006.CCFX'
    jqETFID= '510500.XSHG'
    sdate= '20200401' # date(2020,6,1)
    etfOrderVolumns= 0
    
    dataOV, mdata= getDailyPairStat(jqETFID, jqFutureID, sdate, etfOrderVolumns)
    
    mmean= np.round(mdata['mid'].mean())
    mstd= np.round(mdata['mid'].std())
    #etfOrderVolumns= dataOV
    
    nTestDay= 40
    tradeThreshold= 1
    positionThreshold= 10
    nTradeSilence= 10
    nParamDay= 5
    #tradeData, position= simuDay(mdata, runParams, tradeThreshold , positionThreshold, nTradeSilence, iniPosition= 0)
    #dataOV, mdata= getDailyPairStat(jqETFID, jqFutureID, ndate, etfOrderVolumns)
    
    #mdata[['lesf', 'self']].plot()
    #mdata['mid'].mean()
    listTradeData, position, pdDTradeInfo= PairTradeStrategy(jqETFID, jqFutureID, sdate, nParamDay, tradeThreshold , positionThreshold, nTestDay)
    #tradeData= listTradeData[0]
    
