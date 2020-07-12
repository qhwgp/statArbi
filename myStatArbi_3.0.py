# -*- coding: utf-8 -*-
"""
Created on Wed Jul  1 16:03:45 2020

@author: WAP
"""

import myjqcfg as cfg
import pandas as pd
import numpy as np
import time as ti

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

def getPairQuote(jqETFID, jqFutureID, sdate, unitETFVolumns):
    fMultiply= cfg.getFutureMultiply(jqFutureID)
    mdata= cfg.getMergeData(jqETFID, jqFutureID, sdate)
    mdata['bidp']= mdata.apply(lambda x:getMDPrice(x.b1_p, x.b2_p, x.b3_p, x.b4_p, x.b5_p, x.b1_v, x.b2_v, x.b3_v, x.b4_v, x.b5_v, unitETFVolumns), axis = 1)
    mdata['askp']= mdata.apply(lambda x:getMDPrice(x.a1_p, x.a2_p, x.a3_p, x.a4_p, x.a5_p, x.a1_v, x.a2_v, x.a3_v, x.a4_v, x.a5_v, unitETFVolumns), axis = 1)
    mdata= mdata[['bidp', 'askp', 'fb1_p', 'fa1_p']]
    mdata['lesf']= mdata['askp']*unitETFVolumns- mdata['fb1_p']* fMultiply
    mdata['self']= mdata['bidp']*unitETFVolumns- mdata['fa1_p']* fMultiply
    return mdata

def getPairStatInfo(jqETFID, jqFutureID, listTradeDay, nIndex, nParamDay= 5):
    listdata= []
    for i in range(nParamDay):
        mdata= cfg.getMergeData(jqETFID, jqFutureID, listTradeDay[nIndex- 1- i])
        listdata.append(mdata[['current', 'fcurrent']])
    mdata= pd.concat(listdata)
    inid= mdata.mean()
    fMultiply= cfg.getFutureMultiply(jqFutureID)
    unitETFVolumns= np.round(inid['fcurrent']* fMultiply/ inid['current'], -2)
    mdata['basis']= mdata['current']*unitETFVolumns- mdata['fcurrent']* fMultiply
    basisMean= np.round( mdata['basis'].mean())
    basisStd= np.round( mdata['basis'].std())
    return [unitETFVolumns, basisMean, basisStd]

def drawBOLL(jqETFID, jqFutureID, listTradeDay, nIndex, nParamDay, pdTrade, iniPosition):
    statInfo= getPairStatInfo(jqETFID, jqFutureID, listTradeDay, nIndex, nParamDay)
    pqdata= getPairQuote(jqETFID, jqFutureID, listTradeDay[nIndex], statInfo[0])
    pqdata= pqdata[['lesf', 'self']]
    pqdata['uBOLL']= statInfo[1]+ statInfo[2]
    pqdata['dBOLL']= statInfo[1]- statInfo[2]
    pdft= pdTrade[pdTrade['type']== 'future']
    pdft.index= pdft['time']
    pqdata= pd.merge(pqdata, pdft['volume'], left_index=True, right_index=True, how='outer')
    pqdata.iloc[0, 4]= iniPosition.loc['future', 'volume']
    pqdata.fillna(0, inplace=True)
    pqdata['position']= pqdata['volume'].cumsum()
    pqdata.drop(['volume'], axis= 1, inplace=True)
    fname= 'BOLL_%s_%s_%s.png'% (jqETFID, jqFutureID, listTradeDay[nIndex])
    cfg.drawFigure(pqdata, fname, 'BOLL Line in ' + listTradeDay[nIndex], ['position'])
    return
   
def checkTrade(row, statInfo, tradeThreshold, positionThreshold, levelThreshold, position):
    level= position/ positionThreshold
    if row['lesf']< statInfo[1]- np.ceil(level+ 0.0001)* tradeThreshold* statInfo[2] and position< positionThreshold* levelThreshold:
        return 'buy'
    elif row['self']> statInfo[1]+ np.ceil(-level+ 0.0001)* tradeThreshold* statInfo[2] and position> -positionThreshold* levelThreshold:
        return 'sell'
    else:
        return 'hold'   
    
def simuDay(jqETFID, jqFutureID, listTradeDay, nIndex, tradeParam, iniPosition):
    pdTrade= pd.DataFrame(columns=('time', 'symbol', 'type', 'volume', 'price'))
    statInfo= getPairStatInfo(jqETFID, jqFutureID, listTradeDay, nIndex, tradeParam['nParamDay'])
    pqdata= getPairQuote(jqETFID, jqFutureID, listTradeDay[nIndex], statInfo[0])
    if not 'future' in iniPosition.index:
        iniPosition.loc['future']= [0,0]
    if not 'etf' in iniPosition.index:
        iniPosition.loc['etf']= [0,0]
    fposition= -iniPosition.loc['future', 'volume']
    etfPosition= iniPosition.loc['etf', 'volume']
    diffETF= statInfo[0]* fposition- etfPosition
    if diffETF!= 0:
        row= pqdata.iloc[0]
        if diffETF> 0.1:
            pdTrade= pdTrade.append({'time': row.name, 'symbol': jqETFID, 'type': 'etf', 'volume': diffETF, 'price': row['askp']}, ignore_index=True)
        elif diffETF< -0.1:
            pdTrade= pdTrade.append({'time': row.name, 'symbol': jqETFID, 'type': 'etf', 'volume': diffETF, 'price': row['bidp']}, ignore_index=True)
    nNearTrade= 0
    tradeSignal= 'hold'
    for time, row in pqdata.iterrows():
        #check if near trade
        if nNearTrade> 0.1:
            nNearTrade-= 1
        elif tradeSignal!= 'hold':#check tradeSignal
            nNearTrade= tradeParam['nTradeSilence']
            if tradeSignal== 'buy':
                pdTrade= pdTrade.append({'time': time, 'symbol': jqETFID, 'type': 'etf', 'volume': statInfo[0], 'price': row['askp']}, ignore_index=True)
                pdTrade= pdTrade.append({'time': time, 'symbol': jqFutureID, 'type': 'future', 'volume': -1, 'price': row['fb1_p']}, ignore_index=True)
                fposition+= 1
            elif tradeSignal== 'sell':
                pdTrade= pdTrade.append({'time': time, 'symbol': jqETFID, 'type': 'etf', 'volume': -statInfo[0], 'price': row['bidp']}, ignore_index=True)
                pdTrade= pdTrade.append({'time': time, 'symbol': jqFutureID, 'type': 'future', 'volume': 1, 'price': row['fa1_p']}, ignore_index=True)
                fposition-= 1
            tradeSignal= 'hold'
        else:#check trade
            tradeSignal= checkTrade(row, statInfo, tradeParam['tradeThreshold'], tradeParam['positionThreshold'], 
                                    tradeParam['levelThreshold'], fposition)
         
    pdPosition= pdTrade.groupby(['type']).agg({'volume':'sum', 'price':'count'})
    if not 'future' in pdPosition.index:
        pdPosition.loc['future']= [0,0]
    if not 'etf' in pdPosition.index:
        pdPosition.loc['etf']= [0,0]
    pdPosition['volume']= pdPosition['volume'].add(iniPosition['volume'])
    statInfo.append(pdPosition.loc['future', 'volume'])
    row= pqdata.iloc[-1]
    etfClose= (row.bidp+ row.askp) /2
    futureClose= (row.fb1_p+ row.fa1_p) /2
    pdPosition.loc['etf', 'price']= etfClose
    pdPosition.loc['future', 'price']= futureClose
    fMultiply= cfg.getFutureMultiply(jqFutureID)
    nintraDayTrade= 0
    PL= etfPosition* (etfClose- iniPosition.loc['etf', 'price'])+ iniPosition.loc['future', 'volume']* (futureClose- iniPosition.loc['future', 'price'])* fMultiply
    if 'future' in pdTrade.type.values:
        gdata= pdTrade.groupby(['type', 'volume']).agg({'price':'mean','time':'count'})
        nintraDayTrade= gdata.loc[('future',slice(None)),'time'].max()
        for index, row in gdata.iterrows():
            if index[0]== 'etf':
                PL+= index[1]* row.time* (etfClose- row.price)
            elif index[0]== 'future':
                PL+= index[1]* row.time* (futureClose- row.price)* fMultiply
    statInfo.append(nintraDayTrade)
    statInfo.append(PL)  
    return pdPosition, statInfo

def getNextMonthFutureID(future, strDate):
    year= int(strDate[2:4])
    month= int(strDate[4:6])
    if month== 12:
        strnm= str(year+1)+ '01'
    else:
        strnm= str(year)+ str(month+ 1).zfill(2)
    return '%s%s.CCFX'% (future, strnm)

def PairTradeStrategy(jqETFID, future, tradeParam, listTradeDay):
    pdTradeInfo= pd.DataFrame(columns=('etfOrderVolumns', 'midMean', 'midStd', 'futurePosition', 'nintraDayTrade', 'PL'))
    lastjqFutureID= ''
    iniPosition= pd.DataFrame(columns=('volume', 'price'))
    for nIndex in range(sIndex, len(listTradeDay)):
        tday= listTradeDay[nIndex]
        jqFutureID= getNextMonthFutureID(future, tday)
        if jqFutureID!= lastjqFutureID:
            iniPosition= pd.DataFrame(columns=('volume', 'price'))
        pdPosition, statInfo= simuDay(jqETFID, jqFutureID, listTradeDay, nIndex, tradeParam, iniPosition)
        #drawBOLL(jqETFID, jqFutureID, listTradeDay, nIndex, tradeParam['nParamDay'], pdTrade, iniPosition)
        iniPosition= pdPosition
        lastjqFutureID= jqFutureID
        print('day: %s, P&L: %0.2f'% (tday, statInfo[-1]))
        pdTradeInfo.loc[tday]= statInfo
    saveName= jqETFID+ '_'+ future
    for v in tradeParam.values():
        saveName+= '_'+ str(v)
    saveName+= '.csv'
    cfg.saveResult(pdTradeInfo, saveName)
    return

if __name__ == '__main__':
    t0 = ti.time()
    future= 'IF'#'2007.CCFX'
    jqETFID= '510300.XSHG'
    listTradeDay= cfg.getTradeDays()
    tradeParam= cfg.tradeParam
    PairTradeStrategy(jqETFID, future, tradeParam, listTradeDay)
    print('All done, time elapsed: %.2f min' % ((ti.time() - t0)/60))



























