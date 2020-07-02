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

def drawBOLL(jqETFID, jqFutureID, listTradeDay, nIndex, nParamDay= 5):
    statInfo= getPairStatInfo(jqETFID, jqFutureID, listTradeDay, nIndex, nParamDay)
    pqdata= getPairQuote(jqETFID, jqFutureID, listTradeDay[nIndex], statInfo[0])
    pqdata= pqdata[['lesf', 'self']]
    pqdata['uBOLL']= statInfo[1]+ statInfo[2]
    pqdata['dBOLL']= statInfo[1]- statInfo[2]
    pqdata.plot(title='BOLL Line',fontsize= 20, figsize=(40,30)).get_figure().savefig('fig.png')
    return
    

if __name__ == '__main__':
    t0 = ti.time()
    jqFutureID= 'IF2007.CCFX'
    jqETFID= '510330.XSHG'
    #sdate= '20200430' 
    
    listTradeDay= cfg.getTradeDays()
    nIndex= -1
    
    nTestDay= 3
    tradeThreshold= 1
    positionThreshold= 10
    nTradeSilence= 10
    nParamDay= 5
    
    mdata= cfg.getMergeData(jqETFID, jqFutureID, listTradeDay[-1])
    statInfo= getPairStatInfo(jqETFID, jqFutureID, listTradeDay, nIndex, nParamDay)#unitETFVolumns, basisMean, basisStd
    #pdTradeData, position, pdDTradeInfo= PairTradeStrategy(jqETFID, jqFutureID, sdate, nParamDay, tradeThreshold , positionThreshold, nTestDay)
    print('day: '+ listTradeDay[-1])
    print('unitETFVolumns: %d'% statInfo[0])
    print('basisMean: %d'% statInfo[1])
    print('basisStd: %d'% statInfo[2])
    
    pqdata= getPairQuote(jqETFID, jqFutureID, listTradeDay[-1], statInfo[0])
    drawBOLL(jqETFID, jqFutureID, listTradeDay, nIndex, nParamDay)

    print('All done, time elapsed: %.2f min' % ((ti.time() - t0)/60))


























