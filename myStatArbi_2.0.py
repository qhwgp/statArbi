# -*- coding: utf-8 -*-
"""
Created on Wed Jul  1 16:03:45 2020

@author: WAP
"""

import myjqcfg as cfg
import pandas as pd
import numpy as np
import time as ti



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
    return unitETFVolumns, basisMean, basisStd

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
    unitETFVolumns, basisMean, basisStd= getPairStatInfo(jqETFID, jqFutureID, listTradeDay, nIndex, nParamDay)
    #pdTradeData, position, pdDTradeInfo= PairTradeStrategy(jqETFID, jqFutureID, sdate, nParamDay, tradeThreshold , positionThreshold, nTestDay)
    print('day: '+ listTradeDay[-1])
    print('unitETFVolumns: %d'% unitETFVolumns)
    print('basisMean: %d'% basisMean)
    print('basisStd: %d'% basisStd)

    print('All done, time elapsed: %.2f min' % ((ti.time() - t0)/60))



























