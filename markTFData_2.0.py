# -*- coding: utf-8 -*-
"""
Created on Sat Jul 18 00:27:39 2020

@author: WAP
"""
from os import path
import myjqcfg as cfg
import pandas as pd
import numpy as np
import time as ti
import myjqcfg as cfg
import warnings
warnings.filterwarnings('ignore')

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
    if myStr[0]== '1' or myStr[0]== '5':
        return 'etf'
    elif myStr[0]== '0' or myStr[0]== '3' or myStr[0]== '6':
        return 'stock'
    else:
        return 'other'
    
def getMarkData(data):
    data['codeType']= data['sec_code'].map(codeType)
    lsdata= data[data['codeType']=='etf']
    data.loc[lsdata.index, 'mark']= lsdata['sec_code'].map(normETFCode)
    lsdata= data[data['codeType']=='other']
    data.loc[lsdata.index, 'mark']= 'cash'
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
    for inde in undoData.index:
        #inde= undoData.index[0]
        scode= undoData.loc[inde, 'sec_code']
        qty= undoData.loc[inde, 'sec_chg']
        lsdata= undoData[(undoData['sec_code']== scode)]
        #wap
        if lsdata.loc[inde, 'business_name']== '证券买入':
            sdata= data[(data['sec_code']== scode)&((data['business_name']== '证券买入')|\
                    (data['business_name']== '申购赎回过出'))].groupby('mark')['sec_chg'].sum()
            for mk in sdata.index:
                if mk== 'unmarked':
                    continue
                if qty< -sdata[mk]+0.000001:
                    data.loc[inde, 'mark']= mk
                    break
        else:
            sdata= data[(data['sec_code']== scode)&((data['business_name']== '证券卖出')|\
                    (data['business_name']== '申购赎回过入'))].groupby('mark')['sec_chg'].sum()
            for mk in sdata.index:
                if mk== 'unmarked':
                    continue
                if -qty< sdata[mk]+0.000001:
                    data.loc[inde, 'mark']= mk
                    break
    return data
    
if __name__ == '__main__':
    t0 = ti.time()
    filePath= path.join(cfg.jqDataPath, 'data.csv')
    alldata= pd.read_csv(filePath, index_col=0, encoding='GB18030')
    
    alldata['sec_code']= alldata['sec_code'].map(codeStr)
    alldata['relative_code']= alldata['relative_code'].map(codeStr)
    alldata['contract_no']= alldata['contract_no'].map(myInt)
    
    listDate= alldata.groupby('busi_date')['serial_no'].count().index
    
    #tdate= listDate[0]
    
    for tdate in listDate:
        print('deal data date: %d, '% tdate)
        data= alldata[(alldata['busi_date']<= tdate)&(alldata['mark']=='unmarked')]
        data= getMarkData(data)
        alldata.loc[data.index, 'mark']= data['mark']
    
    print('All done, time elapsed: %.2f min' % ((ti.time() - t0)/60))
    
    