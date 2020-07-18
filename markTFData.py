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
    else:
        return code
    
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
    data['codeType']= data['relative_code'].map(codeType)
    lsdata= data[data['codeType']=='etf']
    data.loc[lsdata.index, 'mark']= lsdata['relative_code'].map(normETFCode)
    lsdata= data[data['codeType']=='other']
    data.loc[lsdata.index, 'mark']= 'cash'
    undoData= data[data['mark']=='unmarked']
    undoData['codeType']= undoData['sec_code'].map(codeType)
    lsdata= undoData[undoData['codeType']=='etf']
    data.loc[lsdata.index, 'mark']= lsdata['sec_code'].map(normETFCode)
    undoData= data[data['mark']=='unmarked']
    
    #undoData.iloc[0]
    lsdata= undoData.groupby('sec_code')['serial_no'].count()
    for scode in lsdata.index:
        #scode= lsdata.index[-1]#for scode in lsdata.index:
        
        sdata= data[(data['sec_code']== scode)&((data['business_name']== '证券买入')|(data['business_name']== '申购赎回过出'))]
        sdata['isMatched']=False
        nLsData= len(sdata)
        for iRow in range(nLsData):
            if sdata.iloc[iRow]['business_name']=='申购赎回过出':
                cRow= 0
                qty= -sdata.iloc[iRow]['sec_chg']
                while cRow<nLsData:
                    if sdata.iloc[cRow]['business_name']=='证券买入' and sdata.iloc[cRow]['sec_chg']==qty and sdata.iloc[cRow]['isMatched']==False:
                        sdata.iloc[cRow,13]=sdata.iloc[iRow]['mark']
                        sdata.iloc[cRow,15]=True
                        sdata.iloc[iRow,15]=True
                        break
                    else:
                        cRow+=1
        data.loc[sdata.index, 'mark']= sdata['mark']
                        
        sdata= data[(data['sec_code']== scode)&((data['business_name']== '证券卖出')|(data['business_name']== '申购赎回过入'))]
        sdata['isMatched']=False
        nLsData= len(sdata)
        for iRow in range(nLsData):
            if sdata.iloc[iRow]['business_name']=='申购赎回过入':
                cRow= 0
                qty= -sdata.iloc[iRow]['sec_chg']
                while cRow<nLsData:
                    if sdata.iloc[cRow]['business_name']=='证券卖出' and sdata.iloc[cRow]['sec_chg']==qty and sdata.iloc[cRow]['isMatched']==False:
                        sdata.iloc[cRow,13]=sdata.iloc[iRow]['mark']
                        sdata.iloc[cRow,15]=True
                        sdata.iloc[iRow,15]=True
                        break
                    else:
                        cRow+=1
        data.loc[sdata.index, 'mark']= sdata['mark']
        
    undoData= data[data['mark']=='unmarked']
    for inde in undoData.index:
        scode= undoData.loc[inde, 'sec_code']
        qty= undoData.loc[inde, 'sec_chg']
        if undoData.loc[inde, 'business_name']== '证券买入':
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
    filePath= path.join(cfg.jqDataPath, 'data.csv')
    alldata= pd.read_csv(filePath, index_col=0, encoding='GB18030')
    tdate= alldata.groupby('busi_date')['serial_no'].count()
    
    #alldata['sec_code']= alldata['sec_code'].map(codeStr)
    #alldata['fscode']=alldata['sec_code'].map(lambda x:x[0])
    #lsdata=alldata[(alldata['fscode']!='0')&(alldata['fscode']!='1')&(alldata['fscode']!='3')&(alldata['fscode']!='5')&(alldata['fscode']!='6')]
    
    
    data= alldata[alldata['busi_date']== tdate.index[1]]
    data['sec_code']= data['sec_code'].map(codeStr)
    data['relative_code']= data['relative_code'].map(codeStr)
    data['contract_no']= data['contract_no'].map(myInt)
    
    data= getMarkData(data)
    
    lsdata= data.groupby(['mark','sec_code'])['sec_chg','fund_chg'].sum()
    lsdata= lsdata[lsdata['sec_chg']!= 0]

    """
    lsdata= data[(data['business_name']=='ETF申购')|(data['business_name']=='ETF赎回')]
    for i in range(len(lsdata)):
        rcn= lsdata.iloc[i,9]
        dn= lsdata.iloc[i,8]
        scode= lsdata.iloc[i,4]
        srdata= undoData[(undoData['rpt_contract_no']== rcn)&(undoData['sec_type']!= '55')]
        data.loc[srdata.index, 'mark']= scode
    
    undoData= data[data['mark']=='unmarked']
    
    lsdata= undoData[(undoData['business_name']!= '证券买入')&(undoData['business_name']!= '证券卖出')]
    
    
    """
    
    
    
    