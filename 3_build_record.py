# -*- coding: utf-8 -*-
"""
Created on Sat Jul 18 00:27:39 2020

@author: WAP
"""
from os import path
import pandas as pd
import time as ti
import warnings
import dtcfg as cfg
from WindPy import w
w.start()
warnings.filterwarnings('ignore')

def checkCode(theroVol, transVol, tradeVol, transFund):
    if transVol+ tradeVol!= 0:
        return 1
    elif theroVol+ transVol!= 0 and transFund== 0:
        return 1
    elif theroVol+ transVol== 0 and transFund!= 0:
        return 1
    else:
        return 0
    
def getSRInfo(date, code):
    data= w.wss(etfcode + ".OF", "fund_etfpr_minnav","unit=1;tradeDate=%s"% tdate)
    return data.Data[0][0]

def getETFConstituent(date, code):
    filePath= "constituent\%d_%s.csv"% (date, code)
    if path.exists(filePath):
        mypdData= pd.read_csv(filePath, index_col= 0, encoding='GB18030')
        mypdData.index= mypdData.index.map(lambda x:str(x).zfill(6))
        return mypdData
    nloop= 0
    while nloop< 20:
        data=w.wset("etfconstituent","date=%s;windcode=%s.OF"%(date,code))
        if data.ErrorCode!=0:
            print('getETFConstituent error, errorCode: %d'% data.ErrorCode)
            nloop+= 1
            ti.sleep(5)
        else:
            nloop= 21

    pdData=pd.DataFrame(data.Data).T
    pdData.columns=data.Fields
    data=w.wss(','.join(list(pdData.wind_code)), "pre_close,close","tradeDate=%s;priceAdj=U;cycle=D"%date)
    closeData=pd.DataFrame(data.Data).T
    closeData.columns=data.Fields
    pdData=pdData.join(closeData)
    pdData.index=pdData['wind_code'].map(lambda x:x[:6])
    pdData.drop(['date','wind_code'], axis=1, inplace=True)
    pdData.to_csv(filePath, encoding='GB18030')
    return pdData

def getConsCheckSubData(etfcode, data, tdate):
    etfUnit= getSRInfo(tdate, etfcode)
    pdData= getETFConstituent(tdate, etfcode)
    lsdata= data[(data['business_name']== 'ETF申购')]
    pdData['theroVol']= int(lsdata['sec_chg'].sum()/ etfUnit)* pdData['volume']
    lsdata= pdData[pdData['cash_substitution_mark']=='必须']
    pdData.loc[lsdata.index, 'theroVol']= 0
    lsdata= data[(data['business_name']== '申购赎回过出')].groupby('sec_code')['sec_chg'].sum()
    lsdata.name='transVol'
    pdData= pdData.join(lsdata)
    lsdata= data[(data['business_name']== '证券买入')].groupby('sec_code')['sec_chg'].sum()
    lsdata.name= 'tradeVol'
    pdData= pdData.join(lsdata)
    lsdata= data[data['business_name'].isin(['现金替代补款', '现金替代退款'])].groupby('relative_code')['fund_chg'].sum()
    lsdata.name= 'transFund'
    pdData= pdData.join(lsdata)
    pdData.fillna(0, inplace= True)
    pdData['check']= pdData.apply(lambda x:checkCode(x.theroVol, x.transVol, x.tradeVol, x.transFund), axis= 1)
    return pdData

def getConsCheckRedeemData(etfcode, data, tdate):
    etfUnit= getSRInfo(tdate, etfcode)
    pdData= getETFConstituent(tdate, etfcode)
    lsdata= data[(data['business_name']== 'ETF赎回')]
    pdData['theroVol']= int(lsdata['sec_chg'].sum()/ etfUnit)* pdData['volume']
    lsdata= pdData[pdData['cash_substitution_mark']=='必须']
    pdData.loc[lsdata.index, 'theroVol']= 0
    lsdata= data[(data['business_name']== '申购赎回过入')].groupby('sec_code')['sec_chg'].sum()
    lsdata.name='transVol'
    pdData= pdData.join(lsdata)
    lsdata= data[(data['business_name']== '证券卖出')].groupby('sec_code')['sec_chg'].sum()
    lsdata.name= 'tradeVol'
    pdData= pdData.join(lsdata)
    lsdata= data[data['business_name'].isin(['现金替代补款', '现金替代退款'])].groupby('relative_code')['fund_chg'].sum()
    lsdata.name= 'transFund'
    pdData= pdData.join(lsdata)
    pdData.fillna(0, inplace= True)
    pdData['check']= pdData.apply(lambda x:checkCode(x.theroVol, x.transVol, x.tradeVol, x.transFund), axis= 1)
    return pdData
  
def getSubRecord(etfcode, data, record):
    pdr= pd.DataFrame(columns= xlsdata.columns)
    #虚拟申购抵消
    record['交易股数']= -record['交易股数']
    record['交易金额']= -record['交易金额']
    record['备注']= '虚拟申购抵消'
    pdr= pdr.append(record, ignore_index= True)
    #申购
    lsdata=data[(data['sec_type']== 'etf')|(data['sec_chg']== 0)]
    record['备注']= '申购'
    record['交易股数']= lsdata['sec_chg'].sum()
    record['交易金额']= 0
    record['交易费用及税金']= -lsdata['fund_chg'].sum()
    record['交易价格']= 0
    pdr= pdr.append(record, ignore_index= True)
    #买入
    lsdata=data[data['business_name']=='证券买入']\
                        .groupby('sec_code')['fund_chg', 'sec_chg', 'done_amt'].sum()
    record['备注']= ''
    record['证券简称']= ''
    for scode in lsdata.index:
        record['证券代码']= scode+ ' CH Equity'
        record['交易股数']= lsdata.loc[scode, 'sec_chg']
        record['交易金额']= lsdata.loc[scode, 'done_amt']
        record['交易价格']= record['交易金额']/ record['交易股数']
        record['交易费用及税金']= -lsdata.loc[scode, 'fund_chg']-record['交易金额']
        pdr= pdr.append(record, ignore_index= True)
    #过出
    lsdata=data[(data['sec_type']!= 'etf')&(data['business_name']!='证券买入')&((data['sec_chg']!= 0))]\
                        .groupby('sec_code')['fund_chg', 'sec_chg', 'done_amt'].sum()
    record['备注']= '申购赎回过出'
    for scode in lsdata.index:
        record['证券代码']= scode+ ' CH Equity'
        record['交易股数']= lsdata.loc[scode, 'sec_chg']
        record['交易金额']= 0
        record['交易价格']= 0
        record['交易费用及税金']= -lsdata.loc[scode, 'fund_chg']
        pdr= pdr.append(record, ignore_index= True)
    return pdr

def getRedeemRecord(etfcode, data, record):
    pdr= pd.DataFrame(columns= xlsdata.columns)
    #虚拟赎回抵消
    #record= xlsdata[(xlsdata['证券代码']== etfcode+ ' CH Equity')&(xlsdata['备注']== '虚拟赎回')].iloc[0]
    record['交易股数']= -record['交易股数']
    record['交易金额']= -record['交易金额']
    record['备注']= '虚拟赎回抵消'
    pdr= pdr.append(record, ignore_index= True)
    #赎回
    lsdata=data[(data['sec_type']== 'etf')|(data['sec_chg']== 0)]
    record['备注']= '赎回'
    record['交易股数']= lsdata['sec_chg'].sum()
    record['交易金额']= 0
    record['交易费用及税金']= -lsdata['fund_chg'].sum()
    record['交易价格']= 0
    pdr= pdr.append(record, ignore_index= True)
    #卖出
    lsdata=data[data['business_name']=='证券卖出']\
                        .groupby('sec_code')['fund_chg', 'sec_chg', 'done_amt'].sum()
    record['备注']= ''
    record['证券简称']= ''
    for scode in lsdata.index:
        record['证券代码']= scode+ ' CH Equity'
        record['交易股数']= lsdata.loc[scode, 'sec_chg']
        record['交易金额']= -lsdata.loc[scode, 'done_amt']
        record['交易价格']= record['交易金额']/ record['交易股数']
        record['交易费用及税金']= -lsdata.loc[scode, 'fund_chg']- record['交易金额']
        pdr= pdr.append(record, ignore_index= True)
    #过入
    lsdata=data[(data['sec_type']!= 'etf')&(data['business_name']!='证券卖出')&((data['sec_chg']!= 0))]\
                        .groupby('sec_code')['fund_chg', 'sec_chg', 'done_amt'].sum()
    record['备注']= '申购赎回过入'
    for scode in lsdata.index:
        record['证券代码']= scode+ ' CH Equity'
        record['交易股数']= lsdata.loc[scode, 'sec_chg']
        record['交易金额']= 0
        record['交易价格']= 0
        record['交易费用及税金']= -lsdata.loc[scode, 'fund_chg']
        pdr= pdr.append(record, ignore_index= True)
    return pdr

if __name__ == '__main__':
    t0 = cfg.timeStart()

    localSQL= cfg.MSSQL(cfg.host, cfg.user, cfg.pwd, cfg.lcdb)
    localSQL.Connect()
    if not localSQL.isConnect:
        print(localSQL.host + ' not connect')
    else:
        listDate= localSQL.getDate('SRDT')
        srData=pd.DataFrame()
        for tdate in listDate:
            #tdate= listDate[0]
            print('dealing date: %d, time elapsed: %.2f min'% (tdate, (ti.time() - t0)/60))
            xlsdata = cfg.getxlsData("每日申赎\SR_Records%d.xlsx"% tdate)
            #diffRecord= pd.DataFrame(columns= xlsdata.columns)
            for i in range(len(xlsdata)):
                record= xlsdata.iloc[i]
                etfcode= record['证券代码'][:6]
                if record['备注']== '虚拟申购':
                    srMark= 'sub'
                    filePath= "record\%d_%s_%s.xls"%(tdate, etfcode, srMark)
                    if path.exists(filePath):
                        continue
                    print('dealing sub data: '+ etfcode)
                    data= localSQL.getsubData(etfcode, tdate)
                    checkData= getConsCheckSubData(etfcode, data, tdate)
                    pdr= getSubRecord(etfcode, data, record)
                elif record['备注']== '虚拟赎回':
                    srMark= 'redeem'
                    filePath= "record\%d_%s_%s.xls"%(tdate, etfcode, srMark)
                    if path.exists(filePath):
                        continue
                    print('dealing redeem data: '+ etfcode)
                    data= localSQL.getRedeemData(etfcode, tdate)
                    checkData= getConsCheckRedeemData(etfcode, data, tdate)
                    pdr= getRedeemRecord(etfcode, data, record)
                xlsdata.loc[record.name, '交易费用及税金']= pdr.iloc[1:,-3:-1].sum().sum()
                xlsdata.loc[record.name, '内部抵消']= checkData['check'].sum()
                with pd.ExcelWriter("record\%d_%s_%s.xls"%(tdate, etfcode, srMark)) as writer:
                    pdr.to_excel(writer, sheet_name= 'record', index= False)
                    data.to_excel(writer, sheet_name= 'transflow', index= False)
                    checkData.to_excel(writer, sheet_name= 'check', index= False)
            srData= pd.concat([srData, xlsdata], axis= 0, ignore_index= True)
        srData.to_csv('每日申赎\srData.csv', index=0, encoding='GB18030')

    cfg.timeEnd(t0)
    
    