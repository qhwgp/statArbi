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
#from WindPy import w
#w.start()
warnings.filterwarnings('ignore')
global serSRInfo

def getxlsData(localSQL, xlsdata, tdate, srData):
    for i in range(len(xlsdata)):
        record= xlsdata.iloc[i]
        if record['countervail']== 0:
                continue
        etfcode= record['stkcd'][:6]
        trader= record['trader']
        if record['mark']== '虚拟申购':
            srMark= 'sub'
            print('dealing sub data: '+ etfcode)
            data= localSQL.getsubData(etfcode, tdate)
            checkData= cfg.getConsCheckSubData(localSQL, etfcode, data, tdate)
            pdr= cfg.getSubRecord(etfcode, data, record)
        elif record['mark']== '虚拟赎回':
            srMark= 'redeem'
            print('dealing redeem data: '+ etfcode)
            data= localSQL.getRedeemData(etfcode, tdate)
            checkData= cfg.getConsCheckRedeemData(localSQL, etfcode, data, tdate)
            pdr= cfg.getRedeemRecord(etfcode, data, record)
        try:
            xlsdata.loc[record.name, 'commission']= pdr.iloc[1:,-3:-1].sum().sum()
            xlsdata.loc[record.name, 'countervail']= checkData['check'].sum()
        except:
            xlsdata.loc[record.name, 'countervail']= -1
        with pd.ExcelWriter("record\%d_%s_%s_%s.xls"%(tdate, etfcode, srMark, trader)) as writer:
            pdr.to_excel(writer, sheet_name= 'record', index= False)
            data.to_excel(writer, sheet_name= 'transflow', index= False)
            checkData.to_excel(writer, sheet_name= 'check', index= True)
    srData= pd.concat([srData, xlsdata], axis= 0, ignore_index= True)
    xlsdata.to_csv('log\srData_%d.csv'% tdate, index=0, encoding='GB18030')
    return srData

if __name__ == '__main__':
    t0 = cfg.timeStart()
    localSQL= cfg.MSSQL(cfg.host, cfg.user, cfg.pwd, cfg.lcdb)
    localSQL.Connect()
    recordSQL= cfg.MSSQL(cfg.host, cfg.user, cfg.pwd, cfg.rcddb)
    recordSQL.Connect()
    if not localSQL.isConnect:
        print(localSQL.host + ' not connect')
    elif not recordSQL.isConnect:
        print(recordSQL.host + ' not connect')
    else:
        listDate= localSQL.getDate('SRDT')
        srData=pd.DataFrame(columns= cfg.col_record)
        for tdate in listDate[:-2]:
            #tdate= listDate[0]
            logpath= 'log\srData_%d.csv'% tdate
            if path.exists(logpath):
                xlsdata= pd.read_csv(logpath, encoding='GB18030')
                print('dealing edit date: %d, time elapsed: %.2f min'% (tdate, (ti.time() - t0)/60))
            else:
                xlsdata= recordSQL.getRecordData(tdate)
                xlsdata['countervail']= 100
                print('dealing new date: %d, time elapsed: %.2f min'% (tdate, (ti.time() - t0)/60))
            srData= getxlsData(localSQL, xlsdata, tdate, srData)
        srData.to_csv('srData_%s.csv'% ti.strftime('%m%d',ti.localtime(ti.time())), index=0, encoding='GB18030')
    cfg.timeEnd(t0)
    
    