# -*- coding: utf-8 -*-
"""
Created on Mon Aug  3 09:06:12 2020

@author: Administrator
"""

import pandas as pd
import random
import time as ti
import dtcfg as cfg
from WindPy import w
w.start()


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
        listDate= localSQL.getDate('TFDT')
        srData=pd.DataFrame()
        for tdate in listDate:
            if tdate< cfg.startDate:
                continue
            #tdate= listDate[0]
            print('dealing date: %d, time elapsed: %.2f min'% (tdate, (ti.time() - t0)/60))
            sqlword="SELECT distinct sec_code FROM TFDT where busi_date=%d and business_name in ('ETF申购','ETF赎回')"% tdate
            fdata= pd.read_sql(sqlword, con=localSQL.conn)
            for i in range(len(fdata)):
                etfcode= fdata.iloc[i,0]
                pdData= localSQL.getETFConstituent(tdate, etfcode)
                if len(pdData)== 0:
                    ti.sleep(random.random()*10)
                    data=w.wset("etfconstituent","date=%s;windcode=%s.OF"%(tdate, etfcode))
                    if data.ErrorCode!=0:
                        print('getETFConstituent error, errorCode: %d, etfcode: %s'% (data.ErrorCode, etfcode))
                    else:
                        pdData=pd.DataFrame(data.Data).T
                        if len(pdData)==0:
                            pdData= pd.DataFrame(data=[['000000', 0, etfcode, tdate]], columns= ['wind_code', 'volume', 'etf_code', 'busi_date'])
                        else:
                            pdData.columns=data.Fields
                            data=w.wss(','.join(list(pdData.wind_code)), "pre_close,close","tradeDate=%s;priceAdj=U;cycle=D"% tdate)
                            closeData=pd.DataFrame(data.Data).T
                            closeData.columns=['PRE_CLOSE', 'DAY_CLOSE']#data.Fields
                            pdData=pdData.join(closeData)
                            pdData.index=pdData['wind_code'].map(lambda x:x[:6])
                            pdData.drop(['date','wind_code'], axis=1, inplace=True)
                            pdData['etf_code']= etfcode
                            pdData['busi_date']= tdate
                        pdData.to_sql('constituentDT', con= localSQL.engine, if_exists= 'append', index= True)
                #srinfo
                etfUnit= localSQL.getSRInfo(tdate, etfcode)
                if etfUnit== 0:
                    ti.sleep(random.random()*10)
                    data= w.wss(etfcode + ".OF", "fund_etfpr_minnav","unit=1;tradeDate=%s"% tdate)
                    if data.ErrorCode!=0:
                        print('fund_etfpr_minnav error, errorCode: %d, etfcode: %s'% (data.ErrorCode, etfcode))
                    else:
                        try:
                            edata= data.Data[0][0]
                            if edata< 1:
                                edata= 1
                        except:
                            edata= 1
                        sqlword= "insert into SRInfoDT values ('%s', %s, %d)"% (etfcode, tdate, edata)
                        localSQL.cur.execute(sqlword)
                        
                
                
                
                
                
                
                
                
                
                
                