# -*- coding: utf-8 -*-
"""
Created on Thu Aug  6 18:24:59 2020

@author: Administrator
"""

import pandas as pd
import dtcfg as cfg
import time as ti

startDate= 20200720
endDate= 20200801

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
        #tdate= 20200729
        listDate= localSQL.getDate('SRDT')
        NfutureCash= 0
        for tdate in listDate:
            if tdate< startDate or tdate>= endDate:
                continue
            print('dealing edit date: %d, time elapsed: %.2f min'% (tdate, (ti.time() - t0)/60))
            sqlword= "select * from pengzf.ETF_trorder where trday='%d'"% tdate
            data= pd.read_sql(sqlword, con=recordSQL.conn)
            data= data.drop(['xh', 'strategy', 'insertime'], axis= 1)
            for col in data.columns:
                data[col]= data[col].map(cfg.strip)
            unfinishData= pd.DataFrame(columns= cfg.col_record)
            errorData= pd.DataFrame(columns= cfg.col_record)
            mySRData= data[(data['mark'].isin(['虚拟申购', '虚拟赎回']))]
            myRecordData= data[(data['mark'].isin(['虚拟申购抵消', '虚拟赎回抵消']))]
            for i in range(len(mySRData)):
                record= mySRData.iloc[i]
                etfcode= record.stkcd[:6]
                #checkData= pd.DataFrame(columns= ['check'])
                if record.mark== '虚拟申购' and len(myRecordData[(myRecordData['stkcd']== record.stkcd)&(myRecordData['mark']== '虚拟申购抵消')])== 0:
                    subData= localSQL.getsubData(etfcode, tdate)
                    subData= subData[subData['busi_date']< endDate]
                    checkData= cfg.getConsCheckSubData(localSQL, etfcode, subData, tdate)
                    if checkData['check'].sum()> 0:
                        fcash= -subData['fund_chg'].sum()- record.trvolume
                        print('sub, tdate: %d,etfcode= %s, fcash: %0.2f'% (tdate, etfcode, fcash))
                        NfutureCash+= fcash
                elif record.mark== '虚拟赎回' and len(myRecordData[(myRecordData['stkcd']== record.stkcd)&(myRecordData['mark']== '虚拟赎回抵消')])== 0:
                    redeemData= localSQL.getRedeemData(etfcode, tdate)
                    redeemData= redeemData[redeemData['busi_date']< endDate]
                    checkData= cfg.getConsCheckRedeemData(localSQL, etfcode, redeemData, tdate)
                    if checkData['check'].sum()> 0:
                        fcash= -redeemData['fund_chg'].sum()- record.trvolume
                        print('redeem, tdate: %d,etfcode= %s, fcash: %0.2f'% (tdate, etfcode, fcash))
                        NfutureCash+= fcash
        print('NfutureCash: %.2f'% NfutureCash)
        
    cfg.timeEnd(t0)
    
    