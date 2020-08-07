# -*- coding: utf-8 -*-
"""
Created on Sat Jul 18 00:27:39 2020

@author: WAP
get data to SRDT, marked data
"""
import dtcfg as cfg

if __name__ == '__main__':
    t0 = cfg.timeStart()
    localSQL= cfg.MSSQL(cfg.host, cfg.user, cfg.pwd, cfg.lcdb)
    localSQL.Connect()
    if not localSQL.isConnect:
        print(localSQL.host + ' not connect')
    else:
        listDate= localSQL.getDate('TFDT')
        listMarkDate= localSQL.getDate('SRDT')
        for tdate in listDate:
            if tdate in listMarkDate:
                continue
            #tdate= listDate[0]
            print('deal data date: %d'% tdate)
            data= localSQL.getUnmarkedData(tdate)  
            data= cfg.getMarkData(data, tdate)
            if not cfg.checkMarkData(data):
                break
            localSQL.updateMarkData(data, tdate)
        cfg.updateRCNPatch(localSQL)
    cfg.timeEnd(t0)
    
    #unmarkdata= localSQL.getUnmarkedData(99999999)
    #lsdata= unmarkdata[unmarkdata['sec_code']=='600460']
    #pdata= unmarkdata.groupby('sec_code')['sec_chg'].sum()
    #cfg.rebuildSRDT(localSQL)