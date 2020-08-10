# -*- coding: utf-8 -*-
"""
Created on Sun Aug  2 10:23:58 2020

@author: sysadmin
"""
import pandas as pd
import dtcfg as cfg

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
        #tdate= 20200622
        listDate= localSQL.getDate('SRDT')
        sumCash= 0
        for tdate in listDate:
            sqlword= "select * from pengzf.ETF_trorder where trday='%d'"% tdate
            data= pd.read_sql(sqlword, con=recordSQL.conn)
            data= data.drop(['xh', 'strategy', 'insertime'], axis= 1)
            for col in data.columns:
                data[col]= data[col].map(cfg.strip)
                
            unfinishData= pd.DataFrame(columns= cfg.col_record)
            errorData= pd.DataFrame(columns= cfg.col_record)
            mySRData= data[(data['trader']== '郭言心')&(data['mark'].isin(['虚拟申购', '虚拟赎回']))]
            myRecordData= data[(data['trader']== '郭言心')&(data['mark'].isin(['虚拟申购抵消', '虚拟赎回抵消']))]
            nRecord= 0
            
            for i in range(len(mySRData)):
                record= mySRData.iloc[i]
                etfcode= record.stkcd[:6]
                if record.mark== '虚拟申购':
                    if len(myRecordData[(myRecordData['stkcd']== record.stkcd)&(myRecordData['mark']== '虚拟申购抵消')])> 0:
                        continue
                    subData= localSQL.getsubData(etfcode, tdate)
                    checkData= cfg.getConsCheckSubData(localSQL, etfcode, subData, tdate)
                    ncf= checkData['check'].sum()
                    if ncf> 0:
                        record.countervail= ncf
                        unfinishData= unfinishData.append(record)
                        continue
                    pdr= cfg.getSubRecord(etfcode, subData, record)
                    if pdr['quantity'].sum()!= 0:
                        errorData= errorData.append(record)
                        continue
                    cash= pdr[['trvolume', 'commission']].sum().sum()
                    if abs(cash)> 500000:
                        errorData= errorData.append(record)
                        continue
                    sumCash+= cash
                    n= len(pdr)
                    nRecord+= n
                    print('insert sub to temporder, date: %d, etfcode: %s, Nrecord= %d, cash: %.2f'%(tdate, etfcode, n, cash))
                    pdr.to_sql('temporder', con= recordSQL.engine, if_exists= 'append', index= False)
                elif record.mark== '虚拟赎回':
                    if len(myRecordData[(myRecordData['stkcd']== record.stkcd)&(myRecordData['mark']== '虚拟赎回抵消')])> 0:
                        continue
                    redeemData= localSQL.getRedeemData(etfcode, tdate)
                    checkData= cfg.getConsCheckRedeemData(localSQL, etfcode, redeemData, tdate)
                    ncf= checkData['check'].sum()
                    if ncf> 0:
                        record.countervail= ncf
                        unfinishData= unfinishData.append(record)
                        continue
                    pdr= cfg.getRedeemRecord(etfcode, redeemData, record)
                    if pdr['quantity'].sum()!= 0:
                        errorData= errorData.append(record)
                        continue
                    cash= pdr[['trvolume', 'commission']].sum().sum()
                    if abs(cash)> 500000:
                        errorData= errorData.append(record)
                        continue
                    sumCash+= cash
                    n= len(pdr)
                    nRecord+= n
                    print('insert redeem to temporder, date: %d, etfcode: %s, Nrecord= %d, cash: %.2f'%(tdate, etfcode, n, cash))
                    pdr.to_sql('temporder', con= recordSQL.engine, if_exists= 'append', index= False)
        print('last date: %d, sumCash: %.2f'% (tdate, sumCash))
        
    cfg.timeEnd(t0)
    
    
    
    
                                    