# -*- coding: utf-8 -*-
"""
Created on Sat Jul 18 00:27:39 2020
 
@author: WAP
get data to SRDT, marked data
"""
import dtcfg as cfg
import pandas as pd

def getMarkData(localSQL, data, tdate):
    listDelBN= ['', 'ETF差额补冻结', 'ETF现金差额冻结', 'ETF差额补解冻', 'ETF现金差额解冻', 'ETF现金赎回冻结', 'ETF申购过户费冻结', 'ETF申购过户费解冻', 
       'ETF赎回过户费冻结', 'ETF赎回过户费解冻', '货基权益过入']
    data['business_name']= data['business_name'].map(cfg.deStrCode)
    data= data[((data['sec_type']!='etf')|(data['business_name'].isin(['证券买入', '证券卖出'])== False))&\
               (data['sec_type']!='other')&(data['business_name'].isin(listDelBN)== False)]
    lsdata= data[data['sec_type']=='etf']
    data.loc[lsdata.index, 'mark']= lsdata['sec_code'].map(cfg.normETFCode)
    lsdata= data[data['sec_code']=='159900']
    data.loc[lsdata.index, 'mark']= lsdata['relative_code']
    
    listStockBN= ['证券买入', '证券卖出']
    markData= data[data['business_name'].isin(listStockBN)== False]
    undoData= markData[markData['mark']=='unmarked']
    markData.loc[undoData.index, 'mark']= undoData['relative_code'].map(cfg.normETFCode)
    
    tradeData= data[data['business_name'].isin(listStockBN)]
    
    lsdata= data[data['sec_type']=='stock'].groupby('sec_code')['serial_no'].count()
    markStockData= pd.DataFrame(columns= data.columns)
    col= ['fund_chg', 'sec_chg', 'done_amt']
    colSeqNo= ['contract_no', 'rpt_contract_no', 'done_no', 'mark']
    colBasis= ['serial_no', 'busi_date', 'business_name', 'sec_code', 'sec_type', 'relative_code']
    napd= 0
    defaultapd= tradeData.iloc[-1]
    for scode in lsdata.index:
        #scode= lsdata.index[300]
        #scode='002371'
        buydata= tradeData[(tradeData['sec_code']== scode)&(tradeData['business_name']== '证券买入')]
        #defaultbuyapd= sbuy.iloc[0]
        sbuy= buydata[col]
        sout= markData[(markData['sec_code']== scode)&(markData['sec_chg']< 0)]
        
        selldata= tradeData[(tradeData['sec_code']== scode)&(tradeData['business_name']== '证券卖出')]
        #defaultsellapd= ssell.iloc[0]
        ssell= selldata[col]
        sin= markData[(markData['sec_code']== scode)&(markData['sec_chg']> 0)]
        volInSell= sin['sec_chg'].sum()+ ssell['sec_chg'].sum()
        volBuyOut= sbuy['sec_chg'].sum()+ sout['sec_chg'].sum()
        if volInSell+ volBuyOut< 0:
            print('error info: buy< out, date: %d, code: %s'% (tdate, scode))
            return pd.DataFrame()
        if volInSell> 0:
            print('add in-not-sell data, date: %d, stock code: %s'% (tdate, scode))
            slast= localSQL.getStockLast(tdate, scode)
            if slast< 0:
                print('error stock last, date: %d, stock code: %s'% (tdate, scode))
                return pd.DataFrame()
            amt= slast*volInSell
            ssell= ssell.append(pd.DataFrame([[amt, -volInSell, amt]], columns= col))
            sbuy= sbuy.append(pd.DataFrame([[-amt, volInSell, amt]], columns= col))
        #buy out
        nplace= (0, 0)
        for inde in sout.index:
            #inde= sout.index[0]
            qty= -markData.loc[inde, 'sec_chg']
            sumamt= 0
            sumvol= 0
            sumfnd= 0
            while sumvol< qty:
                tfnd= sbuy.iloc[nplace[0], 0]
                tqty= sbuy.iloc[nplace[0], 1]
                tamt= sbuy.iloc[nplace[0], 2]
                rqty= tqty- nplace[1]
                if rqty<= qty- sumvol:
                    sumamt+= tamt* rqty/ tqty
                    sumvol+= rqty
                    sumfnd+= tfnd* rqty/ tqty
                    nplace= (nplace[0]+ 1, 0)
                else:
                    lqty= qty- sumvol
                    sumamt+= tamt* lqty/ tqty
                    sumvol+= lqty
                    sumfnd+= tfnd* lqty/ tqty
                    nplace= (nplace[0], nplace[1]+ lqty)
            apd= defaultapd.copy()
            napd+= 1
            srNo= str(tdate)[2:]+ str(napd).zfill(6)
            apd.loc[colBasis]= [srNo, tdate, '证券买入', scode, 'stock', scode]
            apd.loc[col]= [sumfnd, sumvol, sumamt] 
            apd.loc[colSeqNo]= markData.loc[inde, colSeqNo]
            markStockData= markStockData.append(apd)
        #buy left
        nsbuy= len(sbuy)
        sumamt= 0
        sumvol= 0
        sumfnd= 0
        while nplace[0]< nsbuy:
            tfnd= sbuy.iloc[nplace[0], 0]
            tqty= sbuy.iloc[nplace[0], 1]
            tamt= sbuy.iloc[nplace[0], 2]
            rqty= tqty- nplace[1]
            sumamt+= tamt* rqty/ tqty
            sumvol+= rqty
            sumfnd+= tfnd* rqty/ tqty
            nplace= (nplace[0]+ 1, 0)
        leftbuy= [sumfnd, sumvol, sumamt]
        #sell
        nplace= (0, 0)
        for inde in sin.index:
            #inde= sin.index[0]
            qty= markData.loc[inde, 'sec_chg']
            sumamt= 0
            sumvol= 0
            sumfnd= 0
            while -sumvol< qty:
                tfnd= ssell.iloc[nplace[0], 0]
                tqty= ssell.iloc[nplace[0], 1]
                tamt= ssell.iloc[nplace[0], 2]
                rqty= tqty- nplace[1]
                if -rqty<= qty+ sumvol:
                    sumamt+= tamt* rqty/ tqty
                    sumvol+= rqty
                    sumfnd+= tfnd* rqty/ tqty
                    nplace= (nplace[0]+ 1, 0)
                else:
                    lqty= -qty- sumvol
                    sumamt+= tamt* lqty/ tqty
                    sumvol+= lqty
                    sumfnd+= tfnd* lqty/ tqty
                    nplace= (nplace[0], nplace[1]+ lqty)
            apd= defaultapd.copy()
            napd+= 1
            srNo= str(tdate)[2:]+ str(napd).zfill(6)
            apd.loc[colBasis]= [srNo, tdate, '证券卖出', scode, 'stock', scode]
            apd.loc[col]= [sumfnd, sumvol, sumamt] 
            apd.loc[colSeqNo]= markData.loc[inde, colSeqNo]
            markStockData= markStockData.append(apd)
        #sell left
        nssell= len(ssell)
        sumamt= 0
        sumvol= 0
        sumfnd= 0
        while nplace[0]< nssell:
            tfnd= ssell.iloc[nplace[0], 0]
            tqty= ssell.iloc[nplace[0], 1]
            tamt= ssell.iloc[nplace[0], 2]
            rqty= tqty- nplace[1]
            sumamt+= tamt* rqty/ tqty
            sumvol+= rqty
            sumfnd+= tfnd* rqty/ tqty
            nplace= (nplace[0]+ 1, 0)
        leftsell= [sumfnd, sumvol, sumamt]
        #deal left
        if leftbuy[1]< 1 and leftsell[1]> -1:
            pass
        elif leftbuy[1]< 1 and leftsell[1]< -1:
            apd= defaultapd.copy()
            napd+= 1
            srNo= str(tdate)[2:]+ str(napd).zfill(6)
            apd.loc[colBasis]= [srNo, tdate, '证券卖出', scode, 'stock', scode]
            apd.loc[col]= leftsell 
            apd.loc[colSeqNo]= ['', '', '', 'hold']
            markStockData= markStockData.append(apd)
        elif leftbuy[1]> 1 and leftsell[1]> -1:
            apd= defaultapd.copy()
            napd+= 1
            srNo= str(tdate)[2:]+ str(napd).zfill(6)
            apd.loc[colBasis]= [srNo, tdate, '证券买入', scode, 'stock', scode]
            apd.loc[col]= leftbuy 
            apd.loc[colSeqNo]= ['', '', '', 'hold']
            markStockData= markStockData.append(apd)
        elif leftbuy[1]+ leftsell[1]> 1:
            apd= defaultapd.copy()
            napd+= 1
            srNo= str(tdate)[2:]+ str(napd).zfill(6)
            apd.loc[colBasis]= [srNo, tdate, '证券卖出', scode, 'stock', scode]
            apd.loc[col]= leftsell 
            apd.loc[colSeqNo]= ['', '', '', 'trade']
            markStockData= markStockData.append(apd)
            #matchsell buy
            apd= defaultapd.copy()
            napd+= 1
            srNo= str(tdate)[2:]+ str(napd).zfill(6)
            apd.loc[colBasis]= [srNo, tdate, '证券买入', scode, 'stock', scode]
            apd.loc[col]= [-leftsell[1]* leftbuy[0]/leftbuy[1], -leftsell[1], -leftsell[1]* leftbuy[2]/leftbuy[1]] 
            apd.loc[colSeqNo]= ['', '', '', 'trade']
            markStockData= markStockData.append(apd)
            #left buy
            lqty= leftbuy[1]+ leftsell[1]
            apd= defaultapd.copy()
            napd+= 1
            srNo= str(tdate)[2:]+ str(napd).zfill(6)
            apd.loc[colBasis]= [srNo, tdate, '证券买入', scode, 'stock', scode]
            apd.loc[col]= [lqty* leftbuy[0]/leftbuy[1], lqty, lqty* leftbuy[2]/leftbuy[1]] 
            apd.loc[colSeqNo]= ['', '', '', 'hold']
            markStockData= markStockData.append(apd)
        elif leftbuy[1]+ leftsell[1]< -1:
            apd= defaultapd.copy()
            napd+= 1
            srNo= str(tdate)[2:]+ str(napd).zfill(6)
            apd.loc[colBasis]= [srNo, tdate, '证券买入', scode, 'stock', scode]
            apd.loc[col]= leftbuy 
            apd.loc[colSeqNo]= ['', '', '', 'trade']
            markStockData= markStockData.append(apd)
            #matchbuy sell
            apd= defaultapd.copy()
            napd+= 1
            srNo= str(tdate)[2:]+ str(napd).zfill(6)
            apd.loc[colBasis]= [srNo, tdate, '证券卖出', scode, 'stock', scode]
            apd.loc[col]= [-leftbuy[1]* leftsell[0]/leftsell[1], -leftbuy[1], -leftbuy[1]* leftsell[2]/leftsell[1]] 
            apd.loc[colSeqNo]= ['', '', '', 'trade']
            markStockData= markStockData.append(apd)
            #left sell
            lqty= leftbuy[1]+ leftsell[1]
            apd= defaultapd.copy()
            napd+= 1
            srNo= str(tdate)[2:]+ str(napd).zfill(6)
            apd.loc[colBasis]= [srNo, tdate, '证券卖出', scode, 'stock', scode]
            apd.loc[col]= [lqty* leftsell[0]/leftsell[1], lqty, lqty* leftsell[2]/leftsell[1]] 
            apd.loc[colSeqNo]= ['', '', '', 'hold']
            markStockData= markStockData.append(apd)
    return pd.concat([markData, markStockData], axis= 0)

if __name__ == '__main__':
    t0 = cfg.timeStart()
    localSQL= cfg.MSSQL(cfg.host, cfg.user, cfg.pwd, cfg.lcdb)
    localSQL.Connect()
    if not localSQL.isConnect:
        print(localSQL.host + ' not connect')
    else:
        listDate= localSQL.getDate('TFDT')
        listMarkDate= localSQL.getDate('SRDT')
        lastDate= 0
        for tdate in listDate:
            if tdate in listMarkDate:
                lastDate= tdate
                continue
            #if tdate< cfg.startDate:
            #    continue
            #tdate= listDate[0]
            #tdate=20200703
            print('deal data date: %d'% tdate)
            data= localSQL.getUnmarkedData(tdate, lastDate)
            
            markData= getMarkData(localSQL, data, tdate)
            
            #lsdata=markData[markData['mark'].isin(['hold','trade'])]
            if len(markData)<1:
                break
            stockdata= markData[(markData['sec_type']=='stock')&(markData['mark']!='hold')].groupby('sec_code')['sec_chg','fund_chg'].sum()
            stockdata= stockdata[stockdata['sec_chg']!=0]
            if len(stockdata)>1:
                print('error stock data, date: '+ tdate)
                break
            
            localSQL.addData(markData)
            lastDate= tdate
        cfg.updateRCNPatch(localSQL)
    cfg.timeEnd(t0)

    #cfg.rebuildSRDT(localSQL)