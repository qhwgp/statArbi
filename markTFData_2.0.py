# -*- coding: utf-8 -*-
"""
Created on Sat Jul 18 00:27:39 2020

@author: WAP
"""
from os import path
import pandas as pd
import time as ti
import warnings
from pymssql import connect
from sqlalchemy import create_engine
warnings.filterwarnings('ignore')

def deStrCode(strData):
    try:
        return str.strip(strData.encode('latin1').decode('GB2312'))
    except:
        return strData

class MSSQL:

    def __init__(self,host,user,pwd,db):
        self.host = host
        self.user = user
        self.pwd = pwd
        self.db = db
        self.isConnect= False

    def Connect(self):
        try:
            self.conn = connect(host=self.host,user=self.user,password=self.pwd,database=self.db,charset="UTF-8")
            self.conn.autocommit(True)
            self.cur = self.conn.cursor()
            if not self.cur:
                self.isConnect= False
            else:
                self.isConnect= True
        except:
            self.isConnect= False
        
    def Close(self):
        self.cur.close()
        self.conn.close()
        
    def getDate(self, startDate= 0):
        sql="select distinct busi_date from TFDT where busi_date >= %d order by busi_date" % startDate
        data= pd.read_sql(sql, con=self.conn)
        return list(data['busi_date'])
        
    def getDateData(self,tDate):
        sql= 'select * from TFDT where busi_date=%d'% tDate
        data= pd.read_sql(sql, con=self.conn)
        data['business_name']= data['business_name'].map(deStrCode)
        return data
    
    def getUnmarkedData(self,tDate):
        sql= "select * from TFDT where busi_date<=%d and mark='unmarked'"% tDate
        data= pd.read_sql(sql, con=self.conn)
        data['business_name']= data['business_name'].map(deStrCode)
        return data
    
    def getData(self, sql):
        data= pd.read_sql(sql, con=self.conn)
        return data
    
    def addData(self, engine, data):
        if len(data)> 0:
            data.to_sql('TFDT', con= engine, if_exists= 'append', index= False)

    def updateMarkData(self, engine, data):
        try:
            self.cur.execute('drop table tempDT')
        except:
            pass
        sqlword = """
        CREATE TABLE  tempDT (
        serial_no VARCHAR(30) NOT NULL,
        busi_date int,
        business_name  VARCHAR(30),
        fund_chg     float,
        sec_code  VARCHAR(30),
        sec_type   VARCHAR(30),
        sec_chg float,
        done_amt    float,
        contract_no    VARCHAR(30),
        rpt_contract_no    VARCHAR(30),
        done_no    VARCHAR(30),
        done_date VARCHAR(30),
        relative_code VARCHAR(30),
        mark VARCHAR(30),
        code_type VARCHAR(30),
        PRIMARY KEY(serial_no,busi_date)
        )
        """
        self.cur.execute(sqlword)
        data.to_sql('tempDT', con= engine, if_exists= 'append', index= False)
        sql="UPDATE TFDT SET mark=t.mark, code_type=t.code_type FROM (SELECT * from tempDT) AS t WHERE TFDT.serial_no=t.serial_no and TFDT.busi_date=t.busi_date"
        self.cur.execute(sql)

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
    
def code_type(myStr):
    try:
        if myStr[0]== '1' or myStr[0]== '5':
            return 'etf'
        elif myStr[0]== '0' or myStr[0]== '3' or myStr[0]== '6':
            return 'stock'
        else:
            return 'other'
    except:
        return 'other'
    
def getMarkData(data, tdate):
    data['code_type']= data['sec_code'].map(code_type)
    lsdata= data[data['code_type']=='etf']
    data.loc[lsdata.index, 'mark']= lsdata['sec_code'].map(normETFCode)
    lsdata= data[data['code_type']=='other']
    data.loc[lsdata.index, 'mark']= 'cash'
    #data= data.drop(['code_type'], axis=1)
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
    nadp= 10
    nUndo= len(undoData)
    #wap
    for i in range(nUndo):
        #inde= undoData.index[0]
        if undoData.iloc[i, 13]!= 'unmarked':
            continue
        scode= undoData.iloc[i, 4]
        #scode='300059'
        #lsdata= data[(data['sec_code']== scode)&(data['sec_chg']!= 0)]
        #qty= undoData.iloc[i, 6]
        lsdata= undoData[(undoData['sec_code']== scode)&(undoData['mark']== 'unmarked')]
        
        gdata= lsdata.groupby(['business_name','relative_code'])['fund_chg','sec_chg','done_amt'].sum()
        if undoData.iloc[i, 2]== '证券买入':
            qty= gdata.loc[('证券买入', scode), 'sec_chg']
            if '申购赎回过出' in gdata.index:
                for rcode in gdata.loc['申购赎回过出','sec_chg'].index:
                    #rcode= gdata.loc['申购赎回过出','sec_chg'].index[0]
                    lsdata= undoData[(undoData['sec_code']== scode)&(undoData['mark']== 'unmarked')&(undoData['business_name'].isin(['证券买入', '申购赎回过出']))]
                    outqty= gdata.loc[('申购赎回过出', rcode), 'sec_chg']
                    if -outqty== qty:
                        sdata= lsdata[((lsdata['relative_code']== rcode)|(lsdata['business_name']== '证券买入'))]
                        undoData.loc[sdata.index, 'mark']= rcode
                        #data.loc[sdata.index, 'mark']= rcode

                    elif -outqty< qty:
                        sdata= lsdata[((lsdata['relative_code']== rcode)|(lsdata['business_name']== '证券买入'))]
                        undoData.loc[sdata.index, 'mark']= rcode
                        #data.loc[sdata.index, 'mark']= rcode
                        apd= undoData.iloc[i]
                        nadp+= 1
                        seqno= str(tdate)+ str(nadp).zfill(4)
                        apd.name= seqno
                        apd.serial_no= seqno
                        apd.rpt_contract_no= 'append'
                        apd.sec_chg= -qty- outqty
                        apd.fund_chg= -(qty+ outqty)/qty* gdata.loc[('证券买入', scode), 'fund_chg']
                        apd.done_amt= -(qty+ outqty)/qty* gdata.loc[('证券买入', scode), 'done_amt']
                        apd.mark= rcode
                        undoData= undoData.append(apd)
                        
                        nadp+= 1
                        seqno= str(tdate)+ str(nadp).zfill(4)
                        apd.name= seqno
                        apd.serial_no= seqno
                        apd.sec_chg= -apd.sec_chg
                        apd.fund_chg= -apd.fund_chg
                        apd.done_amt= -apd.done_amt
                        apd.mark= 'unmarked'
                        undoData= undoData.append(apd)
                        qty+= outqty
                    else:
                        pass
        elif undoData.iloc[i, 2]== '证券卖出':
            qty= gdata.loc[('证券卖出', scode), 'sec_chg']
            if '申购赎回过入' in gdata.index:
                for rcode in gdata.loc['申购赎回过入','sec_chg'].index:
                    #rcode= gdata.loc['申购赎回过入','sec_chg'].index[0]
                    lsdata= undoData[(undoData['sec_code']== scode)&(undoData['mark']== 'unmarked')&(undoData['business_name'].isin(['证券卖出', '申购赎回过入']))]
                    outqty= gdata.loc[('申购赎回过入', rcode), 'sec_chg']
                    if outqty== -qty:
                        sdata= lsdata[(lsdata['relative_code']== rcode)|(lsdata['business_name']== '证券卖出')]
                        undoData.loc[sdata.index, 'mark']= rcode
                        #data.loc[sdata.index, 'mark']= rcode
                    elif outqty< -qty:
                        sdata= lsdata[((lsdata['relative_code']== rcode)|(lsdata['business_name']== '证券卖出'))]
                        undoData.loc[sdata.index, 'mark']= rcode
                        #data.loc[sdata.index, 'mark']= rcode
                        apd= undoData.iloc[i]
                        nadp+= 1
                        seqno= str(tdate)+ str(nadp).zfill(4)
                        apd.name= seqno
                        apd.serial_no= seqno
                        apd.rpt_contract_no= 'append'
                        apd.sec_chg= -qty- outqty
                        apd.fund_chg= -(qty+ outqty)/qty* gdata.loc[('证券卖出', scode), 'fund_chg']
                        apd.done_amt= -(qty+ outqty)/qty* gdata.loc[('证券卖出', scode), 'done_amt']
                        apd.mark= rcode
                        undoData= undoData.append(apd)
                        
                        nadp+= 1
                        seqno= str(tdate)+ str(nadp).zfill(4)
                        apd.name= seqno
                        apd.serial_no= seqno
                        apd.sec_chg= -apd.sec_chg
                        apd.fund_chg= -apd.fund_chg
                        apd.done_amt= -apd.done_amt
                        apd.mark= 'unmarked'
                        undoData= undoData.append(apd)
                        qty+= outqty
                        
                    else:
                        pass
    lsdata= undoData[undoData['rpt_contract_no']!= 'append']
    data.loc[lsdata.index, 'mark']= lsdata['mark']
    apdata= undoData[(undoData['rpt_contract_no']== 'append')&(undoData['busi_date']== tdate)]
    
    #data.loc[undoData.index[:nUndo], 'mark']= undoData['mark']
    
    undoData= data[data['mark']=='unmarked']
    for scode in undoData.groupby('sec_code')['mark'].count().index:
        #scode= '600036'
        lsdata= undoData[undoData['sec_code']== scode]
        if lsdata['sec_chg'].sum()!= 0:
            continue
        elif not ('证券买入' in lsdata['business_name'].values or '证券卖出' in lsdata['business_name'].values):
            codedata= data[data['sec_code']== scode]
            if '证券买入' in codedata['business_name'].values:
                codedata= codedata[codedata['business_name']== '证券买入']
                apd= codedata.iloc[0]
                sumcode= codedata[['sec_chg', 'done_amt']].sum()
                unitamt= sumcode.done_amt/ sumcode.sec_chg
            elif '证券卖出' in codedata['business_name'].values:
                codedata= codedata[codedata['business_name']== '证券卖出']
                apd= codedata.iloc[0]
                sumcode= codedata[['sec_chg', 'done_amt']].sum()
                unitamt= -sumcode.done_amt/ sumcode.sec_chg
            else:
                apd= codedata.iloc[0]
                unitamt= 0
            for inde in lsdata.index:
                #inde= lsdata.index[0]
                qty= lsdata.loc[inde, 'sec_chg']
                rcode= lsdata.loc[inde, 'relative_code']
                if lsdata.loc[inde, 'business_name']== '申购赎回过出':
                    undoData.loc[inde, 'mark']= rcode
                    nadp+= 1
                    seqno= str(tdate)+ str(nadp).zfill(4)
                    apd.name= seqno
                    apd.serial_no= seqno
                    apd.rpt_contract_no= 'append'
                    apd.business_name= '证券买入'
                    apd.sec_chg= -qty
                    apd.fund_chg= qty* unitamt
                    apd.done_amt= -qty* unitamt
                    apd.mark= rcode
                    undoData= undoData.append(apd)
                elif lsdata.loc[inde, 'business_name']== '申购赎回过入':
                    undoData.loc[inde, 'mark']= rcode
                    nadp+= 1
                    seqno= str(tdate)+ str(nadp).zfill(4)
                    apd.name= seqno
                    apd.serial_no= seqno
                    apd.rpt_contract_no= 'append'
                    apd.business_name= '证券卖出'
                    apd.sec_chg= -qty
                    apd.fund_chg= qty* unitamt
                    apd.done_amt= qty* unitamt
                    apd.mark= rcode
                    undoData= undoData.append(apd)
        elif not ('申购赎回过出' in lsdata['business_name'].values or '申购赎回过入' in lsdata['business_name'].values):
            for inde in lsdata.index:
                undoData.loc[inde, 'mark']= 'trade'
        elif '证券买入' in lsdata['business_name'].values:
            codedata= lsdata[lsdata['business_name']== '证券买入']
            apd= codedata.iloc[0]
            sumcode= codedata[['sec_chg', 'done_amt']].sum()
            unitamt= sumcode.done_amt/ sumcode.sec_chg
            for inde in lsdata.index:
                qty= lsdata.loc[inde, 'sec_chg']
                rcode= lsdata.loc[inde, 'relative_code']
                if lsdata.loc[inde, 'business_name']== '申购赎回过出':
                    undoData.loc[inde, 'mark']= rcode
                    nadp+= 1
                    seqno= str(tdate)+ str(nadp).zfill(4)
                    apd.name= seqno
                    apd.serial_no= seqno
                    apd.rpt_contract_no= 'append'
                    apd.business_name= '证券买入'
                    apd.sec_chg= -qty
                    apd.fund_chg= qty* unitamt
                    apd.done_amt= -qty* unitamt
                    apd.mark= rcode
                    undoData= undoData.append(apd)
                elif lsdata.loc[inde, 'business_name']== '申购赎回过入':
                    undoData.loc[inde, 'mark']= rcode
                    nadp+= 1
                    seqno= str(tdate)+ str(nadp).zfill(4)
                    apd.name= seqno
                    apd.serial_no= seqno
                    apd.rpt_contract_no= 'append'
                    apd.business_name= '证券卖出'
                    apd.sec_chg= -qty
                    apd.fund_chg= qty* unitamt
                    apd.done_amt= qty* unitamt
                    apd.mark= rcode
                    undoData= undoData.append(apd)
                elif lsdata.loc[inde, 'business_name']== '证券买入':
                    undoData.loc[inde, 'mark']= 'trade'
                    nadp+= 1
                    seqno= str(tdate)+ str(nadp).zfill(4)
                    apd.name= seqno
                    apd.serial_no= seqno
                    apd.rpt_contract_no= 'append'
                    apd.business_name= '证券卖出'
                    apd.sec_chg= -qty
                    apd.fund_chg= qty* unitamt
                    apd.done_amt= qty* unitamt
                    apd.mark= 'trade'
                    undoData= undoData.append(apd)
    lsdata= undoData[undoData['rpt_contract_no']!= 'append']
    data.loc[lsdata.index, 'mark']= lsdata['mark']
    apdata= pd.concat([apdata, undoData[(undoData['rpt_contract_no']== 'append')&(undoData['busi_date']== tdate)]], axis= 0)
    
    undoData= data[data['mark']=='unmarked']
    return data, apdata, undoData
    
if __name__ == '__main__':
    t0 = ti.time()
    localSQL= MSSQL('127.0.0.1', 'sa', '123', 'markedTF71')
    localSQL.Connect()
    if not localSQL.isConnect:
        print(localSQL.host + ' not connect')
    else:
        engine = create_engine("mssql+pymssql://sa:123@127.0.0.1:1433/markedTF71")
        listDate= localSQL.getDate()

        for tdate in listDate[:13]:
            print('deal data date: %d'% tdate)
            #tdate= listDate[13]
            data= localSQL.getUnmarkedData(tdate)  
            data, apdata, undoData= getMarkData(data, tdate)
            
            localSQL.updateMarkData(engine, data)
            localSQL.addData(engine, apdata)

    print('All done, time elapsed: %.2f min' % ((ti.time() - t0)/60))
    
    