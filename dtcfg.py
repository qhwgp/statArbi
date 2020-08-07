# -*- coding: utf-8 -*-
"""
Created on Tue Jul 28 09:14:56 2020

@author: WAP
"""
from pymssql import connect
from sqlalchemy import create_engine
import time as ti
import pandas as pd
import warnings
from datetime import date, datetime, timedelta
warnings.filterwarnings('ignore')

host='172.21.6.152'
user='wanggp'
pwd='Wanggp@0511'
dcadb='data_ceneter_all'
lcdb='Wangprivate'

jqUser= '18665883365'
jqPwd= 'Hu12345678'
listIndex= ['000016.XSHG', '000300.XSHG', '000905.XSHG', '000986.XSHG', '000987.XSHG', '000988.XSHG', '000989.XSHG',
       '000990.XSHG', '000991.XSHG', '000992.XSHG', '000993.XSHG', '000994.XSHG', '000995.XSHG']

def getStrToday(tback= 0):
    return (date.today()-timedelta(days= tback)).strftime("%Y%m%d")

def getStrNextDay(strDate):
    dt= datetime.strptime(strDate,'%Y%m%d')
    dt= dt+ timedelta(days=1)
    return dt.strftime("%Y%m%d")

def timeStart():
    return ti.time()
    
def timeEnd(t0):
    print('All done, time elapsed: %.2f min' %  ((ti.time() - t0)/60))

def deStrCode(strData):
    try:
        return str.strip(strData.encode('latin1').decode('GB2312'))
    except:
        return str.strip(strData)
    
def myStr(myStr):
    try:
        resultData= int(myStr)
    except:
        resultData= myStr
    return str(resultData)

def myInt(mystr):
    try:
        return int(mystr)
    except:
        return 0
    
#2
def normETFCode(code):
    code=str(code)
    if code[0]=='5':
        return code[:5]+'0'
    elif code[0]=='1':
        return code
    else:
        return 'unmarked'
    
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
    
class MSSQL:
    def __init__(self,host,user,pwd,db):
        self.host = host
        self.user = user
        self.pwd = pwd
        self.db = db
        self.engine = create_engine("mssql+pymssql://%s:%s@%s:1433/%s"%(user, pwd, host, db))
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
        
    def getDate(self, datatable, startDate= 0):
        sql="select distinct busi_date from %s where busi_date >= %d order by busi_date" % (datatable, startDate)
        data= pd.read_sql(sql, con=self.conn)
        return list(data['busi_date'])
        
    def getDateData(self,tDate):
        sql= 'select * from TFDT where busi_date=%d'% tDate
        data= pd.read_sql(sql, con=self.conn)
        data['business_name']= data['business_name'].map(deStrCode)
        return data
    
    def getUnmarkedData(self,tDate):
        data= self.getDateData(tDate)
        data['mark']= 'unmarked'
        data['mark_date']= 0
        data['sec_type']= data['sec_code'].map(code_type)
        sql= "select * from SRDT where busi_date<%d and mark='unmarked'"% tDate
        unmarkdata= pd.read_sql(sql, con=self.conn)
        unmarkdata['business_name']= unmarkdata['business_name'].map(deStrCode)
        return pd.concat([unmarkdata, data], axis= 0, ignore_index= True)
    
    def getData(self, sql):
        self.cur.execute(sql)
        data= pd.read_sql(sql, con=self.conn) 
        return data
    
    def addData(self, data):
        if len(data)> 0:
            data.to_sql('SRDT', con= self.engine, if_exists= 'append', index= False)

    def updateData(self, data):
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
        done_date int,
        relative_code VARCHAR(30),
        mark VARCHAR(30),
        mark_date int,
        PRIMARY KEY(serial_no,busi_date))
        """
        self.cur.execute(sqlword)
        data.to_sql('tempDT', con= self.engine, if_exists= 'append', index= False)
        sql="UPDATE SRDT SET mark=t.mark, mark_date=t.mark_date FROM (SELECT * from tempDT) AS t WHERE SRDT.serial_no=t.serial_no and SRDT.busi_date=t.busi_date"
        self.cur.execute(sql)
        
    def updateRCNData(self, data):
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
        done_date int,
        relative_code VARCHAR(30),
        mark VARCHAR(30),
        mark_date int,
        PRIMARY KEY(serial_no,busi_date))
        """
        self.cur.execute(sqlword)
        data.to_sql('tempDT', con= self.engine, if_exists= 'append', index= False)
        sql="UPDATE SRDT SET rpt_contract_no=t.rpt_contract_no FROM (SELECT * from tempDT) AS t WHERE SRDT.serial_no=t.serial_no and SRDT.busi_date=t.busi_date"
        self.cur.execute(sql)
        
    def updateMarkData(self, data, tdate):
        lsdata= data[data['busi_date']< tdate]
        if len(lsdata)> 0:
            self.updateData(lsdata)
        lsdata= data[data['busi_date']== tdate]
        if len(lsdata)> 0:
            self.addData(lsdata)
            
    def getsubData(self, scode, tdate):
        sqlword = """
        select * from SRDT where mark='%s' and 
        (business_name not in ('ETF申购过户费冻结','ETF申购过户费解冻')) and 
        (
        (done_no in (select done_no from SRDT where busi_date=%d and business_name in ('ETF申购','ETF现金申购份额确认') and 
                     sec_code='%s'  and done_no<>'0')) or
        (rpt_contract_no in (select rpt_contract_no from SRDT where busi_date=%d and 
                             business_name in ('ETF申购','ETF现金申购份额确认') and sec_code='%s')) or
        (business_name in ('现金替代补款', '现金替代退款') and done_date=%d)
        ) 
        order by sec_code, serial_no
        """%(scode, tdate, scode, tdate, scode, tdate)
        data= pd.read_sql(sqlword, con=self.conn)
        return data
    
    def getRedeemData(self, scode, tdate):
        sqlword = """
        select * from SRDT where mark='%s' and 
        (business_name not in ('ETF赎回过户费冻结','ETF赎回过户费解冻')) and 
        (
        (done_no in (select done_no from SRDT where busi_date=%d and business_name='ETF赎回' and 
                     sec_code='%s'  and done_no<>'0')) or
        (rpt_contract_no in (select rpt_contract_no from SRDT where busi_date=%d and 
                             business_name='ETF赎回' and sec_code='%s'))
        )
        order by sec_code, serial_no
        """%(scode, tdate, scode, tdate, scode)
        data= pd.read_sql(sqlword, con=self.conn)
        return data
    
def rebuildDT(localSQL, dataTable):
    if input('del Data Base! PleaseConfirm:(y/n) ')!= 'y':
        return
    try:
        localSQL.cur.execute('drop table %s'% dataTable)
    except:
        pass
    sqlword = """
    CREATE TABLE  %s (
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
    PRIMARY KEY(serial_no,busi_date)
    )
    """% dataTable
    localSQL.cur.execute(sqlword)
    return

def rebuildSRDT(localSQL):
    try:
        localSQL.cur.execute('drop table SRDT')
    except:
        pass
    sqlword = """
    CREATE TABLE  SRDT (
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
    mark  VARCHAR(30),
    mark_date int,
    PRIMARY KEY(serial_no,busi_date)
    )
    """
    localSQL.cur.execute(sqlword)

def syncDateData(clsSQL, localSQL, engine, tdate):
    sql= "select serial_no, busi_date, business_name, fund_chg, sec_code, sec_type, sec_chg, done_amt, contract_no,\
            rpt_contract_no, done_no, done_date, relative_code from uv_tcl_his_fund_stock_chg_71 where busi_date= %s order by serial_no" % tdate
    data= clsSQL.getData(sql)
    data['business_name']= data['business_name'].map(deStrCode)
    data['busi_date']= data['busi_date'].map(myInt)
    data['done_date']= data['done_date'].map(myInt)
    data['done_no']= data['done_no'].map(myStr)
    data['sec_code']= data['sec_code'].map(str.strip)
    data['contract_no']= data['contract_no'].map(str.strip)
    data['relative_code']= data['relative_code'].map(str.strip)
    data['busi_date']= tdate
    data.to_sql('TFDT', con= engine, if_exists= 'append', index= False)
    
def syncData(clsSQL, localSQL, intSDate= 0):
    engine = create_engine("mssql+pymssql://%s:%s@%s:1433/%s"%(localSQL.user, localSQL.pwd, localSQL.host, localSQL.db))
    sql= "select distinct busi_date from uv_tcl_his_fund_stock_chg_71 order by busi_date"
    pdDate= clsSQL.getData(sql)
    listDate= pdDate['busi_date'].values.tolist()
    
    sql= "select distinct busi_date from TFDT"
    localDate= localSQL.getData(sql)
    listLocalDate= localDate['busi_date'].map(lambda x:str(x)[:8]).values.tolist()
    
    for dt in listDate:
        if dt< intSDate:
            continue
        tdate= str(dt)[:8]
        if tdate in listLocalDate:
            continue
        print('sync date: '+ tdate)
        syncDateData(clsSQL, localSQL, engine, tdate)
    print('last date: '+ tdate)
    
def getMarkData(data, tdate):
    data['business_name']= data['business_name'].map(deStrCode)
    data= data[((data['sec_type']!='etf')|(data['business_name'].isin(['证券买入', '证券卖出'])== False))&\
               (data['sec_type']!='other')]
    lsdata= data[data['business_name'].isin(['证券买入', '证券卖出'])]
    data.loc[lsdata.index, 'contract_no']= ''
    data.loc[lsdata.index, 'rpt_contract_no']= ''
    data.loc[lsdata.index, 'done_no']= ''
    lsdata= data[data['sec_type']=='etf']
    data.loc[lsdata.index, 'mark']= lsdata['sec_code'].map(normETFCode)
    lsdata= data[data['sec_code']=='159900']
    data.loc[lsdata.index, 'mark']= lsdata['relative_code']
    undoData= data[data['mark']=='unmarked']
    listStockBN= ['申购赎回过入', '申购赎回过出', '证券买入', '证券卖出']
    lsdata= undoData[(undoData['business_name'].isin(listStockBN)== False)|(undoData['sec_chg']==0)]
    data.loc[lsdata.index, 'mark']= lsdata['relative_code'].map(normETFCode)
    undoData= data[data['mark']=='unmarked']
    lsdata= undoData.groupby('sec_code')['serial_no'].count()
    #vol matched data
    for scode in lsdata.index:
        sdata= data[(data['sec_code']== scode)&((data['business_name']== '证券买入')|(data['business_name']== '申购赎回过出'))]
        nLsData= len(sdata)
        for iRow in range(nLsData):
            if sdata.iloc[iRow]['business_name']=='申购赎回过出':
                cRow= 0
                qty= -sdata.iloc[iRow]['sec_chg']
                while cRow<nLsData:
                    if sdata.iloc[cRow]['business_name']=='证券买入' and sdata.iloc[cRow]['sec_chg']==qty and sdata.iloc[cRow]['mark']=='unmarked':
                        sdata.iloc[cRow,13]= sdata.iloc[iRow,12]
                        sdata.iloc[cRow,9]= sdata.iloc[iRow,9]
                        sdata.iloc[iRow,13]= sdata.iloc[iRow,12]
                        break
                    else:
                        cRow+=1
        data.loc[sdata.index, ['rpt_contract_no', 'mark']]= sdata[['rpt_contract_no', 'mark']]
        sdata= data[(data['sec_code']== scode)&((data['business_name']== '证券卖出')|(data['business_name']== '申购赎回过入'))]
        nLsData= len(sdata)
        for iRow in range(nLsData):
            if sdata.iloc[iRow]['business_name']=='申购赎回过入':
                cRow= 0
                qty= -sdata.iloc[iRow]['sec_chg']
                while cRow<nLsData:
                    if sdata.iloc[cRow]['business_name']=='证券卖出' and sdata.iloc[cRow]['sec_chg']==qty and sdata.iloc[cRow]['mark']=='unmarked':
                        sdata.iloc[cRow,13]= sdata.iloc[iRow,12]
                        sdata.iloc[cRow,9]= sdata.iloc[iRow,9]
                        sdata.iloc[iRow,13]= sdata.iloc[iRow,12]
                        break
                    else:
                        cRow+=1
        data.loc[sdata.index, ['rpt_contract_no', 'mark']]= sdata[['rpt_contract_no', 'mark']]
    undoData= data[data['mark']=='unmarked']
    nadp= 0
    nUndo= len(undoData)
    #sum matched data
    for i in range(nUndo):
        inde= undoData.index[i]
        #inde= 18539
        if undoData.loc[inde, 'mark']!= 'unmarked':
            continue
        scode= undoData.loc[inde, 'sec_code']
        #scode='002049'
        #lsdata= undoData[(undoData['sec_code']== scode)]
        #qty= undoData.iloc[i, 6]
        lsdata= undoData[(undoData['sec_code']== scode)&(undoData['mark']== 'unmarked')]
        #lsindex= lsdata.index
        #lsdata= undodata.loc[lsindex]
        gdata= lsdata.groupby(['business_name','relative_code'])['fund_chg','sec_chg','done_amt'].sum()
        if undoData.loc[inde, 'business_name']== '证券买入':
            qty= gdata.loc[('证券买入', scode), 'sec_chg']
            if '申购赎回过出' in gdata.index:
                for rcode in gdata.loc['申购赎回过出','sec_chg'].index:
                    #rcode= gdata.loc['申购赎回过出','sec_chg'].index[0]
                    lsdata= undoData[(undoData['sec_code']== scode)&(undoData['mark']== 'unmarked')&(undoData['business_name'].isin(['证券买入', '申购赎回过出']))]
                    outqty= gdata.loc[('申购赎回过出', rcode), 'sec_chg']
                    rcn= lsdata[((lsdata['relative_code']== rcode)|(lsdata['business_name']== '申购赎回过出'))].iloc[0, 9]
                    if -outqty== qty:
                        sdata= lsdata[((lsdata['relative_code']== rcode)|(lsdata['business_name']== '证券买入'))]
                        undoData.loc[sdata.index, 'mark']= rcode
                        undoData.loc[sdata.index, 'rpt_contract_no']= rcn
                    elif -outqty< qty:
                        sdata= lsdata[((lsdata['relative_code']== rcode)|(lsdata['business_name']== '证券买入'))]
                        undoData.loc[sdata.index, 'mark']= rcode
                        undoData.loc[sdata.index, 'rpt_contract_no']= rcn
                        apd= undoData.loc[inde]
                        nadp+= 1
                        seqno= str(tdate)+ str(nadp).zfill(4)
                        apd.name= seqno
                        apd.serial_no= seqno
                        apd.rpt_contract_no= rcn
                        apd.done_no= 'append'
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
                        apd.rpt_contract_no= ''
                        undoData= undoData.append(apd)
                        qty+= outqty
                    else:
                        pass
        elif undoData.loc[inde, 'business_name']== '证券卖出':
            qty= gdata.loc[('证券卖出', scode), 'sec_chg']
            if '申购赎回过入' in gdata.index:
                for rcode in gdata.loc['申购赎回过入','sec_chg'].index:
                    #rcode= gdata.loc['申购赎回过入','sec_chg'].index[1]
                    lsdata= undoData[(undoData['sec_code']== scode)&(undoData['mark']== 'unmarked')&(undoData['business_name'].isin(['证券卖出', '申购赎回过入']))]
                    outqty= gdata.loc[('申购赎回过入', rcode), 'sec_chg']
                    rcn= lsdata[((lsdata['relative_code']== rcode)|(lsdata['business_name']== '申购赎回过入'))].iloc[0, 9]
                    if outqty== -qty:
                        sdata= lsdata[(lsdata['relative_code']== rcode)|(lsdata['business_name']== '证券卖出')]
                        undoData.loc[sdata.index, 'mark']= rcode
                        undoData.loc[sdata.index, 'rpt_contract_no']= rcn
                    elif outqty< -qty:
                        sdata= lsdata[((lsdata['relative_code']== rcode)|(lsdata['business_name']== '证券卖出'))]
                        undoData.loc[sdata.index, 'mark']= rcode
                        undoData.loc[sdata.index, 'rpt_contract_no']= rcn
                        apd= undoData.loc[inde]
                        nadp+= 1
                        seqno= str(tdate)+ str(nadp).zfill(4)
                        apd.name= seqno
                        apd.serial_no= seqno
                        apd.rpt_contract_no= rcn
                        apd.done_no= 'append'
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
    data.loc[undoData.index[:nUndo], ['rpt_contract_no', 'mark']]= undoData[['rpt_contract_no', 'mark']]
    data= pd.concat([data, undoData.iloc[nUndo:]], axis= 0)
    undoData= data[data['mark']=='unmarked']
    nUndo= len(undoData)
    #multi type sum match data
    for scode in undoData.groupby('sec_code')['mark'].count().index:
        #scode= '002049'
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
                rcn= lsdata.loc[inde, 'rpt_contract_no']
                if lsdata.loc[inde, 'business_name']== '申购赎回过出':
                    undoData.loc[inde, 'mark']= rcode
                    nadp+= 1
                    seqno= str(tdate)+ str(nadp).zfill(4)
                    apd.name= seqno
                    apd.serial_no= seqno
                    apd.rpt_contract_no= rcn
                    apd.done_no= 'append'
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
                    apd.rpt_contract_no= rcn
                    apd.done_no= 'append'
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
                rcn= lsdata.loc[inde, 'rpt_contract_no']
                #wap
                nadp+= 1
                seqno= str(tdate)+ str(nadp).zfill(4)
                apd.name= seqno
                apd.serial_no= seqno
                apd.done_no= 'append'
                apd.sec_chg= -qty
                apd.fund_chg= qty* unitamt
                if lsdata.loc[inde, 'business_name']== '申购赎回过出':
                    undoData.loc[inde, 'mark']= rcode
                    apd.business_name= '证券买入'
                    apd.done_amt= -qty* unitamt
                    apd.mark= rcode
                    apd.rpt_contract_no= rcn
                elif lsdata.loc[inde, 'business_name']== '申购赎回过入':
                    undoData.loc[inde, 'mark']= rcode
                    apd.business_name= '证券卖出'
                    apd.done_amt= qty* unitamt
                    apd.mark= rcode
                    apd.rpt_contract_no= rcn
                elif lsdata.loc[inde, 'business_name']== '证券买入':
                    undoData.loc[inde, 'mark']= 'trade'
                    apd.business_name= '证券卖出'
                    apd.done_amt= qty* unitamt
                    apd.mark= 'trade'
                undoData= undoData.append(apd)
        elif '证券卖出' in lsdata['business_name'].values:
            codedata= lsdata[lsdata['business_name']== '证券卖出']
            apd= codedata.iloc[0]
            sumcode= codedata[['sec_chg', 'done_amt']].sum()
            unitamt= -sumcode.done_amt/ sumcode.sec_chg
            for inde in lsdata.index:
                qty= lsdata.loc[inde, 'sec_chg']
                rcode= lsdata.loc[inde, 'relative_code']
                rcn= lsdata.loc[inde, 'rpt_contract_no']
                nadp+= 1
                seqno= str(tdate)+ str(nadp).zfill(4)
                apd.name= seqno
                apd.serial_no= seqno
                apd.done_no= 'append'
                apd.sec_chg= -qty
                apd.fund_chg= qty* unitamt
                if lsdata.loc[inde, 'business_name']== '申购赎回过出':
                    undoData.loc[inde, 'mark']= rcode
                    apd.business_name= '证券买入'
                    apd.done_amt= -qty* unitamt
                    apd.mark= rcode
                    apd.rpt_contract_no= rcn
                elif lsdata.loc[inde, 'business_name']== '申购赎回过入':
                    undoData.loc[inde, 'mark']= rcode
                    apd.business_name= '证券卖出'
                    apd.done_amt= qty* unitamt
                    apd.mark= rcode
                    apd.rpt_contract_no= rcn
                elif lsdata.loc[inde, 'business_name']== '证券卖出':
                    undoData.loc[inde, 'mark']= 'trade'
                    apd.business_name= '证券买入'
                    apd.done_amt= qty* unitamt
                    apd.mark= 'trade'
                undoData= undoData.append(apd)
    data.loc[undoData.index[:nUndo], ['rpt_contract_no', 'mark']]= undoData[['rpt_contract_no', 'mark']]
    data= pd.concat([data, undoData.iloc[nUndo:]], axis= 0)
    data.loc[data[data['mark']!='unmarked'].index, 'mark_date']= tdate
    return data

def updateRCNPatch(localSQL):
    sqlword= "select * from SRDT where business_name in ('现金替代退款','现金替代补款') and len(rpt_contract_no)<2 and len(done_no)<2 and done_date>20200621"
    data= localSQL.getData(sqlword)
    data['business_name']= data['business_name'].map(deStrCode)
    gdata= data.groupby(['done_date', 'relative_code', 'mark'])['rpt_contract_no'].count()
    for inde in gdata.index:
        lsdata= data[(data['done_date']== inde[0])&(data['relative_code']== inde[1])&(data['mark']== inde[2])]
        sqlword= "select * from SRDT where done_date= %s and sec_code= '%s' and\
            mark='%s' and business_name in ('现金替代划出','申购赎回过出') and sec_chg=0"%inde
        rcndata= localSQL.getData(sqlword)
        for i in range(min(len(lsdata), len(rcndata))):
            data.loc[lsdata.index[i], 'rpt_contract_no']= rcndata.iloc[i, 9]
    localSQL.updateRCNData(data)

def checkMarkData(data):
    lsdata= data[(data['sec_type']=='stock')&(data['sec_chg']!= 0)].groupby('mark')['sec_chg'].sum()
    lsdata= lsdata[(lsdata.index!='unmarked')&(lsdata.values!= 0)]
    if len(lsdata)== 0:
        return True
    codedata= data[(data['sec_type']=='stock')&(data['sec_chg']!= 0)&(data['mark']== lsdata.index[0])].groupby('sec_code')['sec_chg'].sum()
    code= codedata[codedata.values!= 0].index[0]
    print('error code: %s'% code)
    return False

def strip(text):
    try:
        return text.strip()
    except:
        return text
    
def getxlsData(fileName):
    xlsdata = pd.read_excel(fileName, sheet_name= 0)
    xlsdata.columns= list(map(strip, xlsdata.columns.values))
    for j in range(len(xlsdata.columns)):
        xlsdata.iloc[:, j]= xlsdata.iloc[:, j].map(strip)
    return xlsdata