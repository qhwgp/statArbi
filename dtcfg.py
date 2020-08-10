# -*- coding: utf-8 -*-
"""
Created on Tue Jul 28 09:14:56 2020

@author: WAP
"""
from pymssql import connect
from sqlalchemy import create_engine
import time as ti
import pandas as pd
from datetime import date, datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

startDate= 20200801

host='172.21.6.152'
user='wanggp'
pwd='Wanggp@0511'
dcadb='data_ceneter_all'
lcdb='Wangprivate'
rcddb= 'oversea'

jqUser= '18665883365'
jqPwd= 'Hu12345678'

col_record= ['investor', 'trader', 'broker', 'account', 'stkcd', 'zqjc', 'trday',
       'countervail', 'maxstrategy', 'midstrategy', 'minstrategy', 'currency',
       'quantity', 'trprc', 'trvolume', 'commission', 'mark']

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
        sql= 'select * from TFDT where busi_date=%d order by serial_no'% tDate
        data= pd.read_sql(sql, con=self.conn)
        data['business_name']= data['business_name'].map(deStrCode)
        return data
    
    def getUnmarkedData(self,tDate, lastDate):
        data= self.getDateData(tDate)
        data['sec_type']= data['sec_code'].map(code_type)
        data['mark']= 'unmarked'
        sql= "select * from SRDT where busi_date=%d and mark='hold'"% lastDate
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
        PRIMARY KEY(serial_no,busi_date))
        """
        self.cur.execute(sqlword)
        data.to_sql('tempDT', con= self.engine, if_exists= 'append', index= False)
        sql="UPDATE SRDT SET mark=t.mark, rpt_contract_no=t.rpt_contract_no FROM (SELECT * from tempDT) AS t WHERE \
            SRDT.serial_no=t.serial_no and SRDT.busi_date=t.busi_date"
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
        (business_name in ('现金替代补款', '现金替代退款') and done_date=%d and busi_date<>%d and len(rpt_contract_no)< 2  and len(done_no)< 2)
        ) 
        order by sec_code, serial_no
        """%(scode, tdate, scode, tdate, scode, tdate, tdate)
        data= pd.read_sql(sqlword, con=self.conn)
        return data
    
    def getRedeemData(self, scode, tdate):
        sqlword = """
        select * from SRDT where mark='%s' and 
        (business_name not in ('ETF赎回过户费冻结','ETF赎回过户费解冻')) and 
        (
        (done_no in (select done_no from SRDT where busi_date=%d and business_name in ('ETF赎回','ETF现金赎回份额确认') and 
                     sec_code='%s'  and done_no<>'0')) or
        (rpt_contract_no in (select rpt_contract_no from SRDT where busi_date=%d and 
                             business_name in ('ETF赎回','ETF现金赎回份额确认') and sec_code='%s'))
        )
        order by sec_code, serial_no
        """%(scode, tdate, scode, tdate, scode)
        data= pd.read_sql(sqlword, con=self.conn)
        if len(data)> 0:
            return data
        sqlword= "select * from SRDT where sec_code='%s' and business_name in ('ETF现金赎回冻结', '现金替代划入') and \
            done_date=%d"% (scode, tdate)
        return pd.read_sql(sqlword, con=self.conn)
    
    def getRecordData(self, tdate):
        sqlword= "select * from pengzf.ETF_trorder where trday='%d' and  ((mark='虚拟申购' and quantity> 0.1) or \
            (mark= '虚拟赎回' and quantity< -0.1))"% tdate
        data= pd.read_sql(sqlword, con=self.conn)
        data= data.drop(['xh', 'strategy', 'insertime'], axis= 1)
        for col in data.columns:
            data[col]= data[col].map(strip)
        return data
    
    def getETFConstituent(self, tdate, code):
        sqlword= "select * from constituentDT where etf_code='%s' and busi_date=%d order by wind_code"% (code, tdate)
        data= pd.read_sql(sqlword, con=self.conn)
        data.set_index('wind_code', inplace= True)
        data= data.drop(['etf_code', 'busi_date'], axis= 1)
        return data
    
    def getSRInfo(self, tdate, code):
        sqlword= "select volume from SRInfoDT where etf_code='%s' and busi_date=%d"% (code, tdate)
        data= pd.read_sql(sqlword, con=self.conn)
        if len(data)== 1:
            return data.iloc[0,0]
        else:
            return 0
        
    def getStockLast(self, tdate, code):
        sqlword= "select top 1 DAY_CLOSE from constituentDT where wind_code='%s' and busi_date=%d"% (code, tdate)
        data= pd.read_sql(sqlword, con=self.conn)
        if len(data)== 1:
            return data.iloc[0,0]
        else:
            return -1
    
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
    PRIMARY KEY(serial_no,busi_date)
    )
    """
    localSQL.cur.execute(sqlword)
    
def rebuildConstituentDT(localSQL):
    try:
        localSQL.cur.execute('drop table constituentDT')
    except:
        pass
    sqlword = """
    CREATE TABLE  constituentDT (
    wind_code VARCHAR(30) NOT NULL,
    sec_name VARCHAR(30),
    volume  int,
    cash_substitution_mark VARCHAR(30),
    cash_substitution_premium_ratio  float,
    fixed_substitution_amount float,
    subscribefixedamount float,
    cashdiscountratio    float,
    redemptionfixedamount float,
    PRE_CLOSE  float,
    DAY_CLOSE  float,
    etf_code VARCHAR(30) NOT NULL,
    busi_date int NOT NULL,
    PRIMARY KEY(wind_code, etf_code, busi_date)
    )
    """
    localSQL.cur.execute(sqlword)
    
def rebuildSRInfoDT(localSQL):
    try:
        localSQL.cur.execute('drop table SRInfoDT')
    except:
        pass
    sqlword = """
    CREATE TABLE  SRInfoDT (
    etf_code VARCHAR(30) NOT NULL,
    busi_date int NOT NULL,
    volume  int,
    PRIMARY KEY(etf_code, busi_date)
    )
    """
    localSQL.cur.execute(sqlword)
    
def rebuildpyFileDT(localSQL):
    try:
        localSQL.cur.execute('drop table pyFileDT')
    except:
        pass
    sqlword = """
    CREATE TABLE  pyFileDT (
    file_name VARCHAR(30) NOT NULL,
    mtime int NOT NULL,
    file_content  text,
    PRIMARY KEY(file_name, mtime)
    )
    """
    localSQL.cur.execute(sqlword)
    
def rebuildjqIndexData(localSQL):
    try:
        localSQL.cur.execute('drop table jqIndexDataDT')
    except:
        pass
    sqlword = """
    CREATE TABLE  jqIndexDataDT (
    mtime datetime NOT NULL,
    tickret float NOT NULL,
    amntw  float,
    jqCode VARCHAR(30) NOT NULL,
    tdate int NOT NULL,
    PRIMARY KEY(mtime, jqCode)
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
               (data['sec_type']!='other')&(data['business_name']!= '股息入帐')]
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
        #inde= 3696
        if undoData.loc[inde, 'mark']!= 'unmarked':
            continue
        scode= undoData.loc[inde, 'sec_code']
        #scode='002049'
        #lsdata= data[(data['sec_code']== scode)]
        #qty= undoData.iloc[i, 6]
        lsdata= undoData[(undoData['sec_code']== scode)&(undoData['mark']== 'unmarked')]
        #lsindex= lsdata.index
        #lsdata= undodata.loc[lsindex]
        gdata= lsdata.groupby(['business_name','relative_code'])['fund_chg','sec_chg','done_amt'].sum()
        if undoData.loc[inde, 'business_name']== '证券买入':
            qty= gdata.loc[('证券买入', scode), 'sec_chg']
            if '申购赎回过出' in gdata.index:
                for rcode in gdata.loc['申购赎回过出','sec_chg'].index:
                    #rcode= gdata.loc['申购赎回过出','sec_chg'].index[1]
                    lsdata= undoData[(undoData['sec_code']== scode)&(undoData['mark']== 'unmarked')&(undoData['business_name'].isin(['证券买入', '申购赎回过出']))]
                    gdata= lsdata.groupby(['business_name','relative_code'])['fund_chg','sec_chg','done_amt'].sum()
                    outqty= gdata.loc[('申购赎回过出', rcode), 'sec_chg']
                    ssdata= lsdata[((lsdata['relative_code']== rcode)&(lsdata['business_name']== '申购赎回过出'))]
                    rcn= ssdata.iloc[0, 9]
                    bdate= ssdata.iloc[0, 1]
                    if -outqty== qty:
                        sdata= lsdata[((lsdata['relative_code']== rcode)|(lsdata['business_name']== '证券买入'))]
                        undoData.loc[sdata.index, 'mark']= rcode
                        sdata= lsdata[lsdata['business_name']== '证券买入']
                        undoData.loc[sdata.index, 'rpt_contract_no']= rcn
                        gssdata= ssdata.groupby('busi_date')['sec_chg'].sum()
                        if len(gssdata)>1:
                            for indedate in gssdata.index:
                                apd= undoData.loc[inde]
                                nadp+= 1
                                seqno= str(tdate)+ str(nadp).zfill(4)
                                apd.name= seqno
                                apd.serial_no= seqno
                                apd.rpt_contract_no= ssdata[ssdata['busi_date']== indedate].iloc[0, 9]
                                apd.done_no= 'append'
                                ssoutqty= gssdata[indedate]
                                if bdate== indedate:
                                    apd.sec_chg= -qty- ssoutqty
                                    apd.fund_chg= -(qty+ ssoutqty)/qty* gdata.loc[('证券买入', scode), 'fund_chg']
                                    apd.done_amt= -(qty+ ssoutqty)/qty* gdata.loc[('证券买入', scode), 'done_amt']
                                else:
                                    apd.sec_chg= -ssoutqty
                                    apd.fund_chg= -ssoutqty/qty* gdata.loc[('证券买入', scode), 'fund_chg']
                                    apd.done_amt= -ssoutqty/qty* gdata.loc[('证券买入', scode), 'done_amt']
                                apd.mark= rcode
                                undoData= undoData.append(apd)
                """        #wap
                    elif -outqty< qty:
                        sdata= lsdata[((lsdata['relative_code']== rcode)|(lsdata['business_name']== '证券买入'))]
                        undoData.loc[sdata.index, 'mark']= rcode
                        sdata= lsdata[lsdata['business_name']== '证券买入']
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
                    """
        elif undoData.loc[inde, 'business_name']== '证券卖出':
            qty= gdata.loc[('证券卖出', scode), 'sec_chg']
            if '申购赎回过入' in gdata.index:
                for rcode in gdata.loc['申购赎回过入','sec_chg'].index:
                    #rcode= gdata.loc['申购赎回过入','sec_chg'].index[0]
                    lsdata= undoData[(undoData['sec_code']== scode)&(undoData['mark']== 'unmarked')&(undoData['business_name'].isin(['证券卖出', '申购赎回过入']))]
                    outqty= gdata.loc[('申购赎回过入', rcode), 'sec_chg']
                    ssdata= lsdata[((lsdata['relative_code']== rcode)&(lsdata['business_name']== '申购赎回过入'))]
                    rcn= ssdata.iloc[0, 9]
                    bdate= ssdata.iloc[0, 1]
                    if outqty== -qty:
                        sdata= lsdata[(lsdata['relative_code']== rcode)|(lsdata['business_name']== '证券卖出')]
                        undoData.loc[sdata.index, 'mark']= rcode
                        sdata= lsdata[lsdata['business_name']== '证券卖出']
                        undoData.loc[sdata.index, 'rpt_contract_no']= rcn
                        
                        gssdata= ssdata.groupby('busi_date')['sec_chg'].sum()
                        if len(gssdata)>1:
                            for indedate in gssdata.index:
                                apd= undoData.loc[inde]
                                nadp+= 1
                                seqno= str(tdate)+ str(nadp).zfill(4)
                                apd.name= seqno
                                apd.serial_no= seqno
                                apd.rpt_contract_no= ssdata[ssdata['busi_date']== indedate].iloc[0, 9]
                                apd.done_no= 'append'
                                ssoutqty= gssdata[indedate]
                                if bdate== indedate:
                                    apd.sec_chg= -qty- ssoutqty
                                    apd.fund_chg= -(qty+ ssoutqty)/qty* gdata.loc[('证券卖出', scode), 'fund_chg']
                                    apd.done_amt= -(qty+ ssoutqty)/qty* gdata.loc[('证券卖出', scode), 'done_amt']
                                else:
                                    apd.sec_chg= -ssoutqty
                                    apd.fund_chg= -ssoutqty/qty* gdata.loc[('证券卖出', scode), 'fund_chg']
                                    apd.done_amt= -ssoutqty/qty* gdata.loc[('证券卖出', scode), 'done_amt']
                                apd.mark= rcode
                                undoData= undoData.append(apd)
                        """        
                    elif outqty< -qty:
                        sdata= lsdata[((lsdata['relative_code']== rcode)|(lsdata['business_name']== '证券卖出'))]
                        undoData.loc[sdata.index, 'mark']= rcode
                        sdata= lsdata[lsdata['business_name']== '证券卖出']
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
                    """
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
                    apd.rpt_contract_no= ''
                    apd.mark= 'trade'
                elif lsdata.loc[inde, 'business_name']== '证券卖出':
                    undoData.loc[inde, 'mark']= 'trade'
                    apd.business_name= '证券买入'
                    apd.done_amt= -qty* unitamt
                    apd.rpt_contract_no= ''
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
    sqlword= "select * from SRDT where business_name in ('现金替代退款','现金替代补款') and len(rpt_contract_no)<9 and len(done_no)<2 and done_date>20200621"
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

#4_build_record
def checkCode(theroVol, transVol, tradeVol, transFund):
    if transVol+ tradeVol!= 0:
        return 1
    elif theroVol+ transVol!= 0 and transFund== 0:
        return 1
    elif theroVol+ transVol== 0 and transFund!= 0:
        return 1
    else:
        return 0

def getConsCheckSubData(localSQL, etfcode, data, tdate):
    etfUnit= localSQL.getSRInfo(tdate, etfcode)
    pdData= localSQL.getETFConstituent(tdate, etfcode)
    lsdata= data[(data['business_name']== 'ETF申购')]
    try:
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
    except:
        pdData= pd.DataFrame()
    return pdData

def getConsCheckRedeemData(localSQL, etfcode, data, tdate):
    etfUnit= localSQL.getSRInfo(tdate, etfcode)
    pdData= localSQL.getETFConstituent(tdate, etfcode)
    if len(pdData)== 0:
        return pdData
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
    pdr= pd.DataFrame(columns= col_record)
    #虚拟申购抵消
    record['quantity']= -record['quantity']
    record['trvolume']= -record['trvolume']
    record['mark']= '虚拟申购抵消'
    pdr= pdr.append(record, ignore_index= True)
    #申购
    lsdata=data[(data['sec_type']== 'etf')|(data['sec_chg']== 0)]
    record['mark']= '申购'
    record['quantity']= lsdata['sec_chg'].sum()
    record['trvolume']= 0
    record['commission']= -lsdata['fund_chg'].sum()
    record['trprc']= 0
    pdr= pdr.append(record, ignore_index= True)
    #买入
    lsdata=data[data['business_name']=='证券买入']\
                        .groupby('sec_code')['fund_chg', 'sec_chg', 'done_amt'].sum()
    record['mark']= ''
    record['zqjc']= ''
    for scode in lsdata.index:
        record['stkcd']= scode+ ' CH Equity'
        record['quantity']= lsdata.loc[scode, 'sec_chg']
        record['trvolume']= lsdata.loc[scode, 'done_amt']
        record['trprc']= record['trvolume']/ record['quantity']
        record['commission']= -lsdata.loc[scode, 'fund_chg']-record['trvolume']
        pdr= pdr.append(record, ignore_index= True)
    #过出
    lsdata=data[(data['sec_type']!= 'etf')&(data['business_name']!='证券买入')&((data['sec_chg']!= 0))]\
                        .groupby('sec_code')['fund_chg', 'sec_chg', 'done_amt'].sum()
    record['mark']= '申购赎回过出'
    for scode in lsdata.index:
        record['stkcd']= scode+ ' CH Equity'
        record['quantity']= lsdata.loc[scode, 'sec_chg']
        record['trvolume']= 0
        record['trprc']= 0
        record['commission']= -lsdata.loc[scode, 'fund_chg']
        pdr= pdr.append(record, ignore_index= True)
    return pdr

def getRedeemRecord(etfcode, data, record):
    pdr= pd.DataFrame(columns= col_record)
    #虚拟赎回抵消
    #record= xlsdata[(xlsdata['stkcd']== etfcode+ ' CH Equity')&(xlsdata['mark']== '虚拟赎回')].iloc[0]
    record['quantity']= -record['quantity']
    record['trvolume']= -record['trvolume']
    record['mark']= '虚拟赎回抵消'
    pdr= pdr.append(record, ignore_index= True)
    #赎回
    lsdata=data[(data['sec_type']== 'etf')|(data['sec_chg']== 0)]
    record['mark']= '赎回'
    record['quantity']= lsdata['sec_chg'].sum()
    record['trvolume']= 0
    record['commission']= -lsdata['fund_chg'].sum()
    record['trprc']= 0
    pdr= pdr.append(record, ignore_index= True)
    #卖出
    lsdata=data[data['business_name']=='证券卖出']\
                        .groupby('sec_code')['fund_chg', 'sec_chg', 'done_amt'].sum()
    record['mark']= ''
    record['zqjc']= ''
    for scode in lsdata.index:
        record['stkcd']= scode+ ' CH Equity'
        record['quantity']= lsdata.loc[scode, 'sec_chg']
        record['trvolume']= -lsdata.loc[scode, 'done_amt']
        record['trprc']= record['trvolume']/ record['quantity']
        record['commission']= -lsdata.loc[scode, 'fund_chg']- record['trvolume']
        pdr= pdr.append(record, ignore_index= True)
    #过入
    lsdata=data[(data['sec_type']!= 'etf')&(data['business_name']!='证券卖出')&((data['sec_chg']!= 0))]\
                        .groupby('sec_code')['fund_chg', 'sec_chg', 'done_amt'].sum()
    record['mark']= '申购赎回过入'
    for scode in lsdata.index:
        record['stkcd']= scode+ ' CH Equity'
        record['quantity']= lsdata.loc[scode, 'sec_chg']
        record['trvolume']= 0
        record['trprc']= 0
        record['commission']= -lsdata.loc[scode, 'fund_chg']
        pdr= pdr.append(record, ignore_index= True)
    return pdr