# -*- coding: utf-8 -*-
"""
Created on Sun Aug  2 18:22:50 2020

@author: sysadmin
"""

import os
import pandas as pd
import dtcfg as cfg

localSQL= cfg.MSSQL(cfg.host, cfg.user, cfg.pwd, cfg.lcdb)
localSQL.Connect()
listfile= os.listdir(".\constituent")
serSRInfo= pd.read_csv('srInfo.csv', index_col= 0, header= None).iloc[:, 0]
serSRInfo.index= serSRInfo.index.map(str)
for file in listfile:
    #file= listfile[0]
    mystr= file.split('.')[0]
    mystr= mystr.split('_')
    """
    mypdData= pd.read_csv(".\constituent\\" + file, encoding='GB18030')
    mypdData['wind_code']= mypdData['wind_code'].map(lambda x:str(x).zfill(6))
    mypdData.rename(columns= {'CLOSE':'DAY_CLOSE'}, inplace= True)
    mypdData['etf_code']= mystr[1]
    mypdData['busi_date']= int(mystr[0])
    mypdData.to_sql('constituentDT', con= localSQL.engine, if_exists= 'append', index= False)
    """
    sqlword= "insert into SRInfoDT values ('%s', %s, %d)"% (mystr[1], mystr[0], serSRInfo[mystr[1]])
    localSQL.cur.execute(sqlword)
    
    data= pd.read_sql("select volume from SRInfoDT where etf_code='515780' and busi_date=20200714", con=localSQL.conn) 
    