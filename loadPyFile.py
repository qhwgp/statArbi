# -*- coding: utf-8 -*-
"""
Created on Wed Aug  5 00:00:15 2020

@author: sysadmin
"""


import dtcfg as cfg
import pandas as pd
import os, base64

def myDecode(encStr):
    return base64.b64decode(encStr).decode()

if __name__ == '__main__':
    t0 = cfg.timeStart()
    localSQL= cfg.MSSQL(cfg.host, cfg.user, cfg.pwd, cfg.lcdb)
    localSQL.Connect()
    if not localSQL.isConnect:
        print(localSQL.host + ' not connect')
    else:
        #load sql file
        sqlword="SELECT * FROM pyFileDT"
        fdata= pd.read_sql(sqlword, con=localSQL.conn)
        listFile= os.listdir()
        for inde in fdata.index:
            fileName= fdata.loc[inde, 'file_name'] +'.py'
            sqlmtime= fdata.loc[inde, 'mtime']
            if fileName== 'loadPyFile.py':
                continue
            if not fileName in listFile or int(os.stat(fileName).st_mtime)< sqlmtime:
                ftxt= myDecode(fdata.loc[inde, 'file_content'])
                with open(fileName, 'w+', encoding= 'utf-8') as f:
                        f.write(ftxt)