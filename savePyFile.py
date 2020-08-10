# -*- coding: utf-8 -*-
"""
Created on Tue Aug  4 21:59:28 2020

@author: sysadmin
"""

import dtcfg as cfg
import pandas as pd
import os, base64

def myEncode(oriStr):
    return base64.b64encode(oriStr.encode()).decode()

if __name__ == '__main__':
    t0 = cfg.timeStart()
    localSQL= cfg.MSSQL(cfg.host, cfg.user, cfg.pwd, cfg.lcdb)
    localSQL.Connect()
    if not localSQL.isConnect:
        print(localSQL.host + ' not connect')
    else:
        if input('save file to sql, overwrtite sql file, pls confirm [y/n]: ')== 'y':
            listFile= os.listdir()
            for filename in listFile:
                #filename= listFile[0]
                splitfile= filename.split('.')
                if splitfile[-1]== 'py':
                    mtime= int(os.stat(filename).st_mtime)
                    with open(filename, 'r', encoding= 'utf-8') as f:
                        fContent= f.read()
                    incodef= myEncode(fContent)
                    sqlword= "select top 1 mtime from pyFileDT where file_name= '%s' order by mtime desc"% splitfile[0]
                    data= pd.read_sql(sqlword, con=localSQL.conn)
                    if len(data)<1:
                        sqlword= "insert into pyFileDT values ('%s', %d, '%s')"% (splitfile[0], mtime, incodef)
                        localSQL.cur.execute(sqlword)
                        print('add file: '+ filename)
                    elif data.iloc[0,0]< mtime:
                        sqlword="update pyFileDT set file_content='%s', mtime=%d where file_name='%s'"% \
                            (incodef, mtime, splitfile[0])
                        localSQL.cur.execute(sqlword)
                        print('update file: '+ filename)
    #cfg.rebuildpyFileDT(localSQL)  
    
    
    