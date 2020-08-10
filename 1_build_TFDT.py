# -*- coding: utf-8 -*-
"""
Created on Wed Jul 15 16:35:27 2020

@author: WAP

get data to TFDT, original data
"""

import dtcfg as cfg
    
if __name__ == '__main__':
    t0 = cfg.timeStart()
    clsSQL= cfg.MSSQL(cfg.host, cfg.user, cfg.pwd, cfg.dcadb)
    clsSQL.Connect()
    localSQL= cfg.MSSQL(cfg.host, cfg.user, cfg.pwd, cfg.lcdb)
    localSQL.Connect()
    if not clsSQL.isConnect:
        print(clsSQL.host + ' not connect')
    elif not localSQL.isConnect:
        print(localSQL.host + ' not connect')
    else:
        cfg.syncData(clsSQL, localSQL)
        #cfg.rebuildDT(localSQL, 'TFDT')
    cfg.timeEnd(t0)