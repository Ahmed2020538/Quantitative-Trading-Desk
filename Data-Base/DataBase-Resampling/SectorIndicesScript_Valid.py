# -*- coding: utf-8 -*-
"""
Created on Tue Apr  6 12:54:37 2021

@author: aya.adel
"""
import pandas as pd
import cx_Oracle
import datetime
import csv
import time
import sys

def dbConnect():
    '''
    establish a standalone connection with the database
    parameters:
        none
        
    return: 
       con: cx_oracle connection
    '''
    
    con = cx_Oracle.connect('STOCK/P3rXdM5HbSgQRmCS@10.1.20.41:1521/STOCK')
    print (con.version)
    return con


while(1):
    
    con2=dbConnect()
#    selects all sectors data that occured today and store it in df
    sql_sectors = "SELECT REPLACE(SECTOR_DESC,' ','') AS SECTOR_CODE,INDEXTIME,INDEXVALUE FROM CASE_SECTOR_INDEX"
    cursor2 = con2.cursor()    
    df_sectors = pd.read_sql(sql_sectors, con2)
    
#    modify the sector names and removing extra letters/spaces to match the database ticker field 
    df_sectors['SECTOR_CODE'] = df_sectors['SECTOR_CODE'].replace(['Shipping&TransportationServices','IndustrialGoods,ServicesandAutomobiles','IT,Media&CommunicationServices','Contracting&ConstructionEngineering'],['Shipping&Transportation','Indust.Goods,&Automobiles','IT,Media&Comm',  'Contracting&Construction'])
    extras = [',', ' ', '&', '-' ]
    for letter in extras:
        df_sectors['SECTOR_CODE'] = df_sectors['SECTOR_CODE'].str.replace(letter,'')
        
    data_sectors = df_sectors.set_index('INDEXTIME', drop=True)
    data_sectors.index = pd.to_datetime(data_sectors.index)
    data_sectors = data_sectors.dropna()
    sectors = list(data_sectors.SECTOR_CODE.unique())
    
    for name in sectors:
#         resamples the data into 5 minutes data
        df_resampled = data_sectors.loc[ data_sectors['SECTOR_CODE'] == name,'INDEXVALUE'].resample('5Min').ohlc()

#        fetch last resampled bar that was inserted into the database to decide where to continue
        sql_last = 'SELECT * FROM STOCK.FILL_OHLCV WHERE Ticker = :name ORDER BY BARTIMESTAMP DESC'
        cursor2.execute(sql_last,[name.upper()])
        res = cursor2.fetchone()
        
        if(res):
            tbIns = df_resampled.iloc[df_resampled.index.to_pydatetime() > res[6], :]
            tbUpd = df_resampled.iloc[df_resampled.index.to_pydatetime() == res[6], :]
        else:
            tbIns=df_resampled.copy()
            tbUpd=[]
        tbIns=tbIns.dropna()
        
        if(len(tbUpd)>0):
            sql='update STOCK.FILL_OHLCV set OPEN=:1,HIGH=:2,LOW=:3,CLOSE=:4,VOLUME=:5 where Ticker = :6 AND BARTIMESTAMP=:7'
            cursor2.execute(sql, [tbUpd['open'].values[0], tbUpd['high'].values[0], tbUpd['low'].values[0], tbUpd['close'].values[0], 0, name.upper(), tbUpd.index.to_pydatetime()[0]])
            print([tbUpd['open'].values[0], tbUpd['high'].values[0], tbUpd['low'].values[0], tbUpd['close'].values[0], 0, name.upper(), tbUpd.index.to_pydatetime()[0]])

        if(len(tbIns)>0):
            lines=[]
            for index,row in tbIns.iterrows():
                try:
                    line=[0,1,2,3,4,5,6,7,8]
                    line[0]=name.upper()
                    line[1]=row['open']
                    line[2]=row['high']
                    line[3]=row['low']
                    line[4]=row['close']
                    line[5]=0
                    line[6]=index.to_pydatetime()
                    line[7]=0
                    line[8]=0
                    lines.append(line)                   
                    cursor2.execute("insert into FILL_OHLCV(TICKER,OPEN,HIGH,LOW,CLOSE,VOLUME,BARTIMESTAMP,ASSET,VWAP) values (:stock, :open,:high,:low,:close,:vol,:time,:1,:2)",line)
                    con2.commit()
                    print(name.upper())
                except Exception as e:
                    print(str(e))
    con2.close()
    time.sleep(30)