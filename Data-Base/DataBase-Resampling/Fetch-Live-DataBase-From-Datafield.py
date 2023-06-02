
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 17 14:41:08 2019

@author: Ahmad.Elsayed
"""
import pandas as pd
import cx_Oracle
import datetime
import csv
import time
import sys

def dbConnect():
    '''
    creates a standalone connection with the database
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
    '''
    How it works: 
        1- collect all trades done today 
        2- group them by ticker
        3- group each group by time (5 minutes)
        4- find first, hight, lowest and last value of 'price' and sum 'volume' for each 5 minutes group.
        5- Insert in database (if a 5 minute already )
    
    
    '''
    
    #print(egx30Sym)
    
    sql='SELECT T2.REUTERS,T1.EXEC_TIME,T1.TRADE_PRICE ,T1.VOLUME_TRADED FROM STOCK.TRADES T1 JOIN STOCK.SYMBOLINFO T2 ON T2.SYMBOL_CODE = T1.SYMBOL_CODE'
    cursor2 = con2.cursor()
    cursor2.execute(sql)
    
    res = cursor2.fetchmany(numRows=100000)
    
    data=pd.DataFrame(res,columns=['code','time','price','volume'])
    data=data.set_index('time', drop=True)
    data.index = pd.to_datetime(data.index)
    data=data.dropna()
    resampled={}
    symbols=data.code.unique()
    
    VWAP=0
    #print(symbols)
    for name in symbols:
        #sql='SELECT * FROM STOCK.TRADE_CHART5'
        #print(name)
        vwap=data.loc[data['code']==name,].copy()
        #vwap['volume']=data.loc[data['code']==name,'volume']
        #vwap['mul']=vwap['price']*vwap['volume']
        vwap=(vwap['price']*vwap['volume']).cumsum()
        #print(name,vwap,data.loc[data['code']==name,'price'],data.loc[data['code']==name,'volume'],data.loc[data['code']==name,'price']*data.loc[data['code']==name,'volume'])
        
        df=data.loc[data['code']==name,'price'].resample('5Min').ohlc()
        df['volume']=data.loc[data['code']==name,'volume'].resample('5Min').sum()
        #print(vwap)
        df['vwap'] = (vwap/data.loc[data['code']==name,'volume'].cumsum()).resample('5Min').last()
        #print(df)
        name=name.replace('.CA','').strip()
        #print('1',df)
        sql='SELECT * FROM STOCK.FILL_OHLCV WHERE Ticker = :name ORDER BY BARTIMESTAMP DESC'
        cursor2.execute(sql,[name])
        res = cursor2.fetchone()
        #print('2',res)
        if(res):
            resampled[name]=df.iloc[-1,:]
            tbIns = df.iloc[df.index.to_pydatetime()>res[6],:]
            tbUpd = df.iloc[df.index.to_pydatetime()==res[6],:]
            #print(tbIns,tbUpd,tbIns.index.to_pydatetime())
        else:
            tbIns=df.copy()
            tbUpd=[]
        tbIns=tbIns.dropna()
        #print(res[5],tbUpd['volume'][0])
        if(len(tbUpd)>0 and (res[5]!=tbUpd['volume'][0] and tbUpd['volume'][0] !=0.0)):
            #================================================================ Must Be activate ====================================================================================================================================
           # sql='update STOCK.FILL_OHLCV set OPEN=:1,HIGH=:2,LOW=:3,CLOSE=:4,VOLUME=:5 where Ticker = :6 AND BARTIMESTAMP=:7'
           #-------------------------------------------------------------------------------------------------------------------
           #-------------------------------------------------------------------------------------------------------------------
            #print(type(tbUpd.index.to_pydatetime()),tbUpd.index.to_pydatetime(),type(tbUpd['volume'].values[0]))
            #======================================================================Must Be deactivate==============================================================================================================================
            print([tbUpd['open'].values[0],tbUpd['high'].values[0],tbUpd['low'].values[0],tbUpd['close'].values[0],tbUpd['volume'].values[0].astype(float),name,tbUpd.index.to_pydatetime()[0]])
            print("///////////////////////////////////////////////////////////////////////////")
            #================================================================================Must Be activate============================================================================================================================
          # cursor2.execute(sql,[tbUpd['open'].values[0],tbUpd['high'].values[0],tbUpd['low'].values[0],tbUpd['close'].values[0],tbUpd['volume'].values[0].astype(float),name,tbUpd.index.to_pydatetime()[0]])
          #-------------------------------------------------------------------------------------------------------------------
          #-------------------------------------------------------------------------------------------------------------------
            #============================================================================================================================================================================================================
        
        if(len(tbIns)>0):
            lines=[]
            for index,row in tbIns.iterrows():
                try:
                    line=[0,1,2,3,4,5,6,7,8]
                    line[0]=name
                    line[1]=row['open']
                    line[2]=row['high']
                    line[3]=row['low']
                    line[4]=row['close']
                    line[5]=row['volume']
                    line[6]=index.to_pydatetime()
                    line[7]=1
                    line[8]=row['vwap']
                    #print(index.to_pydatetime())
                    lines.append(line)
                    #==================================================================Must Be deactivate==========================================================================================================================================
                    print(lines)
                    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                    #-------------------------------------------------------------------------------------------------------------------
                    #-------------------------------------------------------------------------------------------------------------------
                    #=================================================================Must Be activate===================================================================================================================================
                    #cursor2.execute("insert into FILL_OHLCV(TICKER,OPEN,HIGH,LOW,CLOSE,VOLUME,BARTIMESTAMP,ASSET,VWAP) values (:stock, :open,:high,:low,:close,:vol,:time,:1,:2)",line)
                    #-------------------------------------------------------------------------------------------------------------------
                    #-------------------------------------------------------------------------------------------------------------------
                    #==============================================Must Be activate========================================================================================================================================================
                    #con2.commit()
                    #-------------------------------------------------------------------------------------------------------------------
                    #-------------------------------------------------------------------------------------------------------------------
                    #============================================================================================================================================================================================================
                except Exception as e:
                    print(str(e))
        ##### insert to database step 
    con2.close()
    
    
    con2=dbConnect()
    sql='SELECT * FROM CASEINDEX'
    cursor2 = con2.cursor()
    cursor2.execute(sql)
    
    res = cursor2.fetchmany(numRows=100000)
    dataIn=pd.DataFrame(res,columns=['time','code','price'])
    
    dataIn=dataIn.set_index('time', drop=True)
    dataIn.index = pd.to_datetime(dataIn.index)
    dataIn=dataIn.dropna()
    indicies =dataIn.code.unique()
    print(indicies)
    try:
        for name in indicies:
            try:
                nameI=name.replace('EWI','').strip()
                print(nameI)
                if(nameI=='EGX30'):
                    sql='SELECT T2.REUTERS FROM CASE30_COMPANIES T1 JOIN STOCK.SYMBOLINFO T2 ON T2.SYMBOL_CODE = T1.SYMBOL_CODE'
                elif(nameI=='EGX70'):
                    sql='SELECT T2.REUTERS FROM EGX70_SYMBOLS_EWI T1 JOIN STOCK.SYMBOLINFO T2 ON T2.SYMBOL_CODE = T1.SYMBOL_CODE'
                elif(nameI=='EGX100'):
                    sql='SELECT T2.REUTERS FROM EGX100_SYMBOLS T1 JOIN STOCK.SYMBOLINFO T2 ON T2.SYMBOL_CODE = T1.SYMBOL_CODE'
                elif(nameI=='EGX50'):
                    sql='SELECT T2.REUTERS FROM EGX50_SYMBOLS T1 JOIN STOCK.SYMBOLINFO T2 ON T2.SYMBOL_CODE = T1.SYMBOL_CODE'
                elif(nameI=='EGX30 Capped'):
                    sql='SELECT T2.REUTERS FROM EGX30_CAP_SYMBOLS T1 JOIN STOCK.SYMBOLINFO T2 ON T2.SYMBOL_CODE = T1.SYMBOL_CODE'
                else:
                    continue
                
                cursor3 = con2.cursor()
                cursor3.execute(sql)
                #print(name,sql)
                indexSym = cursor3.fetchmany(numRows=110)
                
                indexSym =[s[0].replace('.CA','').strip() for s in indexSym]
                #print(indexSym)
                placeholders = [':%d' % i for i in range(len(indexSym))]
                
                sql="SELECT BARTIMESTAMP,SUM(VOLUME) FROM STOCK.FILL_OHLCV WHERE Ticker IN (%s)  GROUP BY BARTIMESTAMP ORDER BY BARTIMESTAMP DESC"  % ','.join(placeholders)
                #print(sql)
                cursor2.execute(sql,indexSym)
                res = cursor2.fetchmany(numRows=10000)
                volumes=pd.DataFrame(res,columns=['time','volume'])
                volumes=volumes.set_index('time', drop=True)
                volumes.index = pd.to_datetime(volumes.index)
                #print(volumes)
                #resampling new values to 5 minutes
                df=dataIn.loc[dataIn['code']==name,'price'].resample('5Min').ohlc()
                #name=name.replace('.CA','').strip()
                
                #df['volume']=data.loc[data['code']==name,'volume'].resample('5Min').sum()
                df=pd.concat([df, volumes], axis=1, join_axes=[df.index])
                df=df.dropna()
                #print(df)
                #getting previous values for the same asset
                
                sql='SELECT * FROM STOCK.FILL_OHLCV WHERE Ticker = :name ORDER BY BARTIMESTAMP DESC'
                cursor2.execute(sql,[nameI])
                res = cursor2.fetchone()
                #print('2',res)
                if(res and len(res)>0):
                    #resampled[name]=df.iloc[-1,:]
                    print('here')
                    tbIns = df.iloc[df.index.to_pydatetime()>res[6],:]
                    tbUpd = df.iloc[df.index.to_pydatetime()==res[6],:]
                    #print(tbIns,tbUpd,tbIns.index.to_pydatetime())
                else:
                    tbIns=df.copy()
                    tbUpd=[]
                tbIns=tbIns.dropna()
                #print(tbIns)
                #print(res[5],tbUpd['volume'][0])
                if(len(tbUpd)>0):
                    
                    if(res[5]!=tbUpd['volume'][0]):
                        #===============================================================Must Be activate========================================================================================================================================
                        #sql='update STOCK.FILL_OHLCV set OPEN=:1,HIGH=:2,LOW=:3,CLOSE=:4,VOLUME=:5 where Ticker = :6 AND BARTIMESTAMP=:7'
                        #-------------------------------------------------------------------------------------------------------------------
                        #-------------------------------------------------------------------------------------------------------------------
                        #============================================================================================================================================================================================================
                        #print(type(tbUpd.index.to_pydatetime()),tbUpd.index.to_pydatetime(),type(tbUpd['volume'].values[0]))
                        #=====================================================================Must Be deactivate===================================================================================================================================
                        print([tbUpd['open'].values[0],tbUpd['high'].values[0],tbUpd['low'].values[0],tbUpd['close'].values[0],tbUpd['volume'].values[0].astype(float),name,tbUpd.index.to_pydatetime()[0]])
                        #===========================================================================Must Be activate==============================================================================================================================
                        #cursor2.execute(sql,[tbUpd['open'].values[0],tbUpd['high'].values[0],tbUpd['low'].values[0],tbUpd['close'].values[0],tbUpd['volume'].values[0].astype(float),name,tbUpd.index.to_pydatetime()[0]])
                        #-------------------------------------------------------------------------------------------------------------------
                        #-------------------------------------------------------------------------------------------------------------------
                        #=========================================================================Must Be activate============================================================================================================================
                        #con2.commit()
                        #-------------------------------------------------------------------------------------------------------------------
                        #-------------------------------------------------------------------------------------------------------------------
                        #============================================================================================================================================================================================================
                if(len(tbIns)>0):
                    lines=[]
                    for index,row in tbIns.iterrows():
                        try:
                            line=[0,1,2,3,4,5,6,7,8]
                            line[0]=nameI
                            line[1]=row['open']
                            line[2]=row['high']
                            line[3]=row['low']
                            line[4]=row['close']
                            
                            line[6]=index.to_pydatetime()
                            line[5]=row['volume']
                            line[7]=0
                            line[8]=row['close']
                            #print(index.to_pydatetime())
                            lines.append(line)
                            #print(lines)
                            #========================================================================Must Be deactivate===========================================================================================================================
                            print(lines)
                            #=========================================================Must Be activate=========================================================================================================================================
                            #cursor2.execute("insert into FILL_OHLCV(TICKER,OPEN,HIGH,LOW,CLOSE,VOLUME,BARTIMESTAMP,ASSET,VWAP) values (:stock, :open,:high,:low,:close,:vol,:time,:1,:2)",line)
                            #-------------------------------------------------------------------------------------------------------------------
                            #-------------------------------------------------------------------------------------------------------------------
                            #==============================================================Must Be activate=========================================================================================================================================
                            #con2.commit()
                            #-------------------------------------------------------------------------------------------------------------------
                            #-------------------------------------------------------------------------------------------------------------------
                            #============================================================================================================================================================================================================
                        except Exception as e:
                            print(str(e))
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                print(str(e), exc_tb.tb_lineno)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(str(e), exc_tb.tb_lineno)
    con2.close()
    time.sleep(30)