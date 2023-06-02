# -*- coding: utf-8 -*-
"""
Created on Sun Jan 31 16:20:54 2021

@author: aya.adel
"""

import pandas as pd
import cx_Oracle
import datetime
import csv
import time


def dbConnect():
    con = cx_Oracle.connect('STOCK/P3rXdM5HbSgQRmCS@10.1.20.41:1521/STOCK')
    print (con.version)
    return con


con2 = dbConnect()


def PriceAdjustment(new_price,old_price,ticker,adjust_y,adjust_m,adjust_d):
    
  
    '''Price Adjustment Function : updates the ohlcv prices to the new adjusted price
    ------------
    parameters:
        new_price: float
            new price after adjustment
        old_price: float
            old price before adjustment
        ticker: string
            Symbol name eg.'COMI'
        adjust_y: int
            year of adjustment date
        adjust_m: int
            month of adjustment date 
        adjust_d: int
            day after the adjustment prcess eg. (6)
        
    return:
        none
        '''
     
    percent = new_price/old_price    
    
    sql1 ='update STOCK.FILL_OHLCV set OPEN=OPEN*:1,HIGH=HIGH*:1,LOW=LOW*:1,CLOSE=CLOSE*:1,VOLUME=VOLUME/:1,VWAP=VWAP*:1 where Ticker = :2 AND BARTIMESTAMP < :3 '
    sql2 ='update STOCK.FILL_OHLCV_1MIN set OPEN=OPEN*:1,HIGH=HIGH*:1,LOW=LOW*:1,CLOSE=CLOSE*:1,VOLUME=VOLUME/:1,VWAP=VWAP*:1 where Ticker = :2 AND BARTIMESTAMP < :3 '    
    
    param = [percent,percent,percent,percent,percent,percent,ticker,datetime.datetime(adjust_y,adjust_m,adjust_d,0,0)]
    cursor4 = con2.cursor()
    
    try:
        cursor4.execute(sql1,param)
        cursor4.execute(sql2,param)
        con2.commit()

    except Exception as e:
        print(str(e))

'''new price, old price, ticker, year, month,day of the first day after the adjustment '''
PriceAdjustment(2.153, 2.8, 'RMDA', 2021, 9, 23)


#sql = 'SELECT * FROM STOCK.FILL_OHLCV WHERE ticker=:1 ORDER BY bartimestamp DESC'
#sql_2 = 'SELECT SYMBOL_CODE, EXEC_TIME, TRADE_PRICE FROM STOCK.TRADES WHERE SYMBOL_CODE = :1 '
#sql='SELECT T2.REUTERS,T1.EXEC_TIME,T1.TRADE_PRICE ,T1.VOLUME_TRADED FROM STOCK.TRADES T1 JOIN STOCK.SYMBOLINFO T2 ON T2.SYMBOL_CODE = T1.SYMBOL_CODE WHERE T2.REUTERS = :1'
#
#cursor2 = con2.cursor()
#cursor2.execute(sql_2,['EGS745L1C014'])
#res2 = cursor2.fetchall()
#res = (pd.DataFrame(cursor2.fetchall())).sort()
#lst = sorted(res[0])











#cursor2 = con2.cursor()

#for name in ewi_names:
#    sql_del = "DELETE FROM STOCK.FILL_OHLCV WHERE TICKER =:1 and BARTIMESTAMP >:2"
#    param = [str(name),datetime.datetime(2021,2,11,13,30)]
#    cursor2.execute(sql_del,param)
#    con2.commit()




#ewi_names = ['EGX30LAST', 'EGX50LAST' , 'EGX70LAST', 'EGX100LAST']
#new_ewi_names = ['EGX30LASTEWI', 'EGX50LASTEWI' , 'EGX70LASTEWI', 'EGX100LASTEWI']
#
#for i in range(len(ewi_names)):
#    print(str(new_ewi_names[i]),ewi_names[i])
#    sql3='update STOCK.FILL_OHLCV set Ticker= :1 where Ticker = :2 '
#    #sql4='update STOCK.FILL_OHLCV_1MIN set Ticker= :1 where Ticker = :2 AND BARTIMESTAMP > :3 AND BARTIMESTAMP < :4'
#    
#    param = [str(new_ewi_names[i]),str(ewi_names[i])]
#    cursor2.execute(sql3,param)
#    #cursor2.execute(sql4,param)
#    con2.commit()










#sql='update STOCK.FILL_OHLCV set OPEN=:1,HIGH=:2,LOW=:3,CLOSE=:4,VWAP=:5 where Ticker = :6 AND BARTIMESTAMP<:7 AND BARTIMESTAMP>:8'
#param = [2.58,2.59,2.58,2.58,2.58,'RREI',datetime.datetime(2008,11,28,0,0),datetime.datetime(2008,11,27,0,0)]
#cursor1.execute(sql,param)



#dfall_prices = pd.DataFrame()
#for symbol in egx100_symbols:
#    select = 'SELECT * FROM STOCK.FILL_OHLCV WHERE TICKER =:1 and BARTIMESTAMP <:2 and BARTIMESTAMP >:3'
#    param = ['RREI',datetime.datetime(2008,11,28,0,0),datetime.datetime(2008,11,27,0,0)]
#    dfsymbol = pd.read_sql(select, con2, params=param)
#    #    lst = cursor1.fetchall()
#    dfall_prices = dfall_prices.append(dfsymbol)
#
#tickers =dfall_prices['TICKER'].unique()

        
        
        
        
        #all_prices = all_prices.set_index('BARTIMESTAMP',inplace = True)
#        all_prices[price_columns] =all_prices[price_columns]*(1/30)
#        ewi_prices = (pd.DataFrame(all_prices[price_columns +['VOLUME']].sum(axis = 0))).T
#        ewi_prices['BARTIMESTAMP'] = max(all_prices.index)
#        ewi_prices['ASSET']
    
    #for symbol in egx100_symbols:
    ##    for price in price_columns:
    #    sel_query = 'SELECT OPEN,HIGH,LOW,CLOSE,VWAP,VOLUME, FROM STOCK.FILL_OHLCV WHERE TICKER = :1 ORDER BY BARTIMESTAMP DESC'
    #    param = [str(symbol)]
    #    cursor1.execute(sel_query,param)
    ##    df = pd.read_sql(sel_query, con2,  params=param, chunksize=2)
    #    lst = [(element*1/len(egx100_symbols)) for element in cursor1.fetchone()]
    ##    for element in cursor1.fetchone():
    ##        print(element)
    #    open_prices.append(lst)
    #
    #zipped = [sum(elements) for elements in list(zip(*open_prices))]
    #zipped
    #sel_query = """SELECT BARTIMESTAMP,OPEN,HIGH,LOW,CLOSE,VWAP FROM
    #                    (SELECT s.*,
    #                    ROW_NUMBER() OVER (ORDER BY BARTIMESTAMP DESC) AS myr
    #                    FROM STOCK.FILL_OHLCV s
    #                    WHERE TICKER = :1 AND BARTIMESTAMP > :2)
    #                    WHERE myr =1 """

#sql='SELECT T2.REUTERS,T1.EXEC_TIME,T1.TRADE_PRICE ,T1.VOLUME_TRADED FROM STOCK.TRADES_D T1 JOIN STOCK.SYMBOLINFO T2 ON T2.SYMBOL_CODE = T1.SYMBOL_CODE WHERE T1.FLAG_D = 0'
#sql='SELECT * FROM OHLCV_1MIN'
#sql="SELECT * FROM (SELECT OPEN, BARTIMESTAMP FROM FILL_OHLCV WHERE TICKER='COMI' ORDER BY BARTIMESTAMP DESC) WHERE ROWNUM <=20 ORDER BY BARTIMESTAMP ASC"
#sql="SELECT * FROM (SELECT T1.BARTIMESTAMP as \"date\", T2.BARTIMESTAMP FROM FILL_OHLCV T1 JOIN FILL_OHLCV T2 ON T1.BARTIMESTAMP = T2.BARTIMESTAMP AND T1.TICKER=T2.TICKER WHERE T1.TICKER='COMI' ORDER BY T1.BARTIMESTAMP DESC ) WHERE ROWNUM <=20 ORDER BY BARTIMESTAMP ASC"
#sql='SELECT * FROM CASE_SECTOR_INDEX'
#cursor3 = con2.cursor()
#cursor3.execute(sql)
#res = cursor3.fetchmany(numRows=50)
#print(res)

'''
volumes=pd.DataFrame(res,columns=['ticker','time','last','volume'])
volumes['flag']=0

volumes['ticker']=[str(s[1]).replace('.CA','').strip() for s in volumes['ticker'].iteritems()]
volumes.to_csv('testTicks.csv')
print(volumes)
'''
#time2=pd.Timestamp('2019-4-20')
#sql='SELECT SYMBOL_CODE,TRADE_DATE,TRADE_VOLUME,TRADE_VALUE,TRADE_COUNT,OPEN_PRICE,HIGH_PRICE,LOW_PRICE,CLOSE_PRICE FROM STOCK.SYMBOLHISTORY WHERE TRADE_DATE > :time'
#sql='SELECT SYMBOL_CODE,SYMBOL_TYPE,SECTOR_ID,ENG_NAME,ARB_NAME,ISIN,REUTERS,CURRENCY,BOND_TYPE,LISTED_SHARES,PAID_SHARES,FACE_VALUE,COUPON_VALUE,COUPON_DATE,NET_PROFIT,NET_PROFIT_DATE,EPS,PE_RATIO,LAST_TRADE_DATE,DIVIDEND_YIELD_PERC,MARKET_CAP,OPEN_PRICE FROM STOCK.SYMBOLINFO WHERE ROWNUM = 1'
#sql='SELECT * FROM OHLCV'
#sql='SELECT SUM(VOLUME) FROM FILL_OHLCV GROUP BY BARTIMESTAMP'
#sql='SELECT T2.REUTERS FROM CASE30_COMPANIES T1 JOIN STOCK.SYMBOLINFO T2 ON T2.SYMBOL_CODE = T1.SYMBOL_CODE'
#sql='SELECT T2.REUTERS FROM EGX100_SYMBOLS T1 JOIN STOCK.SYMBOLINFO T2 ON T2.SYMBOL_CODE = T1.SYMBOL_CODE'
#sql='TRUNCATE TABLE C_FILL_OHLCV'
#sql="SELECT table_name, column_name, data_type, data_length FROM USER_TAB_COLUMNS WHERE table_name = 'CASEINDEX_R'"
#sql='SELECT T2.REUTERS FROM CASE30_COMPANIES T1 JOIN STOCK.SYMBOLINFO T2 ON T2.SYMBOL_CODE = T1.SYMBOL_CODE'
#sql='SELECT T2.REUTERS,T1.EXEC_TIME,T1.TRADE_PRICE ,T1.VOLUME_TRADED FROM STOCK.TRADES_D T1 JOIN STOCK.SYMBOLINFO T2 ON T2.SYMBOL_CODE = T1.SYMBOL_CODE WHERE T1.FLAG_D = 0'

#cursor3 = con2.cursor()
#cursor3.execute(sql)
#res = cursor3.fetchmany(numRows=1100)
#print(name,sql)
#indexSym = cursor3.fetchmany(numRows=110)
#print(indexSym)
#sql='SELECT MAX(BARTIMESTAMP) FROM FILL_OHLCV WHERE TICKER=:name'
#cursor2.execute(sql,['EGX70-old'])
#res = cursor2.fetchone()
#sql='SELECT * FROM FILL_OHLCV WHERE  TICKER =:1 and BARTIMESTAMP >= :2 ORDER BY BARTIMESTAMP DESC'
#code=['BTFH',datetime.datetime(2019,10,23,0,0)]
#cursor2 = con2.cursor()
#cursor2.execute(sql,code)
#res = cursor2.fetchmany(numRows=500000)

#res = cursor2.fetchmany(numRows=100000)



#res = cursor3.fetchmany(numRows=100000)
'''
sql='SELECT * FROM (SELECT * FROM FILL_OHLCV WHERE  TICKER =:1 and BARTIMESTAMP >= :2 ORDER BY BARTIMESTAMP DESC) WHERE ROWNUM <=1000'
cursor4 = con2.cursor()
cursor4.execute(sql,['EGTS',datetime.datetime(2019,8,18,0,0)])
res = cursor4.fetchmany(numRows=1000)
print(res)
data=pd.DataFrame(res,columns=['ticker','open','high','low','close','volume','date','asset','vwap'])
'''
'''
sql='SELECT T2.REUTERS,T1.EXEC_TIME,T1.TRADE_PRICE ,T1.VOLUME_TRADED FROM STOCK.TRADES T1 JOIN STOCK.SYMBOLINFO T2 ON T2.SYMBOL_CODE = T1.SYMBOL_CODE'



data=pd.DataFrame(res,columns=['code','time','price','volume'])
data=data.loc[data.code=='HELI.CA             ',:]

data['traders']=data.price*data.volume

vwap2=((data['price']*data['volume']).sum())/data['volume'].sum()
data['volume'].sum()
sumV=sum(data.volume)
sumT=sum(data.traders)
sumT/sumV


vwap=((data['price']*data['volume']).cumsum()/data['volume'].cumsum())
'''
'''
sql='update STOCK.FILL_OHLCV_1MIN set Ticker=:1 where Ticker = :2'
cursor4 = con2.cursor()
cursor4.execute(sql,['GTHE','ORTE'])
con2.commit()
'''


#sql='SELECT * FROM INVESTORS'
#df = pd.read_sql(sql, con2)
#df_replaced=df.replace(['???','?????','??????'],['Arab','Foreigners','Egyptians'],inplace=False)
#df_replaced.set_index('UPDATE_DATE',inplace=True)
#df_replaced.to_csv('EGIDTradingComposition.csv',index=True)
#cursor4 = con2.cursor()
#cursor4.execute(sql)
#
#res = cursor4.fetchmany(numRows=100000)


#sql='select TICKER,BARTIMESTAMP,OPEN,HIGH,LOW,CLOSE,VOLUME,VWAP,COUNT(*) FROM FILL_OHLCV GROUP BY TICKER,BARTIMESTAMP HAVING COUNT(*) > 1'
#sql='SELECT COUNT(*) FROM FILL_OHLCV_1MIN WHERE TICKER =:1 AND rowid not in (SELECT MIN(rowid) FROM FILL_OHLCV_1MIN WHERE TICKER=:1 GROUP BY TICKER,BARTIMESTAMP)'
#sql='DELETE FROM FILL_OHLCV_1MIN WHERE rowid not in (SELECT MIN(rowid) FROM FILL_OHLCV_1MIN GROUP BY TICKER,BARTIMESTAMP)'
#sql='SELECT COUNT(*) FROM FILL_OHLCV_1MIN WHERE TICKER=:1'
#sql='SELECT * FROM (SELECT TICKER, BARTIMESTAMP, OPEN, HIGH, LOW, CLOSE, VOLUME FROM FILL_OHLCV_1MIN WHERE TICKER =:1 ORDER BY BARTIMESTAMP DESC) WHERE ROWNUM <=10'
#sql='SELECT table_name FROM user_tables'
#cursor4 = con2.cursor()
#cursor4.execute(sql)#,['COMI']
#res = cursor4.fetchmany(numRows=100000)
#df2=pd.DataFrame(res,columns=['TICKER', 'BARTIMESTAMP', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME'])
#print(res)

#sql='SELECT * FROM (SELECT * FROM FILL_OHLCV WHERE  TICKER =:1 and BARTIMESTAMP < :2ORDER BY BARTIMESTAMP DESC) WHERE ROWNUM <=100'
#




#cursor4.execute(sql2,param)
#con2.commit()
#sql='SELECT * FROM (SELECT * FROM FILL_OHLCV_1MIN WHERE  TICKER =:1 and BARTIMESTAMP < :2 ORDER BY BARTIMESTAMP DESC) WHERE ROWNUM <=100'
#cursor4 = con2.cursor()
#cursor4.execute(sql,['MNHD',datetime.datetime(2019,6,10,0,0)])
#res = cursor4.fetchmany(numRows=100)
#print(res)
#cursor = con2.cursor()
#sql="SELECT * FROM (SELECT * FROM CASEINDEX_R WHERE INDEXCODE=:3 ORDER BY INDEXTIME DESC) WHERE ROWNUM <=100000"
#sql="SELECT * FROM CASEINDEX_R WHERE INDEXCODE=:3 AND INDEXTIME >:2"
#cursor.execute(sql,['EGX30       ',datetime.datetime(2019, 7, 31, 14, 59, 51)])
#res = cursor.fetchmany(numRows=10000000)
#print(res)
'''
sql='SELECT * FROM (SELECT * FROM FILL_OHLCV WHERE  TICKER =:1 and BARTIMESTAMP < :2 ORDER BY BARTIMESTAMP DESC) WHERE ROWNUM <=100'
cursor4 = con2.cursor()
cursor4.execute(sql,['MNHD',datetime.datetime(2019,6,10,0,0)])
res = cursor4.fetchmany(numRows=100)
print(res)
#df=pd.DataFrame(res,columns=['code','open','high','low','close','volume','time'])
'''
'''
sql='SELECT DISTINCT(INDEXCODE) from CASEINDEX'
cursor4 = con2.cursor()
cursor4.execute(sql)
res = cursor4.fetchmany(numRows=100000)

con2.commit()
'''
'''
df=pd.DataFrame(res,columns=['code','open','high','low','close','volume','time'])
df['vwap']=df['close']
df['asset']=1
df[df['code']=='EGXCAP','code']='EGX30 Capped'
indList=['EGX30CAP','EGX30','CASE30','EGX70','EGX50','EGX100','HRMSL']
for asset in indList:
    df[df['code']==asset,'asset']=1
'''
#sql='SELECT * FROM (SELECT *  FROM STOCK.FRX_OHLCV WHERE  TICKER =:1  ORDER BY BARTIMESTAMP DESC) WHERE ROWNUM <=100'
#cursor4.execute(sql,['EURUSD'])
#sql='SELECT * FROM FRX_OHLCV WHERE BARTIMESTAMP =:1'
#cursor4.execute(sql,[datetime.datetime(2019,10,1,0)])
#res = cursor4.fetchmany(numRows=2)
#

#print(res)
'''
res=[s[0].replace('.CA','').strip() for s in res]
print(res,type(res))
placeholders = [':%d' % i for i in range(len(res))]
#query = "SELECT * FROM some_table WHERE some_field IN (%s) G"
sql="SELECT SUM(VOLUME),BARTIMESTAMP FROM STOCK.FILL_OHLCV WHERE Ticker IN (%s)  GROUP BY BARTIMESTAMP ORDER BY BARTIMESTAMP DESC" % ','.join(placeholders)
print(sql)
cursor2.execute(sql,res)
res = cursor2.fetchmany(numRows=10)
print(res)
print(sum([pair[0] for pair in res]))
con2.close()'''
'''
sql='SELECT T2.REUTERS,T1.EXEC_TIME,T1.TRADE_PRICE ,T1.VOLUME_TRADED FROM STOCK.TRADES T1 JOIN STOCK.SYMBOLINFO T2 ON T2.SYMBOL_CODE = T1.SYMBOL_CODE'
cursor2 = con2.cursor()
cursor2.execute(sql)

res = cursor2.fetchmany(numRows=100000)

data=pd.DataFrame(res,columns=['code','time','price','volume'])
data=data.loc[data.code=='EGTS.CA             ',:]

data['traders']=data.price*data.volume
sumV=sum(data.volume)
sumT=sum(data.traders)
sumT/sumV
'''