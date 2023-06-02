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
import sys

'''
1- fetch the timestamp of the last row that was inserted into fill_ohlcv table
2- fetch all the prices of the ew index constituents that occured after the timestamp 
of the last row of the previous step
3- construct a df that have the prices of all the index constituents for all the possible dates
4- multiply those prices by the weight of the index then group by date and sum
5- insert the result into the the fill_ohlcv table

'''



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

def IndexConstituentsSymbols(sqlQuery,db_connection):
    '''parameters:
        sqlQuery: string
            the sql query (select statement) that fetches the index constituents symbols from the database table
        
        return: list of strings
            index_symbols: a list of index constituents symbols as in reuters code 
    '''
    cursor1 = db_connection.cursor()
    cursor1.execute(sqlQuery)
    index_constituents = cursor1.fetchall()
    index_constituents = [i.strip() for x in index_constituents for i in x ]
    
    return index_constituents


def LiveUpdateEWILAST( db_con, df_all_prices, index_symbols, last_bar_timestamp,price_columns):
    '''
    How it works:
    *Calculate EWI Based on today's prices only:
        ps: treat missing data (prices only) with ffill (to fill the upcoming not existing days with 
        the last previous price and fill volumes na with zero (index constituents didn't execute any transactions))
        
        How to create equally weighted index:
            1-we gather the prices (OHLC) of index constituents(make sure there is a price
            of each index constituent on each 5 min bar)
            2-multiply all the prices by equal weights (1/number of index constituents)
            3-sum up the prices of the index constituents for each day (grouped by dates)
            
    
    
    *how to have dataframe containing data for every five minutes even if the symbols didn't execute
        (take the prvious price ffil)
        1-we create an empty data frame that has multiindex( timestamp and ticker) multiindex.from product
        2-pull the data of the index constituents from the database (timestamp and ticker index)
        3-join the 2 dataframes matching the index together
        4-the previous step will result in a huge dataframe containing all the data covering all dates
        5-if the symbol didn't execute at the corresponding date you'll find a null at this date
        6-use ffill method to fill in the null values with the price of the previous bar
    
    parameters: 
    -----------
        db_con: cx_oracle connection
            The database connection
        df_all_prices: dataframe
            empty dataframe to store index constitutes prices (OHLCV and vwap)
        index_symbols : list of strings
            list of index constituents names
        last_bar_timestamp: datetime object
            last bar timestamp existing in the database of the EW index that we'll start updating
        the index data after.
        price_columns: list of strings
            labels of df columns that contain the prices
    ----------
      returns: dataframe
          df_EWI: a dataframe that contains the prices of the EW index that we created
    '''
    
    df_EWI = pd.DataFrame()
    df_fillna = pd.DataFrame()
    cursor1 = db_con.cursor()
    
    for symbol in index_symbols:
#        latest prices to fill null values with 
        prev_prices_query = "SELECT bartimestamp,ticker,open,high,low,close,vwap FROM STOCK.FILL_OHLCV WHERE ticker =:1 AND bartimestamp <=:2 ORDER BY bartimestamp DESC"
        cursor1.execute(prev_prices_query,[str(symbol),last_bar_timestamp])
        prev_prices = (pd.DataFrame(cursor1.fetchone(), ['BARTIMESTAMP','TICKER','OPEN','HIGH','LOW','CLOSE','VWAP'])).T
        df_fillna = df_fillna.append(prev_prices)
        
#        latest prices to calculate ewi 
        select_query = """SELECT bartimestamp,ticker,open,high,low,close,vwap,volume FROM STOCK.FILL_OHLCV WHERE ticker = :1 AND bartimestamp >= :2 ORDER BY bartimestamp DESC"""
        param = [str(symbol),last_bar_timestamp]
        cursor1.execute(select_query, param)
        df_symbol = pd.read_sql(select_query, db_con,index_col = 'BARTIMESTAMP',  params=param)
        df_all_prices = df_all_prices.append(df_symbol)
        df_all_prices.sort_index(ascending = True, inplace=True)

    df_fillna.set_index('TICKER',inplace = True)   
    
    if(not df_all_prices.empty):
        
        df_all_dates = pd.DataFrame(index = pd.MultiIndex.from_product([df_all_prices.index.unique(), index_symbols]))
        df_all_dates.index.names = ['BARTIMESTAMP','TICKER']
        
        df_all_prices = df_all_prices.set_index(pd.MultiIndex.from_arrays([df_all_prices.index,df_all_prices['TICKER']],names = ['BARTIMESTAMP', 'TICKER']))
        del df_all_prices['TICKER']

        df_total = pd.merge(df_all_dates, df_all_prices, how = 'outer', left_on = ['BARTIMESTAMP','TICKER'], right_on = ['BARTIMESTAMP','TICKER'])
        df_total.sort_index(ascending = True, inplace=True)
        df_total[price_columns] = df_total[price_columns].groupby(level = 'TICKER').transform(lambda x: x.fillna(method ='ffill', axis = 0))
        df_total[price_columns] = df_total[price_columns].groupby(level = 'TICKER').transform(lambda x: x.fillna(method ='bfill', axis = 0))
        df_total[price_columns] = df_total[price_columns].fillna(df_fillna)
        df_total['VOLUME'] = df_total['VOLUME'].fillna(0)
        df_total.reset_index(level = 'TICKER',drop = False, inplace = True)

        scale = 1
        df_EWI = (1/len(index_symbols))* df_total[price_columns]
        df_EWI = (df_EWI.groupby('BARTIMESTAMP').sum()) * scale
        df_EWI['VOLUME'] = df_total['VOLUME'].groupby('BARTIMESTAMP').sum()
        
    return df_EWI



con2=dbConnect()
cursor1 = con2.cursor()


#sql quries to fetch the index constituents codes
sql30 = "SELECT REPLACE(T2.REUTERS,'.CA','') FROM CASE30_COMPANIES T1 JOIN STOCK.SYMBOLINFO T2 ON T2.SYMBOL_CODE = T1.SYMBOL_CODE ORDER BY T2.REUTERS"
sql50 = "SELECT REPLACE(T2.REUTERS,'.CA','') FROM EGX50_SYMBOLS T1 JOIN STOCK.SYMBOLINFO T2 ON T2.SYMBOL_CODE = T1.SYMBOL_CODE ORDER BY T2.REUTERS"
sql70 = "SELECT REPLACE(T2.REUTERS,'.CA','') FROM EGX70_SYMBOLS T1 JOIN STOCK.SYMBOLINFO T2 ON T2.SYMBOL_CODE = T1.SYMBOL_CODE ORDER BY T2.REUTERS"
sql100 = "SELECT REPLACE(T2.REUTERS,'.CA','') FROM EGX100_SYMBOLS T1 JOIN STOCK.SYMBOLINFO T2 ON T2.SYMBOL_CODE = T1.SYMBOL_CODE ORDER BY T2.REUTERS"
sql_sectors = "SELECT DISTINCT(REPLACE(INDEXCODE,' ','')) FROM CASE_SECTOR_HIST"


#indices constituents symbols lists 
egx30_symbols = IndexConstituentsSymbols(sql30,con2)
egx50_symbols = IndexConstituentsSymbols(sql50,con2)
egx70_symbols = IndexConstituentsSymbols(sql70,con2)
egx100_symbols = IndexConstituentsSymbols(sql100,con2)
sector_indices = IndexConstituentsSymbols(sql_sectors,con2)
all_indices_symbols = [egx30_symbols,egx50_symbols,egx70_symbols,egx100_symbols]

table_columns=['TICKER','OPEN','HIGH','LOW','CLOSE','VOLUME','BARTIMESTAMP','ASSET','VWAP']
price_columns=['OPEN','HIGH','LOW','CLOSE','VWAP']

#the EWI names
ewi_names = ['EGX30LASTEWI', 'EGX50LASTEWI' , 'EGX70LASTEWI', 'EGX100LASTEWI']


while(1):
    
    con2=dbConnect()
    cursor1 = con2.cursor()
#   fetching the volume and timestamp of the last bar that belongs to the EW indices to start processing and updating after that
    for i in range(len(ewi_names)):
        last_bar_query = 'SELECT BARTIMESTAMP,VOLUME,OPEN,HIGH,LOW,CLOSE FROM STOCK.FILL_OHLCV WHERE TICKER = :1 ORDER BY BARTIMESTAMP DESC'
        param_last_bar_query = str(ewi_names[i])
        cursor1.execute(last_bar_query,[param_last_bar_query])
        last_bar = cursor1.fetchone()
#       last_bar[0] --> timestamp
#        last_bar[1] --> volume
        
#        empty df to store the index constituents prices in
        all_prices = pd.DataFrame()
        df_EWILast = LiveUpdateEWILAST( con2, all_prices, all_indices_symbols[i], last_bar[0],price_columns)

        if(not df_EWILast.empty):
#            new record of EWI to be inserted
            tbIns = df_EWILast.loc[df_EWILast.index.to_pydatetime()>last_bar[0]]
#            existing record of EWI that needs to be updated due to price change
            tbUpd = df_EWILast.loc[df_EWILast.index.to_pydatetime()==last_bar[0]]
        else:
            tbIns = df_EWILast.copy
            tbUpd = []
            
        if(len(tbUpd) > 0 and tbUpd['VOLUME'][0] != last_bar[1]):
#            update records
            update_query='update STOCK.FILL_OHLCV set OPEN=:1,HIGH=:2,LOW=:3,CLOSE=:4,VWAP=:5, VOLUME=:6 where Ticker = :7 AND BARTIMESTAMP=:8'
            cursor1.execute(update_query,[tbUpd['OPEN'].values[0],tbUpd['HIGH'].values[0],tbUpd['LOW'].values[0],tbUpd['CLOSE'].values[0],tbUpd['VWAP'].values[0],tbUpd['VOLUME'].values[0].astype(float),ewi_names[i],tbUpd.index.to_pydatetime()[0]])
            print(ewi_names[i],'is being updated')
        else:
            print('...')
        if(len(tbIns) > 0):
#            insert into database if there are any new records
            for index,row in tbIns.iterrows():
                try:
                    line=[0,1,2,3,4,5,6,7,8]
                    line[0]=ewi_names[i]
                    line[1]=row['OPEN']
                    line[2]=row['HIGH']
                    line[3]=row['LOW']
                    line[4]=row['CLOSE']
                    line[5]=row['VOLUME']
                    line[6]=index.to_pydatetime()
                    line[7]=0
                    line[8]=row['VWAP']
                    cursor1.execute("insert into FILL_OHLCV(TICKER,OPEN,HIGH,LOW,CLOSE,VOLUME,BARTIMESTAMP,ASSET,VWAP) values (:stock, :open,:high,:low,:close,:vol,:time,:1,:2)",line)
                    con2.commit()
                    print(line)
                except Exception as e:
                    print(str(e))
        else:
            print("...")
    con2.close()
    time.sleep(60)
            