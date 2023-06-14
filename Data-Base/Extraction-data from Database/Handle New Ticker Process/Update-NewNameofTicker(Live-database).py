# Load reqiured package :
import pandas as pd
from datetime import datetime as dt
import cx_Oracle
import datetime
import csv
import time
import sys
import os 
#----------------------------------------------------------------------
#----------------------------------------------------------------------
# Access on Orcale DataBase :
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
#----------------------------------------------------------------------
#----------------------------------------------------------------------
HistTicker  = input("Enter Historical Name of Ticker :: ")
NewTicker   = input("Enter New Name of Ticker :: ")
foldataname = input("Enter The Folder Name to store data  :: ")
MergeCSVFileNAme = input("Enter The Csv File Name to store data  :: ")
print("Please double check Names of that Tickers again ::")
print("=================================Set input param first time done done=================================")
print("==================================================================================")
Confirmation  = input("Enter YES if u Confirmed or NO to reset inputs again:: ")
if(Confirmation == "YES"):
    print("=================================second Confirmation without resting case done=================================")
    print("==================================================================================")
    pass
else :
    print("Please reset that inputs again")
    HistTicker  = input("Enter Historical Name of Ticker :: ")
    NewTicker   = input("Enter New Name of Ticker :: ")
    foldataname = input("Enter The Folder Name to store data  :: ")
    MergeCSVFileNAme = input("Enter The Csv File Name to store data  :: ")
    print("=================================second Confirmation and reset inputs case done=================================")
    print("==================================================================================")
    
#----------------------------------------------------------------------
#----------------------------------------------------------------------
# Read all data of FILL_OHLCV Table
con = cx_Oracle.connect('STOCK/P3rXdM5HbSgQRmCS@10.1.20.41:1521/STOCK')
sql='SELECT * FROM FILL_OHLCV' 
cursor = con.cursor()   
cursor.execute(sql)
#con.commit()
FILL_OHLCV_data = pd.read_sql(sql, con)
FILL_OHLCV_data.to_csv("FILL_OHLCV_data.csv")
FILL_OHLCV_dataFile = pd.read_csv("FILL_OHLCV_data.csv")
FILL_OHLCV_dataFile
#----------------------------------------------------------------------
#----------------------------------------------------------------------

FILL_OHLCV_dataFile[FILL_OHLCV_dataFile["TICKER"] == HistTicker].to_csv(f"{HistTicker}HistTickerdata.csv")
FILL_OHLCV_dataFile[FILL_OHLCV_dataFile["TICKER"] == NewTicker].to_csv(f"{NewTicker}NewTickerdata.csv")
print("=================================Save data Tickers on csv files done=================================")
print("==================================================================================")
HistTickerdf = pd.read_csv(f"{HistTicker}HistTickerdata.csv")
NewTickerdf = pd.read_csv(f"{NewTicker}NewTickerdata.csv")
HistTickersdfcols = HistTickerdf.columns
NewTickerdfcols = NewTickerdf.columns
print(f"Columns of Hist Ticker ({HistTicker})\n=========\n{HistTickersdfcols}==========================================" )
print(f"Columns of New Ticker ({NewTicker})\n============\n{NewTickerdfcols}===========================================" )
print(f"Columns of Hist Ticker df of  ({HistTicker})\n============\n{NewTickerdf}===========================================" )
print(f"Columns of New  Ticker df of  ({NewTicker} )\n============\n{NewTickerdf}===========================================" )
HistTickerdf["TICKER"] = HistTickerdf["TICKER"].apply(lambda x : f"{NewTicker}" if x == f"{HistTicker}" else f"{NewTicker}")
print(HistTickerdf)
print(f"=====================Renamed Hist Ticker({HistTicker}) To Ticker({NewTicker})========================")
print("==================================================================================")
os.mkdir(f"{foldataname}")
print(f"====================={foldataname} Folder Created=========================")
print("==================================================================================")
for Histcol in HistTickersdfcols :
    if(Histcol== 'TICKER' or Histcol== 'OPEN' or Histcol== 'HIGH' or Histcol== 'LOW' or Histcol== 'CLOSE' or Histcol== 'VOLUME' or Histcol== 'BARTIMESTAMP' or Histcol== 'ASSET' or Histcol== 'VWAP') :
        print(f"We didn't delet that column :: {Histcol}")
        pass
    else :
        print(f"We deleted that column :: {Histcol}")
        HistTickerdf = HistTickerdf.drop([Histcol], axis=1)
HistTickerdf.to_csv(f"{foldataname}/0({HistTicker})HistTickerdata.csv" , index=False)
print("Handle Historical Ticker data file  Done")
print("=============================================================")
for Newcol in NewTickerdfcols :
    if(Newcol== 'TICKER' or Newcol=='OPEN' or Newcol=='HIGH' or Newcol=='LOW' or Newcol=='CLOSE' or Newcol=='VOLUME' or Newcol=='BARTIMESTAMP' or Newcol=='ASSET' or Newcol=='VWAP') :
        print(f"We didn't delet that columns :: {Newcol}")
        pass
    else :
        print(f"We deleted that columns :: {Newcol}")
        NewTickerdf = NewTickerdf.drop([Newcol], axis=1)
NewTickerdf.to_csv(f"{foldataname}/1({NewTicker})NewTickerdata.csv" , index=False)
print("Handle New Ticker data file  Done")
print("=============================================================")
print("=============================================================")
print("=============================================================")
LastHandleHistTickerdf=pd.read_csv(f"{foldataname}/0({HistTicker})HistTickerdata.csv" )
print(LastHandleHistTickerdf)
print("=============================================================")
print("=============================================================")
LastHandleNewTickerdf=pd.read_csv(f"{foldataname}/1({NewTicker})NewTickerdata.csv" )
print(LastHandleNewTickerdf)
print("=============================================================")
print("=============================================================")
path = os.path.abspath('') 
path = path+"\\"+foldataname
print(f"The Path Dirction :: {path}\n----------------\n\nThe Content Path\n")
print("=======================Path found done===============================")
print("==========================================================================")
#----------------------------------------------------------------------
#----------------------------------------------------------------------
df = pd.DataFrame()
files = os.listdir(path)
for file in files:
    if file.endswith('.csv'):
        print(file)
        df = df.append(pd.read_csv(f"{path}\\{file}"), ignore_index=True) 
df.head() 
df.to_csv(f'{path}\\{MergeCSVFileNAme}.csv' , index = False)
print("=======================Read 2 CSV Filies and Merged done============================")
print("==========================================================================")
tbIns = pd.read_csv(f'{path}\\{MergeCSVFileNAme}.csv' )
# casting DataTime Tybe
tbIns['BARTIMESTAMP'] = pd.to_datetime(tbIns['BARTIMESTAMP'],  errors='coerce')
# Set DateTime as Index of DataFrame
tbIns.set_index("BARTIMESTAMP" , inplace=True)
tbIns.sort_index(ascending = True, inplace=True)
print("======================Sorting Data of File done============================")
print("==========================================================================")
tbIns
#----------------------------------------------------------------------
#----------------------------------------------------------------------
Reconfirmation = input("""Please double check all process before insert any records  before updates
                           If u are confirmed all processes set Yes if didn't confirm set NO:: """)
if(Reconfirmation == "YES") :
    print("Last Confirmation done thanks allots Ahmad Elsayed Ibrahim")
    con=dbConnect()
    cur = con.cursor()
    lines=[]
    for index,row in tbIns.iterrows():
        try:
            line=[0,1,2,3,4,5,6,7,8]
            line[0]=row["TICKER"]
            #line[0]="ABG"
            line[1]=row['OPEN']
            line[2]=row['HIGH']
            line[3]=row['LOW']
            line[4]=row['CLOSE']
            line[5]=row['VOLUME']
            line[6]=index.to_pydatetime()
            line[7]=1
            line[8]=row['VWAP']
            #print(index.to_pydatetime())
            lines.append(line)

            #print(lines)
            print(line)
            cur.execute("insert into FILL_OHLCV(TICKER,OPEN,HIGH,LOW,CLOSE,VOLUME,BARTIMESTAMP,ASSET,VWAP) values (:TICKER, :OPEN,:HIGH,:LOW,:CLOSE,:VOLUME,:BARTIMESTAMP,:1,:2)",line)
            con.commit()
        except Exception as e:

            print(str(e))
            print(line)
else :
    print("Please double check process before any updates")
    break
    
