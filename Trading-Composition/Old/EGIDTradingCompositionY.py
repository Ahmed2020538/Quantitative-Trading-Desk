# -*- coding: utf-8 -*-
"""
Created on Sun Sep 13 14:01:22 2020

@author: aya.adel
"""

import pandas as pd
import numpy as np
import cx_Oracle
import datetime
import csv 



#Database Connection
def dbConnect():
    con = cx_Oracle.connect('STOCK/P3rXdM5HbSgQRmCS@10.1.20.41:1521/STOCK')
    print (con.version)
    return con

con2=dbConnect()



#Reading trading composition data from eg-id table INVESTORS
#sql = 'SELECT * FROM INVESTORS'

sql = 'SELECT * FROM DAILY_FOR_RET_VIEW'
df = pd.read_sql(sql, con2)

#Retail
retailBuyers = np.array(df['BUY_RET'].iloc[-1])
retailBuyers = int(retailBuyers)
retailSellers = np.array(df['SELL_RET'].iloc[-1])
retailSellers = int(retailSellers)
retailNetFlow = retailBuyers - retailSellers
TotalRetail = (retailBuyers + retailSellers)/2


#Institutional
institutionalBuyers = np.array(df['BUY_COM'].iloc[-1])
institutionalBuyers = int(institutionalBuyers)
institutionalSellers = np.array(df['SELL_COM'].iloc[-1])
institutionalSellers = int(institutionalSellers)
institutionalNetFlow = institutionalBuyers - institutionalSellers
TotalInstitutional = (institutionalBuyers + institutionalSellers)/2

Total_Turnover = TotalRetail + TotalInstitutional

#Foreigners
foreignerBuyers = np.array(df['BUY_OTHER'].iloc[-1])
foreignerBuyers = int(foreignerBuyers)
foreignerSellers = np.array(df['SELL_OTHER'].iloc[-1])
foreignerSellers = int(foreignerSellers)
foreignerNetFlow = foreignerBuyers - foreignerSellers
TotalForeigner = (foreignerBuyers + foreignerSellers)/2

#Regional
regionalBuyers = np.array(df['BUY_ARB'].iloc[-1])
regionalBuyers = int(regionalBuyers)
regionalSellers = np.array(df['SELL_ARB'].iloc[-1])
regionalSellers = int(regionalSellers)
regionalNetFlow = regionalBuyers - regionalSellers
TotalRegional = (regionalBuyers + regionalSellers)/2

#Local
localBuyers = np.array(df['BUY_EG'].iloc[-1])
localBuyers = int(localBuyers)
localSellers = np.array(df['SELL_EG'].iloc[-1])
localSellers = int(localSellers)
localNetFlow = localBuyers - localSellers
TotalLocal = (localBuyers + localSellers)/2

all_Array = []
buy_Array = []
sell_Array = []
total_Array = []
netFlow_Array = []

today_date = datetime.datetime.today().strftime('%Y-%m-%d')

all_Array.append(today_date)
all_Array.append(Total_Turnover)
all_Array.append(TotalRetail)
all_Array.append(TotalInstitutional)
all_Array.append(TotalForeigner)
all_Array.append(TotalRegional)
all_Array.append(TotalLocal)
all_Array.append(retailBuyers)
all_Array.append(institutionalBuyers)
all_Array.append(foreignerBuyers)
all_Array.append(regionalBuyers)
all_Array.append(localBuyers)
all_Array.append(retailSellers)
all_Array.append(institutionalSellers)
all_Array.append(foreignerSellers)
all_Array.append(regionalSellers)
all_Array.append(localSellers)
all_Array.append(retailNetFlow)
all_Array.append(institutionalNetFlow)
all_Array.append(foreignerNetFlow)
all_Array.append(regionalNetFlow)
all_Array.append(localNetFlow)



buy_Array.append('BUY')
buy_Array.append(today_date)
buy_Array.append(retailBuyers)
buy_Array.append(institutionalBuyers)
buy_Array.append(foreignerBuyers)
buy_Array.append(regionalBuyers)
buy_Array.append(localBuyers)

sell_Array.append('SELL')
sell_Array.append(today_date)
sell_Array.append(retailSellers)
sell_Array.append(institutionalSellers)
sell_Array.append(foreignerSellers)
sell_Array.append(regionalSellers)
sell_Array.append(localSellers)

total_Array.append('TOTAL')
total_Array.append(today_date)
total_Array.append(Total_Turnover)
total_Array.append(TotalRetail)
total_Array.append(TotalInstitutional)
total_Array.append(TotalForeigner)
total_Array.append(TotalRegional)
total_Array.append(TotalLocal)


netFlow_Array.append('NETFLOW')
netFlow_Array.append(today_date)
netFlow_Array.append(retailNetFlow)
netFlow_Array.append(institutionalNetFlow)
netFlow_Array.append(foreignerNetFlow)
netFlow_Array.append(regionalNetFlow)
netFlow_Array.append(localNetFlow)

print(total_Array)


path = 'Y:/Asset Management-Data science/EGIDTradingComposition/'
path1 = 'C:/Users/Aya.Adel/MyFiles/TradingComposition/'

#appending the result to trading composition  csv file
#with open(path + 'allTradingCompositionEGX.csv', mode='a') as all_csv:
#    all_writer = csv.writer(all_csv)
#    all_writer.writerow(all_Array)

with open(path + 'BuyTradingCompositionEGX.csv', mode='a') as buy_csv:
    buy_writer = csv.writer(buy_csv)
    buy_writer.writerow(buy_Array)

with open(path + 'SellTradingCompositionEGX.csv', mode='a') as sell_csv:
    sell_writer = csv.writer(sell_csv)
    sell_writer.writerow(sell_Array)

with open(path + 'TotalTradingCompositionEGX.csv', mode='a') as total_csv:
    total_writer = csv.writer(total_csv)
    total_writer.writerow(total_Array)
    

with open(path + 'NetFlowTradingCompositionEGX.csv', mode='a') as netFlow_csv:
    netFlow_writer = csv.writer(netFlow_csv)
    netFlow_writer.writerow(netFlow_Array)


