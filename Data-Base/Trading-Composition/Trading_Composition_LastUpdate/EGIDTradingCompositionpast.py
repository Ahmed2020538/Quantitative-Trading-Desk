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




buy_Array = []
sell_Array = []
total_Array = []
netFlow_Array = []

today_date = datetime.datetime.today().strftime('%Y-%m-%d')

buy_Array.append('BUY')
buy_Array.append(today_date)
buy_Array.append(retailBuyers)
buy_Array.append(institutionalBuyers)
buy_Array.append(foreignerBuyers)
buy_Array.append(regionalBuyers)
buy_Array.append(localBuyers)

df_buy = pd.DataFrame([buy_Array],columns = ["BUY","Date","Retail","Institutional","Foreign","Regional","Local"])


sell_Array.append('SELL')
sell_Array.append(today_date)
sell_Array.append(retailSellers)
sell_Array.append(institutionalSellers)
sell_Array.append(foreignerSellers)
sell_Array.append(regionalSellers)
sell_Array.append(localSellers)

df_sell = pd.DataFrame([sell_Array],columns = ["SELL","Date","Retail","Institutional","Foreign","Regional","Local"])



total_Array.append('TOTAL')
total_Array.append(today_date)
total_Array.append(Total_Turnover)
total_Array.append(TotalRetail)
total_Array.append(TotalInstitutional)
total_Array.append(TotalForeigner)
total_Array.append(TotalRegional)
total_Array.append(TotalLocal)

df_total = pd.DataFrame([total_Array],columns = ["TOTAL","Date","Retail","Institutional","Foreign","Regional","Local","Total TurnOver"])


netFlow_Array.append('NETFLOW')
netFlow_Array.append(today_date)
netFlow_Array.append(retailNetFlow)
netFlow_Array.append(institutionalNetFlow)
netFlow_Array.append(foreignerNetFlow)
netFlow_Array.append(regionalNetFlow)
netFlow_Array.append(localNetFlow)

df_netflow = pd.DataFrame([netFlow_Array],columns = ["NWTFLOW","Date","Retail","Institutional","Foreign","Regional","Local"])


print(total_Array)


y_Path ="Y:/Asset Management-Data science/PharosQuantitativeTeam/TradesComposition/Trading_Composition_LastUpdate/"
#path = 'Y:/Asset Management-Data science/EGIDTradingComposition/'
ahmad_path = "E:/Repos/WORK/Daily_Tasks/Trading_Compossions/TradesComposition/"
new_path   ="Y:/Asset Management-Data science/Asset Management-Data science/Quantitative Trading Repo/Data-Filies/Trading-Composition-data/"
GithubRepo_path   ="Y:/Asset Management-Data science/PharosQuantitativeTeam/Quantitative-Trading-Desk/Data-Base/Trading-Composition/Trading_Composition_LastUpdate/"
#appending the result to trading composition  csv file
paths = [y_Path , ahmad_path, GithubRepo_path]
for path in paths :
	df_buy.to_csv(path+ 'BuyTradingCompositionEGX.csv', mode='a',header=False,index=False)
	df_sell.to_csv(path+ 'SellTradingCompositionEGX.csv', mode='a',header=False,index=False)
	df_total.to_csv(path+ 'TotalTradingCompositionEGX.csv', mode='a',header=False,index=False)
	df_netflow.to_csv(path+ 'NetFlowTradingCompositionEGX.csv', mode='a',header=False,index=False)
	print(path)
	print("=====================")

print("The Script run well and apenned all columns over all paths,  Thanks")
