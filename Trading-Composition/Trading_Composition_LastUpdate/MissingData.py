# -*- coding: utf-8 -*-
"""
Created on Tue Sep 15 12:10:13 2020

@author: aya.adel
"""

import pandas as pd
import numpy as np
from datetime import datetime
import csv 



path = 'C:/Users/Aya.Adel/MyFiles/TradingComposition/'

df_2019 = pd.read_excel(path+'YTD 2019 Trading Composition.xlsx',header=1)
df_2020 = pd.read_excel(path+'YTD 2020 Trading Composition-3.xlsx',header=1)


all_columns = ['Date','Total Turnover','Retail','Institutional','Foreign','Regional','Local','Retail.1','Institutional.1','Foreign.1','Regional.1','Local.1','Retail.2','Institutional.2','Foreign.2','Regional.2','Local.2','Retail.3','Institutional.3','Foreign.3','Regional.3','Local.3']
total_columns = ['Date','Total Turnover','Retail','Institutional','Foreign','Regional','Local']
buy_columns = ['Date','Retail.1','Institutional.1','Foreign.1','Regional.1','Local.1']
sell_columns = ['Date','Retail.2','Institutional.2','Foreign.2','Regional.2','Local.2']
netFlow_columns = ['Date','Retail.3','Institutional.3','Foreign.3','Regional.3','Local.3']


#2019 missing data
all_2019 = df_2019.loc[76:237 , all_columns]
total_2019= df_2019.loc[76:237 , total_columns]
buy_2019 = df_2019.loc[76:237 , buy_columns]
sell_2019 = df_2019.loc[76:237 , sell_columns]
netFlow_2019 = df_2019.loc[76:237 , netFlow_columns]


#2020 missing data
all_2020 = df_2020.loc[0:165 , all_columns]
total_2020= df_2020.loc[0:165 , total_columns]
buy_2020 = df_2020.loc[0:165 , buy_columns]
sell_2020 = df_2020.loc[0:165 , sell_columns]
netFlow_2020 = df_2020.loc[0:165 , netFlow_columns]


#Appending missing data to the csv files
all_2019.to_csv(path+ 'allTradingCompositionEGX.csv', mode='a',header=False,index=False)
total_2019.to_csv(path+ 'TotalTradingCompositionEGX.csv', mode='a',header=False,index=False)
buy_2019.to_csv(path+ 'BuyTradingCompositionEGX.csv', mode='a',header=False,index=False)
sell_2019.to_csv(path+ 'SellTradingCompositionEGX.csv', mode='a',header=False,index=False)
netFlow_2019.to_csv(path+ 'NetFlowTradingCompositionEGX.csv', mode='a',header=False,index=False)

all_2020.to_csv(path+ 'allTradingCompositionEGX.csv', mode='a',header=False,index=False)
total_2020.to_csv(path+ 'TotalTradingCompositionEGX.csv', mode='a',header=False,index=False)
buy_2020.to_csv(path+ 'BuyTradingCompositionEGX.csv', mode='a',header=False,index=False)
sell_2020.to_csv(path+ 'SellTradingCompositionEGX.csv', mode='a',header=False,index=False)
netFlow_2020.to_csv(path+ 'NetFlowTradingCompositionEGX.csv', mode='a',header=False,index=False)


 