#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Aug 15 12:32:14 2018

@author: Mennat Allah Tarek
"""
import pandas as pd
import datetime
import numpy as np
from selenium import webdriver
import time


def readData(path,skiprows,start_date,end_date):
   #parse_date = lambda x: x.strftime('%Y/%m/%d')
   xls = pd.ExcelFile(path)
   stocks={}
   for sh in xls.sheet_names:
       if(sh!='Sheet'):
           stocks[sh]=pd.read_excel(xls,sh, index_col=0,skiprows=skiprows)
           #print(stocks[sh]['Timestamp'])
           #stocks[sh]=stocks[sh].iloc[::-1]
           stocks[sh]=stocks[sh].reset_index(drop=True)
           stocks[sh]=stocks[sh].fillna(method='ffill')
           stocks[sh]=stocks[sh].fillna(method='bfill')  
           if(start_date!=0 and end_date!=0):
               stocks[sh]=stocks[sh][(stocks[sh]['Timestamp'] >= start_date) & (stocks[sh]['Timestamp'] <= end_date) ]
               stocks[sh]=stocks[sh].reset_index(drop=True)
   return stocks



options = webdriver.ChromeOptions()
options.add_argument("--start-maximized")
options.add_argument('--ignore-certificate-errors')
options.add_argument("--test-type")
options.binary_location = "/usr/bin/chromium"
driver = webdriver.Chrome() #"/usr/lib/chromium-browser/chromedriver"
driver.maximize_window()
driver.get('http://egx.com.eg/english/InvestorsTypePieChart.aspx')

#driver.switch_to.frame('select')
Radio = driver.find_element_by_id("ctl00_C_rblSecuritiesBonds_1")

Radio.click()

EgyptionTotalSellers = driver.find_element_by_xpath('//span[@id="ctl00_C_Pc_GridView1_ctl02_lblSell"]').text
#print(EgyptionTotalSellers)
EgyptionTotalSellers = EgyptionTotalSellers.replace(",","")
EgyptionTotalSellers = int(EgyptionTotalSellers)
#print(EgyptionTotalSellers)

#EgyptionTotalBuyers
##self.log('I just Visited:' + response.url)
#scrapy.FormRequest.from_response(response,clickdata={"name":"ctl00$C$rblSecuritiesBonds", 'checked' :'Accept'})

EgyptionTotalBuyers = driver.find_element_by_xpath('//span[@id="ctl00_C_Pc_GridView1_ctl02_lblBuy"]').text
EgyptionTotalBuyers = EgyptionTotalBuyers.replace(",","")
EgyptionTotalBuyers = int(EgyptionTotalBuyers)
#print(EgyptionTotalBuyers)


#EgyptionTotalNet
#self.log('I just Visited:' + response.url)
EgyptionTotalNet = driver.find_element_by_xpath('//span[@id="ctl00_C_Pc_GridView1_ctl02_lblNet"]').text
EgyptionTotalNet = EgyptionTotalNet.replace(",","")
EgyptionTotalNet = int(EgyptionTotalNet)
#print(EgyptionTotalNet)




#ArabTotalSellers
#self.log('I just Visited:' + response.url)
ArabTotalSellers = driver.find_element_by_xpath('//span[@id="ctl00_C_Pc_GridView1_ctl03_lblSell"]').text
ArabTotalSellers = ArabTotalSellers.replace(",","")
ArabTotalSellers = int(ArabTotalSellers)
#print(ArabTotalSellers)


#ArabTotalBuyers
#self.log('I just Visited:' + response.url)
ArabTotalBuyers = driver.find_element_by_xpath('//span[@id="ctl00_C_Pc_GridView1_ctl03_lblBuy"]').text
ArabTotalBuyers = ArabTotalBuyers.replace(",","")
ArabTotalBuyers = int(ArabTotalBuyers)
#print(ArabTotalBuyers)


#ArabTotalNet
#self.log('I just Visited:' + response.url)
ArabTotalNet = driver.find_element_by_xpath('//span[@id="ctl00_C_Pc_GridView1_ctl03_lblNet"]').text
ArabTotalNet = ArabTotalNet.replace(",","")
ArabTotalNet = int(ArabTotalNet)
#print(ArabTotalNet)



#ForignerTotalSeller
#self.log('I just Visited:' + response.url)
ForignerTotalSeller = driver.find_element_by_xpath('//span[@id="ctl00_C_Pc_GridView1_ctl04_lblSell"]').text
ForignerTotalSeller = ForignerTotalSeller.replace(",","")
ForignerTotalSeller = int(ForignerTotalSeller)
#print(ForignerTotalSeller)


#ForignerTotalBuyer
#self.log('I just Visited:' + response.url)
ForignerTotalBuyer = driver.find_element_by_xpath('//span[@id="ctl00_C_Pc_GridView1_ctl04_lblBuy"]').text
ForignerTotalBuyer = ForignerTotalBuyer.replace(",","")
ForignerTotalBuyer = int(ForignerTotalBuyer)
#print(ForignerTotalBuyer)


#ForignerTotalNet
#self.log('I just Visited:' + response.url)
ForignerTotalNet = driver.find_element_by_xpath('//span[@id="ctl00_C_Pc_GridView1_ctl04_lblNet"]').text
ForignerTotalNet = ForignerTotalNet.replace(",","")
ForignerTotalNet = int(ForignerTotalNet)
#print(ForignerTotalNet)







''' Retail '''

#EgyptionRetailSellers
#self.log('I just Visited:' + response.url)
EgyptionRetailSellers = driver.find_element_by_xpath('//span[@id="ctl00_C_Pc_gvIndByNationality_ctl02_lblSell1"]').text
EgyptionRetailSellers = EgyptionRetailSellers.replace(",","")
EgyptionRetailSellers = int(EgyptionRetailSellers)
#print(EgyptionRetailSellers)


#EgyptionRetailBuyers
#self.log('I just Visited:' + response.url)
EgyptionRetailBuyers = driver.find_element_by_xpath('//span[@id="ctl00_C_Pc_gvIndByNationality_ctl02_lblBuy1"]').text
EgyptionRetailBuyers = EgyptionRetailBuyers.replace(",","")
EgyptionRetailBuyers = int(EgyptionRetailBuyers)
#print(EgyptionRetailBuyers)


#EgyptionRetailNet
#self.log('I just Visited:' + response.url)
EgyptionRetailNet = driver.find_element_by_xpath('//span[@id="ctl00_C_Pc_gvIndByNationality_ctl02_lblNet1"]').text
EgyptionRetailNet = EgyptionRetailNet.replace(",","")
EgyptionRetailNet = int(EgyptionRetailNet)
#print(EgyptionRetailNet)


#ArabRetailSeller
#self.log('I just Visited:' + response.url)
ArabRetailSeller = driver.find_element_by_xpath('//span[@id="ctl00_C_Pc_gvIndByNationality_ctl03_lblSell1"]').text
ArabRetailSeller = ArabRetailSeller.replace(",","")
ArabRetailSeller = int(ArabRetailSeller)
#print(ArabRetailSeller)

#ArabRetailBuyer
#self.log('I just Visited:' + response.url)
ArabRetailBuyer = driver.find_element_by_xpath('//span[@id="ctl00_C_Pc_gvIndByNationality_ctl03_lblBuy1"]').text
ArabRetailBuyer = ArabRetailBuyer.replace(",","")
ArabRetailBuyer = int(ArabRetailBuyer)
#print(ArabRetailBuyer)


#ArabRetailNet
#self.log('I just Visited:' + response.url)
ArabRetailNet = driver.find_element_by_xpath('//span[@id="ctl00_C_Pc_gvIndByNationality_ctl03_lblNet1"]').text
ArabRetailNet = ArabRetailNet.replace(",","")
ArabRetailNet = int(ArabRetailNet)
#print(ArabRetailBuyer)


#ForeignerRetailSeller
#self.log('I just Visited:' + response.url)
ForeignerRetailSeller = driver.find_element_by_xpath('//span[@id="ctl00_C_Pc_gvIndByNationality_ctl04_lblSell1"]').text
ForeignerRetailSeller = ForeignerRetailSeller.replace(",","")
ForeignerRetailSeller = int(ForeignerRetailSeller)
#print(ForeignerRetailSeller)


#ForeignerRetailBuyer
#self.log('I just Visited:' + response.url)
ForeignerRetailBuyer = driver.find_element_by_xpath('//span[@id="ctl00_C_Pc_gvIndByNationality_ctl04_lblBuy1"]').text
ForeignerRetailBuyer = ForeignerRetailBuyer.replace(",","")
ForeignerRetailBuyer = int(ForeignerRetailBuyer)
#print(ForeignerRetailBuyer)


#ForeignerRetailNet
#self.log('I just Visited:' + response.url)
ForeignerRetailNet = driver.find_element_by_xpath('//span[@id="ctl00_C_Pc_gvIndByNationality_ctl04_lblNet1"]').text
ForeignerRetailNet = ForeignerRetailNet.replace(",","")
ForeignerRetailNet = int(ForeignerRetailNet)
#print(ForeignerRetailNet)




#inistitutions

#EgyptionInistitutionsSeller
#self.log('I just Visited:' + response.url)
EgyptionInistitutionsSeller = driver.find_element_by_xpath('//span[@id="ctl00_C_Pc_gvInstByNationality_ctl02_lblSell1"]').text
EgyptionInistitutionsSeller = EgyptionInistitutionsSeller.replace(",","")
EgyptionInistitutionsSeller = int(EgyptionInistitutionsSeller)
#print(EgyptionInistitutionsSeller)



#EgyptionInistitutionsBuyer
#self.log('I just Visited:' + response.url)
EgyptionInistitutionsBuyer = driver.find_element_by_xpath('//span[@id="ctl00_C_Pc_gvInstByNationality_ctl02_lblBuy1"]').text
EgyptionInistitutionsBuyer = EgyptionInistitutionsBuyer.replace(",","")
EgyptionInistitutionsBuyer = int(EgyptionInistitutionsBuyer)
#print(EgyptionInistitutionsBuyer)


#EgyptionInistitutionsNet
#self.log('I just Visited:' + response.url)
EgyptionInistitutionsNet = driver.find_element_by_xpath('//span[@id="ctl00_C_Pc_gvInstByNationality_ctl02_lblNet1"]').text
EgyptionInistitutionsNet = EgyptionInistitutionsNet.replace(",","")
EgyptionInistitutionsNet = int(EgyptionInistitutionsNet)
#print(EgyptionInistitutionsNet)



#ArabInistitutionsSeller
#self.log('I just Visited:' + response.url)
ArabInistitutionsSeller = driver.find_element_by_xpath('//span[@id="ctl00_C_Pc_gvInstByNationality_ctl03_lblSell1"]').text
ArabInistitutionsSeller = ArabInistitutionsSeller.replace(",","")
ArabInistitutionsSeller = int(ArabInistitutionsSeller)
#print(ArabInistitutionsSeller)



#ArabInistitutionsBuyer
#self.log('I just Visited:' + response.url)
ArabInistitutionsBuyer = driver.find_element_by_xpath('//span[@id="ctl00_C_Pc_gvInstByNationality_ctl03_lblBuy1"]').text
ArabInistitutionsBuyer = ArabInistitutionsBuyer.replace(",","")
ArabInistitutionsBuyer = int(ArabInistitutionsBuyer)
#print(ArabInistitutionsBuyer)


#ArabInistitutionsNet
#self.log('I just Visited:' + response.url)
ArabInistitutionsNet = driver.find_element_by_xpath('//span[@id="ctl00_C_Pc_gvInstByNationality_ctl03_lblNet1"]').text
ArabInistitutionsNet = ArabInistitutionsNet.replace(",","")
ArabInistitutionsNet = int(ArabInistitutionsNet)
#print(ArabInistitutionsNet)


#ForignerInistitutionsSeller
#self.log('I just Visited:' + response.url)
ForignerInistitutionsSeller = driver.find_element_by_xpath('//span[@id="ctl00_C_Pc_gvInstByNationality_ctl04_lblSell1"]').text
ForignerInistitutionsSeller = ForignerInistitutionsSeller.replace(",","")
ForignerInistitutionsSeller = int(ForignerInistitutionsSeller)
#print(ForignerInistitutionsSeller)

#ForignerInistitutionsBuyer
#self.log('I just Visited:' + response.url)
ForignerInistitutionsBuyer = driver.find_element_by_xpath('//span[@id="ctl00_C_Pc_gvInstByNationality_ctl04_lblBuy1"]').text
ForignerInistitutionsBuyer = ForignerInistitutionsBuyer.replace(",","")
ForignerInistitutionsBuyer = int(ForignerInistitutionsBuyer)
#print(ForignerInistitutionsBuyer)


#ForignerInistitutionsNet
#self.log('I just Visited:' + response.url)
ForignerInistitutionsNet = driver.find_element_by_xpath('//span[@id="ctl00_C_Pc_gvInstByNationality_ctl04_lblNet1"]').text
ForignerInistitutionsNet = ForignerInistitutionsNet.replace(",","")
ForignerInistitutionsNet = int(ForignerInistitutionsNet)
#print(ForignerInistitutionsNet)

#BUY
RetailBuy = EgyptionRetailBuyers + ArabRetailBuyer + ForeignerRetailBuyer
InistitutionsBuy = EgyptionInistitutionsBuyer + ArabInistitutionsBuyer + ForignerInistitutionsBuyer

#SELL
RetailSell = EgyptionRetailSellers + ArabRetailSeller + ForeignerRetailSeller
InistitutionsSell = EgyptionInistitutionsSeller + ArabInistitutionsSeller + ForignerInistitutionsSeller

#NETFLOW
RetailNet = EgyptionRetailNet + ArabRetailNet + ForeignerRetailNet
InistitutionsNet = EgyptionInistitutionsNet + ArabInistitutionsNet + ForignerInistitutionsNet


#TOTAL
TotalRetail = (RetailBuy + RetailSell)/2
TotalInistitutional = (InistitutionsBuy + InistitutionsSell)/2
TotalForigner = (ForignerTotalBuyer + ForignerTotalSeller)/2
TotalRegional = (ArabTotalBuyers + ArabTotalSellers)/2
TotalLocal = (EgyptionTotalBuyers + EgyptionTotalSellers)/2
TotalTurnOver = TotalRetail + TotalInistitutional        


Array=[]
#TOTAL
x = datetime.datetime.today().strftime('%Y-%m-%d')
Array.append(x)
Array.append(int(TotalTurnOver))
Array.append(TotalRetail)
Array.append(TotalInistitutional)
Array.append(TotalForigner)
Array.append(TotalRegional)
Array.append(TotalLocal)
#BUY
Array.append(RetailBuy)
Array.append(InistitutionsBuy)
Array.append(ForignerTotalBuyer)
Array.append(ArabTotalBuyers)
Array.append(EgyptionTotalBuyers)

#SELL
Array.append(RetailSell)
Array.append(InistitutionsSell)
Array.append(ForignerTotalSeller)
Array.append(ArabTotalSellers)
Array.append(EgyptionTotalSellers)	

#NETFLOW
Array.append(RetailNet)
Array.append(InistitutionsNet)
Array.append(ForignerTotalNet)
Array.append(ArabTotalNet)
Array.append(EgyptionTotalNet)


print(Array)        

#DATAFRAME
path = 'C:/Users/Aya.Adel/MyFiles/PythonProjects/'

df = readData(path+'ChangedDateCopy.xlsx',0,0,0)
df = df['Sheet1']
n = pd.Series(np.array(Array),index=df.columns)
#print(n)
df = df.append(n,ignore_index=True)

#print(df['Date'])



#columnNames = list(df.head(0)) 
#print (columnNames)
#df.to_csv(path + '/csvSheets/TradingComposition/Data/ChangedDateCopy.csv',index=False)

writer = pd.ExcelWriter( path+'ChangedDateCopy.xlsx')
df.to_excel(writer,'Sheet1')
writer.save()
#df.to_excel()






'''=============================================Separating Sheets For each Category Daily=============================='''
x = df.copy()
''' Total TradingComposition '''
total = pd.DataFrame()
total['Date'] = x['Date']
total['Total Turnover'] = x['Total Turnover']
total['Retail'] = x['Retail']
total['Institutional'] = x['Institutional']
total['Foreign'] = x['Foreign']
total['Regional'] = x['Regional']
total['Local'] = x['Local']

total.to_csv(path+'TotalTradingCompositionEGX.csv',index=False)

''' Buy '''
total = pd.DataFrame()
total['Date'] = x['Date']
total['Retail'] = x['Retail.1']
total['Institutional'] = x['Institutional.1']
total['Foreign'] = x['Foreign.1']
total['Regional'] = x['Regional.1']
total['Local'] = x['Local.1']

total.to_csv(path+'BuyTradingCompositionEGX.csv',index=False)

''' Sell '''
total = pd.DataFrame()
total['Date'] = x['Date']
total['Retail'] = x['Retail.2']
total['Institutional'] = x['Institutional.2']
total['Foreign'] = x['Foreign.2']
total['Regional'] = x['Regional.2']
total['Local'] = x['Local.2']

total.to_csv(path+'SellTradingCompositionEGX.csv',index=False)



''' NetFlow '''
total = pd.DataFrame()
total['Date'] = x['Date']
total['Retail'] = x['Retail.3']
total['Institutional'] = x['Institutional.3']
total['Foreign'] = x['Foreign.3']
total['Regional'] = x['Regional.3']
total['Local'] = x['Local.3']

total.to_csv(path+'NetFlowTradingCompositionEGX.csv',index=False)













