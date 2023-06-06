# Load Requairies Packeage::
#---------------------------
import pandas as pd
import numpy as np 
from datetime import datetime as dt
import time
import os 
#-----------------------------------------------
#-----------------------------------------------
# Identify dirction and present Content of path::
#-----------------------------------------------
path = os.path.abspath('') 
print(f"The Path Dirction :: {path}\n----------------\n\nThe Content Path\n--------------------")
files = os.listdir(path)
filelis = []
for file in files :
    if file.endswith('.csv'):
        filelis.append(file)
        print(file)
print(f"========================================\nCsv files::\n----------\n{filelis}")
#-----------------------------------------------
#-----------------------------------------------
# drop subset of data from Files:
#--------------------------------
Byear  = input("Enter The year  of hist date :: ")
Bmonth = input("Enter The Month of hist date :: ")
Bday   = input("Enter The Day   of hist date :: ")
Nyear  = input("Enter The Year  of new date  :: ")
Nmonth = input("Enter The Month of new date  :: ")
Nday   = input("Enter The Day   of new date  :: ")
TFdata = input("Enter The timeFrameof  data  :: ")
foldataname = input("Enter The Folder Name to store data  :: ")
print(f"------------------------------------\n Done Thanks for so much my freind \n---------------------------------")
os.mkdir(f"{foldataname}")
for fi in filelis :
    df = pd.read_csv(f"{fi}") 
    fileName = fi.split("-")[0]
    print(fileName)
    df.drop(df[(df['Date'] >= f"{Byear}-{Bmonth}-{Bday}")].index , inplace=True) 
    print(f"{df}\n================================================================\n================================================================\n")
    df.to_csv(f"{foldataname}/{fileName}-{TFdata}-HistData({Nday}-{Nmonth}-{Nyear}-YMD).csv" , index=False)
    