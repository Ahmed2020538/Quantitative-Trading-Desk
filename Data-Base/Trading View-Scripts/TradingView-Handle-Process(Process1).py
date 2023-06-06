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
print(f"=================\nCsv files::\n----------\n{filelis}")
#-----------------------------------------------
#-----------------------------------------------
# load data:
#---------------------
Pyear  = input("Enter The year  of hist date :: ")
Pmonth = input("Enter The Month of hist date :: ")
Pday   = input("Enter The Day   of hist date :: ")
PHour  = input("Enter The Hour  of hist date :: ")
PMins  = input("Enter The Min   of hist date :: ")
Nyear  = input("Enter The Year  of new date  :: ")
Nmonth = input("Enter The Month of new date  :: ")
Nday   = input("Enter The Day   of new date  :: ")
TFdata = input("Enter The timeFrameof  data  :: ")
foldataname = input("Enter The Folder Name to store data  :: ")
print(f"---------------------------\n Done Thanks for so much my freind \n---------------------------")
os.mkdir(f"{foldataname}")
for fi in filelis :
    df = pd.read_csv(f"{fi}") 
    fileName = fi.split(",")[0].split("_")[2]
    print(fileName)
    #print(f" Columns :: {df.info()} \n===========================\n Infos :: {df.columns}")
    df.drop(df[(df['time'] <= f"{Pyear}-{Pmonth}-{Pday}T{PHour}:{PMins}:00Z")].index, inplace=True) # changeable
    df['time'] = pd.to_datetime(df['time'], errors='coerce' )
    #print(f"{df.info()} \n==================================\n {df}" )
    df = df.assign(Date=df.time.dt.date, Time=df.time.dt.time)
    df.drop("time", axis = 1 , inplace = True)
    df = df[["Date","Time" , "open" , "high" , "low" , "close"]]
    print(f"{df}\n======================================\n")
    #df.to_csv("5Min/DFM-1H-HistData(01-03-2023-YMD).csv" , index=False) # example of Enteries data
    df.to_csv(f"{foldataname}/{fileName}-{TFdata}-HistData({Nday}-{Nmonth}-{Nyear}-YMD).csv" , index=False)
    