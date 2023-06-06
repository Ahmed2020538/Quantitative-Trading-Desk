# Load Requairies Packeage::
#---------------------------
import pandas as pd
import numpy as np 
from datetime import datetime as dt
import time
import os 
print("===================(Loading Package done)=======================")
# Identify dirction and present Content of path::
#----------------------------------------------------
#----------------------------------------------------
path = os.path.abspath('') 
print(f"The Path Dirction :: {path}\n----------------\n\nThe Content Path\n--------------------")
files = os.listdir(path)
for file in files :
    if file.endswith('.csv') :
        print(file)
print("===================(Extract Path Content done)=======================")        
# Merge Csv filies which have the same Name together::
#----------------------------------------------------
#----------------------------------------------------
Nyear  = input("Enter The Year  of new date  :: ")
Nmonth = input("Enter The Month of new date  :: ")
Nday   = input("Enter The Day   of new date  :: ")
TFdata = input("Enter The timeFrameof  data  :: ")
foldataname = input("Enter The Folder Name to store data  :: ")
print(f"==========================================")
os.mkdir(f"{foldataname}")
df = pd.DataFrame()
for file in files:
    fileName = file.split("-")[0]
    if file.endswith('.csv') and file.startswith(fileName) :
        df = df.append(pd.read_csv(file), ignore_index=True) 
        print(f"{fileName}")
        #print(fileName)
        df.head() 
        df.to_csv(f"{foldataname}/{fileName}-{TFdata}-HistData({Nday}-{Nmonth}-{Nyear}-YMD).csv" , index=False)
        #print(f"===================(Merge {fileName} Files done)=======================") 
print(f"===================(Merge All Same Files done)=======================") 