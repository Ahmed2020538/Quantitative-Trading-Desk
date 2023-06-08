import pandas as pd
import cx_Oracle
import datetime
import csv
import time
import sys

def Extract_EGX_Symbol(sql , Name) :
    con = cx_Oracle.connect('STOCK/P3rXdM5HbSgQRmCS@10.1.20.41:1521/STOCK')
    cursor = con.cursor()    
    df_sectors = pd.read_sql(sql, con)
    df_sectors.REUTERS = df_sectors.REUTERS.apply(lambda x : x.split(".")[0])
    print (f"""DB_Connection_Version :: {con.version}\n===========\n{Name} Symbols ::\n=================\n
{df_sectors.REUTERS.unique()}\n============\n{Name}Symbols count :: {df_sectors.REUTERS.count()}""")
   



# 1st TAble ::


Extract_EGX_Symbol("""SELECT EGX20_CAP_SYMBOLS.SYMBOL_CODE ,  SYMBOLINFO.REUTERS
                    from EGX20_CAP_SYMBOLS LEFT JOIN SYMBOLINFO
                    ON EGX20_CAP_SYMBOLS.SYMBOL_CODE = SYMBOLINFO.SYMBOL_CODE 
                    ORDER BY SYMBOLINFO.REUTERS """ , "EGX20_CAP_SYMBOLS")




# 2nd TAble ::


Extract_EGX_Symbol("""SELECT CASE30_COMPANIES.SYMBOL_CODE ,  SYMBOLINFO.REUTERS
                from CASE30_COMPANIES LEFT JOIN SYMBOLINFO
                ON CASE30_COMPANIES.SYMBOL_CODE = SYMBOLINFO.SYMBOL_CODE 
                ORDER BY SYMBOLINFO.REUTERS """ , "CASE30_COMPANIES")




# 3dr TAble ::



Extract_EGX_Symbol("""SELECT EGX50_SYMBOLS.SYMBOL_CODE ,  SYMBOLINFO.REUTERS
                from EGX50_SYMBOLS LEFT JOIN SYMBOLINFO
                ON EGX50_SYMBOLS.SYMBOL_CODE = SYMBOLINFO.SYMBOL_CODE 
                ORDER BY SYMBOLINFO.REUTERS """ , "EGX50_SYMBOLS")



# 4th TAble ::


Extract_EGX_Symbol("""SELECT EGX100_SYMBOLS.SYMBOL_CODE ,  SYMBOLINFO.REUTERS
                from EGX100_SYMBOLS LEFT JOIN SYMBOLINFO
                ON EGX100_SYMBOLS.SYMBOL_CODE = SYMBOLINFO.SYMBOL_CODE 
                ORDER BY SYMBOLINFO.REUTERS """ , "EGX100_SYMBOLS")




# 5th TAble ::

Extract_EGX_Symbol("""SELECT EGX_NILEX_SYMBOLS.SYMBOL_CODE ,  SYMBOLINFO.REUTERS
                from EGX_NILEX_SYMBOLS LEFT JOIN SYMBOLINFO
                ON EGX_NILEX_SYMBOLS.SYMBOL_CODE = SYMBOLINFO.SYMBOL_CODE 
                ORDER BY SYMBOLINFO.REUTERS """ , "EGX_NILEX_SYMBOLS")


con = cx_Oracle.connect('STOCK/P3rXdM5HbSgQRmCS@10.1.20.41:1521/STOCK')
cursor = con.cursor()
sqls_scripts = ["""SELECT EGX20_CAP_SYMBOLS.SYMBOL_CODE ,  SYMBOLINFO.REUTERS
                    from EGX20_CAP_SYMBOLS LEFT JOIN SYMBOLINFO
                    ON EGX20_CAP_SYMBOLS.SYMBOL_CODE = SYMBOLINFO.SYMBOL_CODE 
                    ORDER BY SYMBOLINFO.REUTERS """ ,
                """SELECT CASE30_COMPANIES.SYMBOL_CODE ,  SYMBOLINFO.REUTERS
                from CASE30_COMPANIES LEFT JOIN SYMBOLINFO
                ON CASE30_COMPANIES.SYMBOL_CODE = SYMBOLINFO.SYMBOL_CODE 
                ORDER BY SYMBOLINFO.REUTERS """ ,
                """SELECT EGX50_SYMBOLS.SYMBOL_CODE ,  SYMBOLINFO.REUTERS
                from EGX50_SYMBOLS LEFT JOIN SYMBOLINFO
                ON EGX50_SYMBOLS.SYMBOL_CODE = SYMBOLINFO.SYMBOL_CODE 
                ORDER BY SYMBOLINFO.REUTERS """ ,
                """SELECT EGX100_SYMBOLS.SYMBOL_CODE ,  SYMBOLINFO.REUTERS
                from EGX100_SYMBOLS LEFT JOIN SYMBOLINFO
                ON EGX100_SYMBOLS.SYMBOL_CODE = SYMBOLINFO.SYMBOL_CODE 
                ORDER BY SYMBOLINFO.REUTERS """ ,
                """SELECT EGX_NILEX_SYMBOLS.SYMBOL_CODE ,  SYMBOLINFO.REUTERS
                from EGX_NILEX_SYMBOLS LEFT JOIN SYMBOLINFO
                ON EGX_NILEX_SYMBOLS.SYMBOL_CODE = SYMBOLINFO.SYMBOL_CODE 
                ORDER BY SYMBOLINFO.REUTERS """]

Names = ["EGX20_CAP_SYMBOLS" , "CASE30_COMPANIES" , "EGX50_SYMBOLS" , "EGX100_SYMBOLS" , "EGX_NILEX_SYMBOLS"]
for sql in sqls_scripts :
    for Name in Names :
    
        df_sectors = pd.read_sql(sql, con)
        df_sectors.REUTERS = df_sectors.REUTERS.apply(lambda x : x.split(".")[0])

        df_sectors.to_csv(path = "E:/Repos/WORK/Daily_Tasks/Runing_DataBase" + f'{Name}.csv' , mode='a',header=False,index=False)
        print(f"{Name}Done\n======================")



