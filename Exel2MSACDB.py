import pandas as pd
import pyodbc
import os, fnmatch
from datetime import datetime
import xlrd

listOfFiles = os.listdir('.')
pattern = "*.accdb"
for entry in listOfFiles:
    if fnmatch.fnmatch(entry, pattern):
        DBFile = os.path.abspath(entry)
exl_files = []
df_array = []
time_set = set()
table_names = ["reports_date", "region", "unit_number", "hour"]
date_types = ["Text", "Text", "Number", "Number"]
pattern = "*.xls"
for entry in listOfFiles:
    if fnmatch.fnmatch(entry, pattern):
        exl_files.append(datetime.strptime(entry[:8], '%Y%m%d'))
        exl_files[-1] = exl_files[-1].strftime('%Y%m%d')
        exFile = os.path.abspath(entry)
        xls = xlrd.open_workbook(exFile, on_demand=True)
        time_set.update(xls.sheet_names())
        df = pd.read_excel(exFile, sheet_name=None, names=["unit_number", "unit_name", "U_nom", "U", "region", "price", "empty"], header=0)
        for key in df.keys():
            df[key] = df[key].drop(labels=[0, 1], axis=0)
            df[key] = df[key].drop(labels="empty", axis=1)
            df[key]["Date"] = exl_files[-1]
            df[key]["hour"] = key
        df_array.append(df)

time_lst = list(time_set)
for i in range(len(time_lst)):
    time_lst[i] = int(time_lst[i])
time_lst.sort()

union_df_lst = []
for i in range(len(df_array)):
    union_df_lst.append(pd.concat(df_array[i]))
union_df = pd.concat(union_df_lst)
conn = pyodbc.connect('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + DBFile)
curs = conn.cursor()
try:
    curs.execute("DROP TABLE RESULT_PRICE_TABLE")
except Exception:
    print("RESULT_PRICE_TABLE not found ")
for i in range(len(table_names)):
    try:
        curs.execute("CREATE TABLE %s (%s INTEGER PRIMARY KEY, %s %s)" % (table_names[i], "ID" + table_names[i].upper(), table_names[i], date_types[i]))
    except Exception:
        curs.execute("DROP TABLE %s" % table_names[i])
        curs.execute("CREATE TABLE %s (%s INTEGER PRIMARY KEY, %s %s)" % (table_names[i], "ID" + table_names[i].upper(), table_names[i], date_types[i]))
for i in range(len(exl_files)):
    curs.execute("INSERT INTO reports_date(IDREPORTS_DATE, reports_date) VALUES ((?),(?))", (i+1, exl_files[i]))
unique_region = union_df.region.unique()
for i in range(len(unique_region)):
    curs.execute("INSERT INTO region(IDREGION, region) VALUES ((?),(?))", (i+1, unique_region[i]))
for i in range(len(time_lst)):
    curs.execute("INSERT INTO HOUR(IDHOUR, hour) VALUES ((?),(?))", (i+1, time_lst[i]))
unique_unit_number = union_df.unit_number.unique()
for i in range(len(unique_unit_number)):
    curs.execute("INSERT INTO unit_number(IDUNIT_NUMBER, unit_number) VALUES ((?),(?))", (i+1, unique_unit_number[i]))
try:
    curs.execute("CREATE TABLE RESULT_PRICE_TABLE (ID AUTOINCREMENT PRIMARY KEY, IDREPORTS_DATE INTEGER REFERENCES reports_date(IDREPORTS_DATE), IDREGION INTEGER REFERENCES region(IDREGION), IDUNIT_NUMBER INTEGER REFERENCES unit_number(IDUNIT_NUMBER), IDHOUR INTEGER REFERENCES hour(IDHOUR), PRICE FLOAT)")
except Exception:
    curs.execute("DROP TABLE RESULT_PRICE_TABLE")
    curs.execute("CREATE TABLE RESULT_PRICE_TABLE (ID AUTOINCREMENT PRIMARY KEY, IDREPORTS_DATE TEXT REFERENCES reports_date(IDREPORTS_DATE), IDREGION INTEGER REFERENCES region(IDREGION), IDUNIT_NUMBER INTEGER REFERENCES unit_number(IDUNIT_NUMBER), IDHOUR INTEGER REFERENCES hour(IDHOUR), PRICE FLOAT)")
for i in range(len(union_df.region)):
    name = "'" + union_df.region[i] + "'"
    date_name = "'" + union_df.Date[i] + "'"
    curs.execute("SELECT region.IDREGION FROM region where region.region = %s union all SELECT unit_number.IDUNIT_NUMBER FROM unit_number where unit_number.unit_number = %d union all SELECT reports_date.IDREPORTS_DATE FROM reports_date where reports_date.reports_date = %s union all SELECT hour.IDHOUR FROM hour where hour.hour = %d"  %(name, union_df.unit_number[i], date_name, int(union_df.hour[i])))
    row = curs.fetchall()
    curs.execute("INSERT INTO RESULT_PRICE_TABLE(IDREGION,IDUNIT_NUMBER, PRICE, IDREPORTS_DATE, IDHOUR) VALUES ((?),(?),(?),(?),(?))", (row[0][0], row[1][0], union_df.price[i], row[2][0], row[3][0]))
conn.commit()
conn.close()
