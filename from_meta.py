import pandas as pd
import psycopg2
import numpy as np


conn = psycopg2.connect(
    host="localhost",
    database="hyperleda",
    user="hyperleda",
    password="password",
    port="6432"
)

df = pd.DataFrame()

for table in ["metaa000", "metaa001", "metaa002", 'metaa003', 'metaa004', 'metaa005', 
              'metaa006', 'metaa007', 'metaa009', 'metaa010','metaa011', 'metaa101', 
              'metaa102', 'metaa103', 'metaa104', 'metaa105', 'metaa106', 'metaa107',
              'metaa108', 'metaa109', 'metaa110', 'metaa112', 'metaa113', 'metaa114', 
              'metaa115']:
    query = f"""select DISTINCT * from {table}"""
    df = df._append(pd.read_sql_query(query, conn))

df = df.drop_duplicates()
df = df.pivot_table(values='field', index='field', columns='name', aggfunc='first')
df = df.reset_index() 

def check_nans(row, col1='unit', col2='units') -> str: 
    if pd.isna(row[col1]) and not pd.isna(row[col2]):
        return row[col2]
    return row[col1]


df['unit'] = df.apply(check_nans, axis=1)
df = df.drop('units', axis=1)
df.rename(columns={"field": "column_name"}, inplace=True)
df = df[['column_name', 'unit',"description", "ucd"]]
# print(df)


query = f"""SELECT COLUMN_NAME, DATA_TYPE 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'm000'"""
m000_columns = pd.read_sql_query(query, conn)
m000_columns = pd.merge(m000_columns, df, on="column_name", how="left")
# print(m000_columns[m000_columns['ucd'].isna()]) -> logavmm e_logavmm hptr e_mabs этих почему то нет в meta

