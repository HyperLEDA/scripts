import hyperleda
import numpy as np
import pandas as pd
import psycopg2


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
print(df.columns)
df = df[['column_name', 'unit',"description", "ucd"]]
# print(df)


for table_name in ["m000", "designation", "bref04"]:
    if table_name != "m000":
        df = df[['column_name',"description", "ucd"]]
    else: 
        # меняем невалидные значения
        df = df.replace({"src.morph.param;meta.code.multip;stat.mean": "src.morph.param;stat.mean",
                         "src.morph.type;stat.error": "stat.error;src.morph.type"})

    query = f"""SELECT COLUMN_NAME, DATA_TYPE 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_NAME = '{table_name}'"""
    table_columns = pd.read_sql_query(query, conn)
    table_columns = pd.merge(table_columns, df, on="column_name", how="left")
    table_columns.rename(columns={"column_name": "name"}, inplace=True)

    # print(table_columns)

    table_columns.to_csv(f"./tables/{table_name}_info.csv", index=False)

conn.close()