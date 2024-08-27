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

client = hyperleda.HyperLedaClient(endpoint="http://89.169.133.242") 


def del_nans(row):
    # remove nans
    return {k:v for k,v in row.items() if v == v}

def leda_dtyper(row) -> str: 
    return hyperleda.DataType(row["data_type"])


for old_table_name in ["m000", "designation", "bref04"]:
    # getting columns info
    table_columns = pd.read_csv(f"./tables/{old_table_name}_info.csv")
    table_columns["data_type"] = table_columns.apply(leda_dtyper, axis=1)

    table_dict = table_columns.to_dict("records")

    # table creation
    table_name = f"new_{old_table_name}" 

    table_id = client.create_table(
        hyperleda.CreateTableRequestSchema(
            table_name=table_name,
            columns=[
                hyperleda.ColumnDescription(**del_nans(column)) for column in table_dict
                ],
            bibcode="2014A&A...570A..13M", # bibcode for Leda 2014 article from ads
        )
    )

    print(f"Created table '{table_name}' with ID: {table_id}")

    offset = 0
    batch = 10000

    while True:
        query = f"SELECT * FROM {old_table_name} OFFSET {offset} LIMIT {batch};"
        data = pd.read_sql_query(query, conn)

        if data.empty:
            break

        print(data)
        client.add_data(table_id, data)

        print(f"Added {data.shape[0]} rows to the table '{table_name}'")

        offset += batch
    print(f"Added all data to the table '{table_name}'")

conn.close()