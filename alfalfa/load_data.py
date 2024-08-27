import pandas as pd
import hyperleda
import os
import psycopg2



ALFALFA_BIBCODE = "2018ApJ...861...49H" # bibcode for ALFALFA 2018 article from ads and offisial website

conn = psycopg2.connect(
    host= os.getenv("HYPERLEDA_DB_HOST"),
    database=os.getenv("HYPERLEDA_DB_DATABASE"),
    user=os.getenv("HYPERLEDA_DB_USER"),
    password=os.getenv("HYPERLEDA_DB_PASSWORD"),
    port=os.getenv("HYPERLEDA_DB_PORT")
)

client = hyperleda.HyperLedaClient(endpoint=hyperleda.TEST_ENDPOINT) 


def del_nans(row):
    return {k:v for k,v in row.items() if v == v}

def leda_dtyper(row) -> str: 
    return hyperleda.DataType(row["data_type"])


# getting columns info
table_columns = pd.read_csv(f"./alfalfa/tables/main_info.csv")
table_columns["data_type"] = table_columns.apply(leda_dtyper, axis=1)
table_dict = table_columns.to_dict("records")

# table creation
table_name = f"alfalfa_hi_source_catalog" 

table_id = client.create_table(
    hyperleda.CreateTableRequestSchema(
        table_name=table_name,
        columns=[
            hyperleda.ColumnDescription(**del_nans(column)) for column in table_dict
            ],
        bibcode=ALFALFA_BIBCODE,
    )
)

print(f"Created table '{table_name}' with ID: {table_id}")

# reading all data from alfalfa catalog
df = pd.read_csv("./alfalfa/tables/main_data.csv")

offset = 0
batch = 500
test_limit = 1000

while offset <= test_limit:
    data = df.iloc[offset:offset+batch]

    if data.empty:
        break

    print(data)
    client.add_data(table_id, data)

    print(f"Added {data.shape[0]} rows to the table {table_name}. In total {offset + batch} rows")

    offset += batch

print(f"Added all data to the table '{table_name}'")

conn.close()

