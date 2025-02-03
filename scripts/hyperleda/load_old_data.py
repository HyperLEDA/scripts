import os

import hyperleda
import pandas as pd
import psycopg2

HYPERLEDA_BIBCODE = "2014A&A...570A..13M"  # bibcode for Leda 2014 article from ads

conn = psycopg2.connect(
    host=os.getenv("HYPERLEDA_DB_HOST"),
    database=os.getenv("HYPERLEDA_DB_DATABASE"),
    user=os.getenv("HYPERLEDA_DB_USER"),
    password=os.getenv("HYPERLEDA_DB_PASSWORD"),
    port=os.getenv("HYPERLEDA_DB_PORT"),
)

client = hyperleda.HyperLedaClient(endpoint=hyperleda.TEST_ENDPOINT)


def del_nans(row):
    return {k: v for k, v in row.items() if v == v}


def leda_dtyper(row) -> str:
    return hyperleda.DataType(row["data_type"])


for old_table_name in ["m000", "designation"]:
    # getting columns info
    table_columns = pd.read_csv(f"./hyperleda/tables/{old_table_name}_info.csv")
    table_columns["data_type"] = table_columns.apply(leda_dtyper, axis=1)
    table_dict = table_columns.to_dict("records")

    # table creation
    table_name = f"hyperleda_{old_table_name}"

    table_id = client.create_table(
        hyperleda.CreateTableRequestSchema(
            table_name=table_name,
            columns=[hyperleda.ColumnDescription(**del_nans(column)) for column in table_dict],
            bibcode=HYPERLEDA_BIBCODE,
        )
    )

    print(f"Created table '{table_name}' with ID: {table_id}")

    offset = 0
    batch = 500
    test_limit = 1000

    while offset <= test_limit:
        query = f"SELECT * FROM {old_table_name} OFFSET {offset} LIMIT {batch};"
        data = pd.read_sql_query(query, conn)

        # removing unknown bit type field
        if old_table_name == "m000":
            data = data.drop("hptr", axis=1)

        if data.empty:
            break

        print(data)
        client.add_data(table_id, data)

        print(f"Added {data.shape[0]} rows to the table {table_name}. In total {offset + batch} rows")

        offset += batch
    print(f"Added all data to the table '{table_name}'")

conn.close()
