import hyperleda
import pandas as pd
import psycopg

HYPERLEDA_BIBCODE = "2014A&A...570A..13M"  # bibcode for Leda 2014 article from ads
METADATA_PATH = "hyperleda_scripts/leda/tables/{}_info.csv"


def del_nans(row):
    return {k: v for k, v in row.items() if v == v}


def leda_dtyper(row) -> str:
    return hyperleda.DataType(row["data_type"])


def command(
    hyperleda_db_host: str = "localhost",
    hyperleda_db_database: str = "hyperleda",
    hyperleda_db_user: str = "hyperleda",
    hyperleda_db_password: str = "password",
    hyperleda_db_port: str = "7432",
    endpoint: str = hyperleda.DEFAULT_ENDPOINT,
    test_limit: int = 100000,
    batch_size: int = 500,
):
    dsn = f"postgresql://{hyperleda_db_host}:{hyperleda_db_port}/{hyperleda_db_database}?user={hyperleda_db_user}&password={hyperleda_db_password}"

    conn = psycopg.connect(dsn)

    client = hyperleda.HyperLedaClient(endpoint=endpoint)

    for old_table_name in ["m000"]:
        # getting columns info
        table_columns = pd.read_csv(METADATA_PATH.format(old_table_name))
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

        while offset <= test_limit:
            query = f"SELECT * FROM {old_table_name} ORDER BY pgc OFFSET {offset} LIMIT {batch_size};"
            data = pd.read_sql_query(query, conn)

            # removing unknown bit type field
            if old_table_name == "m000":
                data = data.drop("hptr", axis=1)

            if data.empty:
                break

            print(data)
            client.add_data(table_id, data)

            print(f"Added {data.shape[0]} rows to the table {table_name}. In total {offset + batch_size} rows")

            offset += batch_size
        print(f"Added all data to the table '{table_name}'")

    conn.close()
