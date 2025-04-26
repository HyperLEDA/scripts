from collections.abc import Hashable
from typing import Any

import hyperleda
import pandas as pd
import psycopg
import structlog

HYPERLEDA_BIBCODE = "2014A&A...570A..13M"  # bibcode for Leda 2014 article from ads
METADATA_PATH = "hyperleda_scripts/leda/tables/{}_info.csv"

log = structlog.get_logger()


def del_nans(row: dict[Hashable, Any]) -> dict[str, Any]:
    return {str(k): v for k, v in row.items() if v == v}


def leda_dtyper(row: pd.Series) -> pd.Series:
    return pd.Series([hyperleda.DataType(row["data_type"])])


def command(
    hyperleda_db_host: str = "localhost",
    hyperleda_db_database: str = "hyperleda",
    hyperleda_db_user: str = "hyperleda",
    hyperleda_db_password: str = "password",
    hyperleda_db_port: str = "7432",
    endpoint: str = hyperleda.TEST_ENDPOINT,
    test_limit: int = 100000,
    batch_size: int = 500,
):
    log.info(
        "Starting Leda data migration",
        db_host=hyperleda_db_host,
        db_database=hyperleda_db_database,
        db_port=hyperleda_db_port,
        endpoint=endpoint,
        test_limit=test_limit,
        batch_size=batch_size,
    )

    dsn = f"postgresql://{hyperleda_db_host}:{hyperleda_db_port}/{hyperleda_db_database}?user={hyperleda_db_user}&password={hyperleda_db_password}"
    log.debug("Connecting to database", dsn=dsn)
    conn = psycopg.connect(dsn)
    log.info("Database connection established")

    client = hyperleda.HyperLedaClient(endpoint=endpoint)

    for old_table_name in ["m000"]:
        log.info("Processing table", old_table_name=old_table_name)

        # getting columns info
        log.debug("Reading table metadata", path=METADATA_PATH.format(old_table_name))
        table_columns = pd.read_csv(METADATA_PATH.format(old_table_name))
        table_columns["data_type"] = table_columns.apply(leda_dtyper, axis=1)
        table_dict = table_columns.to_dict("records")
        log.info("Table metadata loaded", column_count=len(table_dict))

        # table creation
        table_name = f"hyperleda_{old_table_name}"
        log.info("Creating new table", old_name=old_table_name, new_name=table_name)

        table_id = client.create_table(
            hyperleda.CreateTableRequestSchema(
                table_name=table_name,
                columns=[hyperleda.ColumnDescription(**del_nans(column)) for column in table_dict],
                bibcode=HYPERLEDA_BIBCODE,
            )
        )

        log.info("Table created successfully", table_id=table_id, table_name=table_name)

        offset = 0
        total_rows = 0

        while offset <= test_limit:
            query = f"SELECT * FROM {old_table_name} ORDER BY pgc OFFSET {offset} LIMIT {batch_size};"
            log.debug("Executing query", query=query)
            data = pd.read_sql_query(query, conn)

            # removing unknown bit type field
            if old_table_name == "m000":
                data = data.drop("hptr", axis=1)

            if data.empty:
                log.info("No more data to process", table_name=old_table_name)
                break

            client.add_data(table_id, data)
            total_rows += len(data)

            log.info(
                "Added batch to table",
                table_name=table_name,
                batch_size=len(data),
                total_rows=total_rows,
                progress=f"{offset}/{test_limit}",
            )

            offset += batch_size

        log.info("Table migration completed", table_name=table_name, total_rows=total_rows, test_limit=test_limit)

    log.info("Closing database connection")
    conn.close()
