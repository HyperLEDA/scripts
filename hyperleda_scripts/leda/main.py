import concurrent.futures
import warnings
from collections.abc import Hashable
from typing import Any

import hyperleda
import pandas as pd
import psycopg
import structlog
from psycopg import sql

# Silence the pandas warning about DBAPI2 connection
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable")

HYPERLEDA_BIBCODE = "2014A&A...570A..13M"
METADATA_PATH = "hyperleda_scripts/leda/tables/{}_info.csv"
MAX_WORKERS = 4
BATCH_SIZE = 1000

log = structlog.get_logger()


def del_nans(row: dict[Hashable, Any]) -> dict[str, Any]:
    return {str(k): v for k, v in row.items() if v == v}


def leda_dtyper(row: pd.Series) -> pd.Series:
    return pd.Series([hyperleda.DataType(row["data_type"])])


def process_batch(
    conn: psycopg.Connection,
    old_table_name: str,
    offset: int,
    batch_size: int,
    client: hyperleda.HyperLedaClient,
    table_id: int,
) -> int:
    query = f"SELECT * FROM {old_table_name} where pgc > {offset} ORDER BY pgc LIMIT {batch_size};"
    data = pd.read_sql_query(query, conn)
    if len(data) == 0:
        return 0

    # removing unknown bit type field
    if old_table_name == "m000":
        data = data.drop("hptr", axis=1)

    if not data.empty:
        client.add_data(table_id, data)
        return len(data)
    return 0


def command(
    hyperleda_db_host: str = "localhost",
    hyperleda_db_database: str = "hyperleda",
    hyperleda_db_user: str = "hyperleda",
    hyperleda_db_password: str = "password",
    hyperleda_db_port: str = "7432",
    endpoint: str = "http://dm2.sao.ru:81",
    test_limit: int | None = None,
    start_offset: int = 0,
    batch_size: int = BATCH_SIZE,
    max_workers: int = MAX_WORKERS,
):
    log.info(
        "Starting Leda data migration",
        db_host=hyperleda_db_host,
        db_database=hyperleda_db_database,
        db_port=hyperleda_db_port,
        endpoint=endpoint,
        test_limit=test_limit,
        batch_size=batch_size,
        max_workers=max_workers,
    )

    dsn = f"postgresql://{hyperleda_db_host}:{hyperleda_db_port}/{hyperleda_db_database}?user={hyperleda_db_user}&password={hyperleda_db_password}"
    conn = psycopg.connect(dsn)
    log.info("Database connection established")

    client = hyperleda.HyperLedaClient(endpoint=endpoint)

    for old_table_name in ["m000"]:
        log.info("Processing table", old_table_name=old_table_name)

        # getting columns info
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

        with conn.cursor() as cur:
            query = sql.SQL("SELECT MAX(pgc) FROM {}").format(sql.Identifier(old_table_name))
            cur.execute(query)
            result = cur.fetchone()
            if result is not None:
                total_objects = min(result[0], test_limit or result[0])
            else:
                total_objects = 0

        total_rows = start_offset
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for offset in range(start_offset, test_limit or total_objects, batch_size):
                futures.append(
                    executor.submit(
                        process_batch,
                        conn,
                        old_table_name,
                        offset,
                        batch_size,
                        client,
                        table_id,
                    )
                )

            for future in concurrent.futures.as_completed(futures):
                batch_size = future.result()
                total_rows += batch_size
                log.info(
                    "Progress update",
                    table_name=table_name,
                    added_objects=batch_size,
                    progress=f"{total_rows}/{total_objects}",
                )

        log.info(
            "Table migration completed",
            table_name=table_name,
            total_rows=total_rows,
            test_limit=test_limit,
        )

    log.info("Closing database connection")
    conn.close()
