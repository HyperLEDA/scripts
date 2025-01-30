from pathlib import Path

import structlog
from astropy.io import votable
from astropy.io.votable import tree
from astroquery import vizier

import hyperleda
from vizier import helpers, vizier_requests

log: structlog.stdlib.BoundLogger = structlog.get_logger()

def load_schema(catalog_name: str, table_name: str, ignore_cache: bool) -> tree.VOTableFile:
    schema_cache_filename = f".vizier_cache/schemas/{helpers.get_filename(catalog_name, table_name)}.vot"

    try:
        if ignore_cache:
            log.info("Ignore cache flag is set")
            raise Exception()

        table: tree.VOTableFile = votable.parse(schema_cache_filename)
    except Exception:
        log.info("Schema cache file does not exist, downloading...")

        vizier_client = vizier.VizierClass(row_limit=5)

        columns = vizier_requests.get_columns(vizier_client, catalog_name)
        raw_header = vizier_requests.get_table_schema(table_name, columns)

        path = Path(schema_cache_filename)
        path.parent.mkdir(parents=True, exist_ok=True)

        with path.open("w") as f:
            f.write(raw_header)

        log.info("Wrote schema cache", location=schema_cache_filename)

        table: tree.VOTableFile = votable.parse(schema_cache_filename)

    return table


def command(catalog_name: str, table_name: str, ignore_cache: bool):
    # 1. check cache for the votable file
    # 2. get columns of the table from vizier metadata method
    # 3. query votable from vizier with the columns
    # 4. save votable file to cache
    # 5. convert votable metadata to hyperleda request
    # 6. create table in hyperleda
    # 7. in batches of 500 rows, send add data request to the table

    schema = load_schema(catalog_name, table_name, ignore_cache)
    log.info("Schema loaded", table=schema.resources[0].description)

    table = schema.get_first_table()

    columns = []

    for field in table.fields:
        columns.append(
            hyperleda.ColumnDescription(
                name=field.name,
                data_type=field.datatype,
                ucd=field.ucd,
                description=field.description,
                unit=field.unit,
            )
        )

    bibcode = next(filter(lambda info: info.name == "cites", schema.resources[0].infos))
    bibcode = bibcode.value.split(":")[1]

    request = hyperleda.CreateTableRequestSchema(
        table_name=helpers.get_filename(catalog_name, table_name),
        columns=columns,
        description=table.description,
        bibcode=bibcode,
    )

    client = hyperleda.HyperLedaClient()
    table_id = client.create_table(request)

    log.info("Table created", table_id=table_id)
