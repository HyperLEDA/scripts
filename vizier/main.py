import itertools
from pathlib import Path

import astropy
import astropy.table
import pandas
import structlog
from astropy.io import votable
from astropy.io.votable import tree
from astroquery import vizier

import hyperleda
from vizier import helpers, vizier_requests

log: structlog.stdlib.BoundLogger = structlog.get_logger()


def load_table(
    catalog_name: str,
    table_name: str,
    cache_path: str,
    ignore_cache: bool,
    max_rows: int | None = None,
) -> tree.VOTableFile:
    cache_filename = f"{cache_path}/{helpers.get_filename(catalog_name, table_name)}.vot"

    try:
        if ignore_cache:
            log.info("Ignore cache flag is set")
            raise Exception()

        table: tree.VOTableFile = votable.parse(cache_filename, verify="warn")
    except Exception:
        log.info("Schema cache file does not exist, downloading...")

        vizier_client = vizier.VizierClass(row_limit=5)

        columns = vizier_requests.get_columns(vizier_client, catalog_name)
        raw_header = vizier_requests.download_table(table_name, columns, max_rows=max_rows)

        path = Path(cache_filename)
        path.parent.mkdir(parents=True, exist_ok=True)

        with path.open("w") as f:
            f.write(raw_header)

        log.info("Wrote schema cache", location=cache_filename)

        table: tree.VOTableFile = votable.parse(cache_filename)

    return table


def get_table_schema(
    schema: tree.VOTableFile, catalog_name: str, table_name: str
) -> hyperleda.CreateTableRequestSchema:
    table: tree.TableElement = schema.get_first_table()

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

    return hyperleda.CreateTableRequestSchema(
        table_name=helpers.get_filename(catalog_name, table_name),
        columns=columns,
        description=table.description,
        bibcode=bibcode,
    )


def download_table(
    catalog_name: str,
    table_name: str,
    vizier_client: vizier.VizierClass,
    cache_path: str,
    ignore_cache: bool,
) -> astropy.table.Table:
    cache_filename = f"{cache_path}/{helpers.get_filename(catalog_name, table_name)}.vot"

    try:
        if ignore_cache:
            log.info("Ignore cache flag is set")
            raise Exception()

        table: astropy.table.Table = astropy.table.Table.read(cache_filename, format="votable")
        return table
    except Exception:
        log.info("Table cache file does not exist, downloading...")

    catalogs = vizier_client.get_catalogs(catalog_name)
    tbl: astropy.table.Table | None = None

    for catalog in catalogs:
        if catalog.meta["name"] == table_name:
            tbl = catalog
            break

    if not tbl:
        raise Exception("Table not found in the catalog")

    path = Path(cache_filename)
    path.parent.mkdir(parents=True, exist_ok=True)
    tbl.write(cache_filename, format="votable")

    return tbl


def upload_table(hyperleda_client: hyperleda.HyperLedaClient, table_id: int, table: astropy.table.Table):
    for batch in itertools.batched(table, 500):
        log.info("Uploading batch", batch_size=len(batch))
        rows = []

        for row in batch:
            row = dict(row)
            row = {k: v for k, v in row.items() if v != "--"}
            rows.append(dict(row))

        df = pandas.DataFrame(rows)
        print(df)

        hyperleda_client.add_data(table_id=table_id, data=df)


def command(catalog_name: str, table_name: str, ignore_cache: bool):
    # 1. check cache for the votable file
    # 2. get columns of the table from vizier metadata method
    # 3. query votable from vizier with the columns
    # 4. save votable file to cache
    # 5. convert votable metadata to hyperleda request
    # 6. create table in hyperleda
    # 7. in batches of 500 rows, send add data request to the table

    client = hyperleda.HyperLedaClient()

    schema = load_table(catalog_name, table_name, ".vizier_cache/schemas", ignore_cache)
    log.info("Schema loaded", table=schema.resources[0].description)

    request = get_table_schema(schema, catalog_name, table_name)
    log.info("Requesting table creation...", table_name=request.table_name)

    table_id = client.create_table(request)

    log.info("Table created", table_id=table_id)

    table = download_table(
        catalog_name,
        table_name,
        vizier.VizierClass(row_limit=-1),
        ".vizier_cache/tables",
        ignore_cache,
    )

    upload_table(client, table_id, table)
