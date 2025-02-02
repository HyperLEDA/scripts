import structlog

import hyperleda
from vizier import hyperleda_manager, vizier_manager

log: structlog.stdlib.BoundLogger = structlog.get_logger()


def command(catalog_name: str, table_name: str, ignore_cache: bool):
    table_manager = vizier_manager.VizierTableManager(".vizier_cache", ignore_cache)
    uploader = hyperleda_manager.HyperLedaUploader(hyperleda.HyperLedaClient())

    try:
        schema = table_manager.get_schema_from_cache(catalog_name, table_name)
    except FileNotFoundError:
        schema = table_manager.download_schema(catalog_name, table_name)

    table_id = uploader.upload_schema(schema, catalog_name, table_name)

    try:
        table = table_manager.get_table_from_cache(catalog_name, table_name)
    except Exception:
        table = table_manager.download_table(catalog_name, table_name)

    uploader.upload_table_data(table_id, table)
