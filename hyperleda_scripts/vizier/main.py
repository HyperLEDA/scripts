import hyperleda
import structlog

from hyperleda_scripts.vizier import helpers, hyperleda_manager, vizier_manager

log: structlog.stdlib.BoundLogger = structlog.get_logger()


def command(
    catalog_name: str,
    table_name: str,
    ignore_cache: bool = False,
    hyperleda_table_name: str | None = None,
    bib_title: str | None = None,
    bib_year: str | None = None,
    bib_author: str | None = None,
    log_level: str = "info",
) -> int:
    structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(log_level))

    bib_info = None
    if bib_title and bib_year and bib_author:
        bib_info = hyperleda_manager.BibInfo(title=bib_title, year=int(bib_year), author=bib_author)

    hyperleda_table_name = hyperleda_table_name or helpers.get_filename(catalog_name, table_name)
    table_manager = vizier_manager.VizierTableManager(".vizier_cache", ignore_cache)
    uploader = hyperleda_manager.HyperLedaUploader(hyperleda.HyperLedaClient(), bib_info)

    try:
        schema = table_manager.get_schema_from_cache(catalog_name, table_name)
        log.debug("Hit cache for the schema, no downloading will be performed")
    except FileNotFoundError:
        schema = table_manager.download_schema(catalog_name, table_name)

    table_id = uploader.upload_schema(schema, hyperleda_table_name)

    try:
        table = table_manager.get_table_from_cache(catalog_name, table_name)
        log.debug("Hit cache for the table, no downloading will be performed")
    except Exception:
        table = table_manager.download_table(catalog_name, table_name)

    uploader.upload_table_data(table_id, table)

    return table_id
