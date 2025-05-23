import click
import hyperleda

import hyperleda_scripts


@click.group()
def cli():
    pass


@cli.group()
def vizier():
    """
    Query the table from Vizier (https://vizier.cds.unistra.fr/viz-bin/VizieR) database
    """


@vizier.command()
@click.option("--catalog", "-c", required=True, help="Catalog name")
@click.option("--table", "-t", required=True, help="Table name")
@click.option("--ignore-cache", "-i", is_flag=True, help="Ignore cache")
@click.option(
    "--hyperleda-table-name",
    "-n",
    help="""
Target table name inside Hyperleda database. If not specified, will be generated from catalog and table names
""",
)
@click.option(
    "--bib-title",
    help="Title of the source paper. Can only be specified with author and year, otherwise ignored",
)
@click.option(
    "--bib-year",
    help="Year the paper was published in. Can only be specified with author and title, otherwise ignored",
)
@click.option(
    "--bib-author",
    help="Author of the source paper. Can only be specified with title and year, otherwise ignored",
)
@click.option("--log-level", default="info", help="Log level")
@click.option(
    "--endpoint",
    help="HyperLeda API endpoint. If not specified, will use the testing HyperLEDA API",
    default=hyperleda.TEST_ENDPOINT,
)
def download(
    catalog,
    table,
    ignore_cache,
    hyperleda_table_name,
    bib_title,
    bib_year,
    bib_author,
    log_level,
    endpoint,
):
    """
    Downloads specified table
    """

    hyperleda_scripts.vizier_command(
        catalog,
        table,
        ignore_cache,
        hyperleda_table_name,
        bib_title,
        bib_year,
        bib_author,
        log_level,
        endpoint,
    )


@cli.group()
def leda():
    """
    Query the table from Old Hyperleda database
    """


@leda.command(name="download")
@click.option("--start-offset", default=0, help="Start offset")
@click.option("--test-limit", default=None, help="Test limit")
@click.option("--batch-size", default=600, help="Batch size")
@click.option("--max-workers", default=4, help="Max workers")
def leda_download(start_offset, test_limit, batch_size, max_workers):
    hyperleda_scripts.leda_command(
        start_offset=start_offset,
        test_limit=test_limit,
        batch_size=batch_size,
        max_workers=max_workers,
    )


if __name__ == "__main__":
    cli()
