import click

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
@click.option("--endpoint", help="HyperLeda API endpoint. If not specified, will use the default endpoint")
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
def leda_download():
    hyperleda_scripts.leda_command()


if __name__ == "__main__":
    cli()
