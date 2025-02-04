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
def download(catalog, table, ignore_cache, hyperleda_table_name):
    """
    Downloads specified table
    """

    hyperleda_scripts.vizier_command(catalog, table, ignore_cache, hyperleda_table_name)


if __name__ == "__main__":
    cli()
