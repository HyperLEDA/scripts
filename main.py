import click
import vizier.main as vizier_downloader


@click.group()
def cli():
    pass


@cli.group()
def vizier():
    """
    Query the table from Vizier (https://vizier.cds.unistra.fr/viz-bin/VizieR) database
    """


@vizier.command()
@click.option("--table", "-t", required=True, help="Table name")
@click.option("--catalog", "-c", required=True, help="Catalog name")
def download(table, catalog):
    """
    Downloads specified table
    """

    vizier_downloader.command(catalog, table, False)


if __name__ == "__main__":
    cli()
