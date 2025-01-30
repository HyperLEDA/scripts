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
def download():
    """
    Downloads specified table
    """

    vizier_downloader.command("III/258", "III/258/fbs", False)


if __name__ == "__main__":
    cli()
