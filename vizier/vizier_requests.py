import http

import requests
from astropy import table as atable
from astroquery import vizier

VIZIER_URL = "https://vizier.cds.unistra.fr/viz-bin/votable/-tsv"


def get_columns(client: vizier.VizierClass, catalog_name: str) -> list[str]:
    meta: atable.Table = client.get_catalogs(catalog_name)[0]

    return meta.colnames


def download_table(table_name: str, columns: list[str], max_rows: int | None = None) -> str:
    out_max = "unlimited" if max_rows is None else max_rows

    payload = [
        "-oc.form=dec",
        f"-out.max={out_max}",
        "-sort=_r",
        "-order=I",
        f"-out.src={table_name}",
        "-c.eq=J2000",
        "-c.r=++2",
        "-c.u=arcmin",
        "-c.geom=r",
        f"-source={table_name}",
    ]

    columns = [f"-out={column}" for column in columns]

    data = "&".join(payload + columns)

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }

    response = requests.request(http.HTTPMethod.POST, VIZIER_URL, data=data, headers=headers)

    return response.text
