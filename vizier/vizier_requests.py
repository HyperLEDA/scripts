import http

import requests
from astropy import table as atable
from astroquery import vizier

VIZIER_URL = "https://vizier.cds.unistra.fr/viz-bin/votable/-tsv"


def get_columns(client: vizier.VizierClass, catalog_name: str) -> list[str]:
    meta: atable.Table = client.get_catalogs(catalog_name)[0]

    return meta.colnames


def get_table_schema(table_name: str, columns: list[str]) -> str:
    payload = [
        "-oc.form=dec",
        "-out.max=10",
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
