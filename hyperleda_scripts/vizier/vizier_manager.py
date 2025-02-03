import http
from pathlib import Path

import astropy
import astropy.table
import requests
import structlog
from astropy.io import votable
from astropy.io.votable import tree
from astroquery import vizier

from hyperleda_scripts.vizier import helpers

VIZIER_URL = "https://vizier.cds.unistra.fr/viz-bin/votable/-tsv"


class VizierTableManager:
    def __init__(self, cache_path: str, ignore_cache: bool):
        self.cache_path = cache_path
        self.ignore_cache = ignore_cache
        self.log = structlog.get_logger()

    def get_schema_from_cache(self, catalog_name: str, table_name: str) -> tree.VOTableFile:
        cache_filename = self._get_cache_path("schemas", catalog_name, table_name)
        return self._read_votable(cache_filename)

    def get_table_from_cache(self, catalog_name: str, table_name: str) -> astropy.table.Table:
        cache_filename = self._get_cache_path("tables", catalog_name, table_name)
        return astropy.table.Table.read(cache_filename, format="votable")

    def download_schema(self, catalog_name: str, table_name: str) -> tree.VOTableFile:
        vizier_client = vizier.VizierClass(row_limit=5)
        columns = get_columns(vizier_client, catalog_name)
        raw_header = download_table(table_name, columns, max_rows=10)

        cache_filename = self._get_cache_path("schemas", catalog_name, table_name)
        cache_filename.parent.mkdir(parents=True, exist_ok=True)
        cache_filename.write_text(raw_header)
        self.log.info("Wrote cache", location=str(cache_filename))

        return votable.parse(cache_filename, verify="warn")

    def download_table(self, catalog_name: str, table_name: str) -> astropy.table.Table:
        vizier_client = vizier.VizierClass(row_limit=-1)
        catalogs = vizier_client.get_catalogs(catalog_name)

        table = next((cat for cat in catalogs if cat.meta["name"] == table_name), None)
        if not table:
            raise ValueError("Table not found in the catalog")

        cache_filename = self._get_cache_path("tables", catalog_name, table_name)
        table.write(cache_filename, format="votable")

        return table

    def _get_cache_path(self, type_path: str, catalog_name: str, table_name: str) -> Path:
        filename = f"{helpers.get_filename(catalog_name, table_name)}.vot"
        return Path(self.cache_path) / type_path / filename

    def _read_votable(self, path: Path) -> tree.VOTableFile:
        if self.ignore_cache:
            self.log.info("Ignore cache flag is set")
            raise FileNotFoundError()
        return votable.parse(str(path), verify="warn")


def get_columns(client: vizier.VizierClass, catalog_name: str) -> list[str]:
    meta: astropy.table.Table = client.get_catalogs(catalog_name)[0]

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
