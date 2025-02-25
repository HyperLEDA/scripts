import itertools
from dataclasses import dataclass

import astropy
import astropy.table
import hyperleda
import pandas
import structlog
from astropy.io.votable import tree


@dataclass
class BibInfo:
    title: str
    year: int
    author: str


class HyperLedaUploader:
    def __init__(self, client: hyperleda.HyperLedaClient, bib_info: BibInfo | None = None):
        self.client = client
        self.log = structlog.get_logger()
        self.bib_info = bib_info

    def upload_schema(self, schema: tree.VOTableFile, table_name: str) -> int:
        request = self._create_schema_request(schema, table_name)

        if self.bib_info:
            self.log.info("Creating internal bibliography entry", title=self.bib_info.title)
            bib_name = self.client.create_internal_source(
                self.bib_info.title, [self.bib_info.author], self.bib_info.year
            )
            request.bibcode = bib_name

        self.log.info("Creating table", table_name=request.table_name)
        return self.client.create_table(request)

    def upload_table_data(self, table_id: int, table: astropy.table.Table, batch_size: int = 500):
        offset = 0
        for batch in itertools.batched(table, batch_size):
            self.log.info("Uploading batch", offset=offset, batch_size=len(batch))
            offset += len(batch)

            rows = []
            for row in batch:
                row_dict = {k: v for k, v in dict(row).items() if v != "--"}
                rows.append(row_dict)
            df = pandas.DataFrame(rows)

            self.client.add_data(table_id=table_id, data=df)

    def _create_schema_request(self, schema: tree.VOTableFile, table_name: str) -> hyperleda.CreateTableRequestSchema:
        table = schema.get_first_table()
        columns = [
            hyperleda.ColumnDescription(
                name=field.ID,
                data_type=field.datatype,
                ucd=field.ucd,
                description=field.description,
                unit=field.unit,
            )
            for field in table.fields
        ]
        bibcode = self._extract_bibcode(schema)

        return hyperleda.CreateTableRequestSchema(
            table_name=table_name,
            columns=columns,
            description=table.description,
            bibcode=bibcode,
        )

    def _extract_bibcode(self, schema: tree.VOTableFile) -> str:
        bibcode_info = next(filter(lambda info: info.name == "cites", schema.resources[0].infos))
        return bibcode_info.value.split(":")[1]
