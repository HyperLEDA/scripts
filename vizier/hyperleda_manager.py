import itertools

import astropy
import astropy.table
import pandas
import structlog
from astropy.io.votable import tree

import hyperleda
from vizier import helpers


class HyperLedaUploader:
    def __init__(self, client: hyperleda.HyperLedaClient):
        self.client = client
        self.log = structlog.get_logger()

    def upload_schema(self, schema: tree.VOTableFile, catalog_name: str, table_name: str) -> int:
        request = self._create_schema_request(schema, catalog_name, table_name)
        self.log.info("Creating table", table_name=request.table_name)
        return self.client.create_table(request)

    def upload_table_data(self, table_id: int, table: astropy.table.Table, batch_size: int = 500):
        for batch in itertools.batched(table, batch_size):
            self.log.info("Uploading batch", batch_size=len(batch))

            rows = []
            for row in batch:
                row_dict = {k: v for k, v in dict(row).items() if v != "--"}
                rows.append(row_dict)
            df = pandas.DataFrame(rows)

            self.client.add_data(table_id=table_id, data=df)

    def _create_schema_request(
        self, schema: tree.VOTableFile, catalog_name: str, table_name: str
    ) -> hyperleda.CreateTableRequestSchema:
        table = schema.get_first_table()
        columns = [
            hyperleda.ColumnDescription(
                name=field.name,
                data_type=field.datatype,
                ucd=field.ucd,
                description=field.description,
                unit=field.unit,
            )
            for field in table.fields
        ]
        bibcode = self._extract_bibcode(schema)

        return hyperleda.CreateTableRequestSchema(
            table_name=helpers.get_filename(catalog_name, table_name),
            columns=columns,
            description=table.description,
            bibcode=bibcode,
        )

    def _extract_bibcode(self, schema: tree.VOTableFile) -> str:
        bibcode_info = next(filter(lambda info: info.name == "cites", schema.resources[0].infos))
        return bibcode_info.value.split(":")[1]
