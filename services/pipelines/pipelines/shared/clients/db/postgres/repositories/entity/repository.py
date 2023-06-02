from shared.clients.db.postgres.repositories.base import PgRepositories
from shared.clients.db.postgres.repositories.entity.schema import Entity
from shared.utils.conversion.converter import model_to_polars_schema
from shared.utils.sql.file import QueryFile


class EntityRepo(PgRepositories):
    table = ("data", "entity")
    schema = Entity

    def find_all(self):
        return self._query.find("SELECT * FROM data.entity").get_polars_df()

    def _add(self, data):
        """
        Add helper method.
        """
        return self._query.add(
            data=data, column_model=Entity, table=self.table, conflict="DO_NOTHING", returning="ALL_COLUMNS"
        ).get_polars_df(model_to_polars_schema(self.schema))

    def add(self, data: list[dict]):
        BATCH_SIZE = 50 * 1000

        if isinstance(data, list) and len(data) > BATCH_SIZE:
            print("spliting")
            batches = [data[i : i + BATCH_SIZE] for i in range(0, len(data), BATCH_SIZE)]

            for batch in batches:
                print(len(batch))
                self._add(batch)

        return self._add(data)

    def bulk_add(self, data):
        add = self._query.bulk_add(
            data=data,
            table=self.table,
            columns=[
                "lei",
                "name",
                # "legal_form_id",
                "jurisdiction",
                "legal_address_street",
                "legal_address_street_number",
                "legal_address_zip_code",
                "legal_address_city",
                "legal_address_country",
                "headquarter_address_street",
                "headquarter_address_street_number",
                "headquarter_address_city",
                "headquarter_address_zip_code",
                "headquarter_address_country",
                # "status",
                # "creation_date",
                # "expiration_date",
                # "expiration_reason",
                # "registration_date",
                # "registration_status",
                "id",
            ],
            returning="ALL_COLUMNS",
            conflict={
                "target": ["id"],
                "action": [{"column": "is_active", "value": False}],
            },
        )
        print(add)
