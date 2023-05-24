import io
import json

import polars as pl
from shared.clients.api.eod.client import EodHistoricalDataApiClient
from shared.clients.duck.client import duck

ApiClient = EodHistoricalDataApiClient
ASSET_SOURCE = ApiClient.client_key
INDEX_EXCHANGE_CODE = ApiClient.index_exhange_code


def ingest(index_code: str):
    return ApiClient.get_fundamentals(security_code=index_code, exchange_code=INDEX_EXCHANGE_CODE)


def transform(data: str):
    import logging

    data_dict = json.loads(data)

    members = list(data_dict["Components"].values())
    meta_info = data_dict["General"]
    index_code = meta_info.get("Code", None)

    # index has no members
    if len(members) == 0:
        logging.warning(f"{index_code} has no members")

        return pl.DataFrame()

    members_df = pl.from_dicts(members)

    members_df = members_df.with_columns(
        [
            pl.when(pl.col(pl.Utf8) == "Unknown").then(None).otherwise(pl.col(pl.Utf8)).keep_name(),
            pl.lit(ASSET_SOURCE).alias("data_source"),
        ]
    )

    return duck.query("./sql/transform.sql", members=members_df, source=ASSET_SOURCE, index_code=index_code).pl()
