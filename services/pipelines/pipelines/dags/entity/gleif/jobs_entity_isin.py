import polars as pl
from shared.clients.api.gleif.client import GleifApiClient
from shared.loggers import logger

ASSET_SOURCE = GleifApiClient.client_key


def transform(data: pl.LazyFrame):
    from shared.jobs.surrogate_keys.jobs import map_surrogate_keys

    data = data.rename({"LEI": "lei", "ISIN": "isin"})

    # isin
    data = map_surrogate_keys(data=data, product="entity_isin", uid_col_name="isin", collect=False)

    # lei
    data = map_surrogate_keys(
        data=data, product="entity", uid_col_name="lei", id_col_name="entity_id", add_missing_keys=False, collect=False
    )

    return data
