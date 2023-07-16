import polars as pl
from shared.clients.api.gleif.client import GleifApiClient
from shared.loggers import logger

ASSET_SOURCE = GleifApiClient.client_key


def transform(df: pl.DataFrame):
    from shared.jobs.surrogate_keys.jobs import map_surrogate_keys

    df = df.rename({"LEI": "lei", "ISIN": "isin"})

    # isin
    data = map_surrogate_keys(data=df, product="entity_isin", uid_col_name="isin")

    # lei
    data = map_surrogate_keys(data=data, product="entity", uid_col_name="lei", id_col_name="entity_id")

    return data
