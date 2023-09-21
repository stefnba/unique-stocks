# %%
# ruff: noqa: E402

import sys
import os

sys.path.append("..")
os.chdir("..")


# %%

import polars as pl
from dags.security.eod_historical_data.jobs import (
    ingest,
    transform_pre_figi,
    map_figi,
    transform_post_figi,
    extract_security,
    load_security_into_database,
    extract,
    extract_security_ticker,
    load_security_ticker_into_database,
    extract_security_listing,
    load_security_listing_into_database,
)

codes = extract()
# codes = ["XETRA"]

print(codes)
# codes = ["NASDAQ"]


for code in codes:
    raw_data = pl.DataFrame(ingest(code))
    transformed = transform_pre_figi(raw_data)
    mapped = map_figi(transformed)
    post_figi = transform_post_figi(mapped)
    security = extract_security(post_figi)
    load_security_into_database(security)

    security_ticker = extract_security_ticker(post_figi)
    load_security_ticker_into_database(security_ticker)

    security_listing = extract_security_listing(post_figi)

    load_security_listing_into_database(security_listing)

print("Done")
print(codes)

# %%

load_security_into_database(security)

security_ticker = extract_security_ticker(post_figi)
load_security_ticker_into_database(security_ticker)

security_listing = extract_security_listing(post_figi)

load_security_listing_into_database(security_listing)


# %%

mapped = map_figi(transformed.head(500))
post_figi = transform_post_figi(mapped)
security = extract_security(post_figi)

# %%
mapped.filter(pl.col("isin") == "DE000A1NZLR7")


# %%
from dags.security.eod_historical_data.jobs import extract_security_listing, load_security_listing_into_database

post_figi = transform_post_figi(mapped)


# %%

# mapped.filter(pl.col("figi") == "BBG00VTHN4L5")
post_figi.filter(pl.col("isin") == "CH0454664001")


# %%

load_security_into_database(security)

# %%

# load_security_into_database(security)

# from dags.security.eod_historical_data.jobs import extract_security_ticker, load_security_ticker_into_database
# post_figi = transform_post_figi(mapped)

#
# mapped.filter(pl.col("share_class_figi") == "BBG00VY1KBC1")
security.filter(pl.col("figi") == "BBG001S5ZJM1")
# %%
post_figi = transform_post_figi(mapped)
security = extract_security(post_figi)
load_security_into_database(security)

security_ticker = extract_security_ticker(post_figi)
load_security_ticker_into_database(security_ticker)

# %%
from shared.clients.api.open_figi.client import OpenFigiApiClient

OpenFigiApiClient.get_mapping([{"idType": "ID_ISIN", "idValue": "US0028241000"}]).explode(columns="data").unnest("data")


# .to_dicts()

# %%


import duckdb

duckdb.sql(
    """
    --sql
    SELECT
        DISTINCT name_figi
        --security_id, isin, security_type_id, name_figi, security_uid AS id
    FROM mapped
    WHERE isin = 'US0028241000'
    --GROUP BY security_id, isin, security_type_id, name_figi, security_uid
    ;
    """
).pl().to_series().to_list()

# %%

duckdb.sql(
    """
    --sql
    WITH security_level AS (SELECT
        security_id AS id,
        security_uid AS figi,
        FIRST (security_type_id) security_type_id,
        MAX(level_figi) level_figi,
        FIRST (name_figi) name_figi,
        LIST_DISTINCT(LIST (isin)) isin,
        LIST_DISTINCT(LIST (name_figi)) name_figi_alias
     FROM post_figi
     GROUP BY
        security_id,
        security_uid)

    SELECT
        *
    EXCLUDE (isin, name_figi_alias),
    LIST_FILTER(name_figi_alias, x -> x <> name_figi) name_figi_alias,
    LIST_FILTER(isin, x -> x IS NOT NULL)[1] isin
FROM
    security_level
        
    ;
    """
).pl()


# %%
