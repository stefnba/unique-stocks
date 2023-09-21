# %%
import sys
import os

sys.path.append("..")
os.chdir("..")


# %%


from dags.security.eod_historical_data.jobs import extract

extract()


# %%

from dags.security.eod_historical_data.jobs import (
    ingest,
    transform,
    map_figi,
    extract_security,
    extract_security_listing,
    extract_security_ticker,
    load_security_into_database,
    load_security_listing_into_database,
    load_security_ticker_into_database,
    transform_post_figi,
    map_surrogate_key,
)
import polars as pl

codes = ["OTCQX", "NASDAQ", "XETRA", "SW", "NYSE", "BE"]
# codes = ["OTCQX"]
for code in codes:
    print(code)
    raw = ingest(code)
    transformed = transform(pl.DataFrame(raw))
    mapped = map_figi(transformed)

    transformed_post_figi = transform_post_figi(mapped)

    mapped_surrogate_key = map_surrogate_key(transformed_post_figi)

    extract_security_data = extract_security(mapped_surrogate_key)
    extract_security_ticker_data = extract_security_ticker(mapped_surrogate_key)
    extract_security_listing_data = extract_security_listing(mapped_surrogate_key)

    load_security_into_database(extract_security_data)
    load_security_ticker_into_database(extract_security_ticker_data)
    load_security_listing_into_database(extract_security_listing_data)

    print(f"Done with {code}")


print("Done")

# %%

import duckdb as duck

df = map_surrogate_key(transform_post_figi(mapped))


duck.sql(
    """
    --sql
    WITH cte AS (SELECT
        security_uid,
        FIRST(security_type_id) security_type_id,
        FIRST(security_id) id,
        FIRST(level_figi) level_figi,
        FIRST(name_figi) name_figi,
        LIST_DISTINCT(LIST(isin)) isin,
        LIST_DISTINCT(LIST(name_figi)) name_figi_alias
    FROM df
    GROUP BY security_uid)

    SELECT
        * EXCLUDE(isin, name_figi_alias),
        LIST_FILTER(name_figi_alias, x -> x <> name_figi) name_figi_alias,
        LIST_FILTER(isin, x -> x IS NOT NULL)[1] isin
    FROM cte
    ;
    """
).pl()

# %%

duck.sql(
    """

    SELECT
        security_ticker_id,
        security_id,
        ticker_figi AS ticker
    FROM df
    GROUP BY security_ticker_id, security_id, ticker_figi
    """
).pl()

# %%

duck.sql(
    """
    --sql
    WITH cte AS (SELECT
        security_listing_id,
        figi,
        LIST(exchange_id) exchange_id,
        LIST(currency) currency,
        LIST(quote_source) quote_source
    FROM df
    GROUP BY security_listing_id, figi)
    
    SELECT * EXCLUDE(exchange_id, currency, quote_source),
        LIST_FILTER(exchange_id, x -> x IS NOT NULL)[1] exchange_id,
        LIST_FILTER(currency, x -> x IS NOT NULL)[1] currency,
        LIST_FILTER(quote_source, x -> x IS NOT NULL)[1] quote_source,
    FROM cte
    ;
    """
)


# BBG00BR8QSH0

# %%

extract_security_listing(mapped).filter(pl.col("id") == 14224).to_pandas()
# AH

# %%

transformed.filter(pl.col("ticker") == "PKKW")


# %%

mapped.filter(pl.col("share_class_figi") == "BBG001S67TP5")
