import polars as pl
from shared.clients.api.eod.client import EodHistoricalDataApiClient

ASSET_SOURCE = EodHistoricalDataApiClient.client_key


def get_exchange_codes():
    return ["NASDAQ", "XETRA"]


def ingest(exchange_code: str):
    """
    Retrieves listed securities for a given exchange code.
    """
    from shared.clients.api.eod.client import EodHistoricalDataApiClient

    return EodHistoricalDataApiClient.get_securities_listed_at_exchange(exhange_code=exchange_code)


def transform(data: pl.DataFrame):
    """
    Apply the following transformations:
    - Map security_type_id
    - Map exchange_uid
    """

    import polars as pl
    from shared.clients.db.postgres.repositories import DbQueryRepositories
    from shared.clients.duck.client import duck
    from shared.loggers import logger

    # some exchange like CC or MONEY don't have ISIN column, so needs to be added
    if "ISIN" not in data.columns:
        data = data.with_columns(pl.lit(None).alias("ISIN"))

    data = duck.query(
        "./sql/transform_raw_securities.sql",
        securities=data,
        security_type_mapping=DbQueryRepositories.mappings.get_mappings(
            source=ASSET_SOURCE, product="security", field="security_type"
        ),
        country_mapping=DbQueryRepositories.mappings.get_mappings(source=ASSET_SOURCE, product="country"),
        exchange_mapping=DbQueryRepositories.mappings.get_mappings(
            source=ASSET_SOURCE, product="exchange", field="exchange_code"
        ),
        source=ASSET_SOURCE,
    ).pl()

    exchange_uids = list(data["exchange_uid"].unique())

    if len(exchange_uids) > 1:
        logger.transform.warning(
            "Multiple exchanges detected",
            event=logger.transform.events.MULTIPLE_RECORDS,
            extra={"exchanges": exchange_uids},
        )

    logger.transform.info(event=logger.transform.events.SUCCESS)

    return map_figi(data)


def map_figi(data: pl.DataFrame):
    """map figi to securities"""
    from dags.security.open_figi.jobs import map_figi_to_securities

    return map_figi_to_securities(data)


def extract_security(security: pl.DataFrame):
    """
    Extract unique securities from mapping results and save records to database.


    Unique securities have
    - unique ISIN (if available)
    - unique share class or composite FIGI
    - security type

    A unique security then can have multiple tickers and exchange listings, these
    are extracted with the get_security_ticker() and get_security_listing() methods.


    """
    from shared.clients.db.postgres.repositories import DbQueryRepositories
    from shared.jobs.surrogate_keys.jobs import map_surrogate_keys

    # since some securities don't have share class figi, we must replace those null with composite figi
    security = security.with_columns(
        pl.when(pl.col("share_class_figi").is_null())
        .then(pl.col("composite_figi"))
        .otherwise(pl.col("share_class_figi"))
        .alias("security_uid")
    )

    security = security[["isin", "security_uid", "name_figi", "security_type_id"]].unique()

    # security id based on figi, either share class or composite
    security = map_surrogate_keys(data=security, product="security", uid_col_name="security_uid")

    # save to db
    DbQueryRepositories.security.add(security)

    return security


def extract_security_ticker(security_ticker: pl.DataFrame):
    """
    Extract unique security tickers from mapping results and save records to database.
    """
    from shared.clients.db.postgres.repositories import DbQueryRepositories
    from shared.jobs.surrogate_keys.jobs import map_surrogate_keys

    # since some securities don't have share class figi, we must replace those null with composite figi
    security_ticker = security_ticker.with_columns(
        pl.when(pl.col("share_class_figi").is_null())
        .then(pl.col("composite_figi"))
        .otherwise(pl.col("share_class_figi"))
        .alias("security_uid")
    )

    security_ticker = security_ticker[["ticker_figi", "security_uid"]].unique()

    # foreign key to security
    security_ticker = map_surrogate_keys(
        data=security_ticker, product="security", uid_col_name="security_uid", id_col_name="security_id"
    )

    # ticker id based on ticker_figi
    security_ticker = map_surrogate_keys(
        data=security_ticker, product="security_ticker", uid_col_name="ticker_figi", id_col_name="id"
    )

    # save to db
    DbQueryRepositories.security_ticker.add(security_ticker)

    return security_ticker


def extract_security_listing(security_listing: pl.DataFrame):
    """
    Extract unique security listings from mapping results and save records to database.

    Security listings have relationship to an exchange.
    """
    from shared.clients.db.postgres.repositories import DbQueryRepositories
    from shared.jobs.surrogate_keys.jobs import map_surrogate_keys

    security_listing = security_listing[["ticker_figi", "exchange_mic", "figi", "quote_source", "currency"]]

    # foreign key to security_ticker
    security_listing = map_surrogate_keys(
        data=security_listing,
        product="security_ticker",
        uid_col_name="ticker_figi",
        id_col_name="security_ticker_id",
    )

    # foreign key to exchange
    security_listing = map_surrogate_keys(
        data=security_listing, product="exchange", uid_col_name="exchange_mic", id_col_name="exchange_id"
    )

    # id based on figi
    security_listing = map_surrogate_keys(
        data=security_listing, product="security_listing", uid_col_name="figi", id_col_name="id"
    )

    # save to db
    DbQueryRepositories.security_listing.add(security_listing)

    return security_listing
