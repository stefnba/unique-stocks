import polars as pl
from shared.clients.duck.client import duck
from shared.clients.api.eod.client import EodHistoricalDataApiClient

ASSET_SOURCE = EodHistoricalDataApiClient.client_key
from shared.loggers import logger, events as logger_events


def extract():
    """
    Extracts and provides exchanges codes from database.
    """
    JOB_NAME = "ExtractEodExchangeCodes"

    logger.job.info(event=logger_events.job.Init(job=JOB_NAME))

    from shared.clients.db.postgres.repositories import DbQueryRepositories

    exchanges = DbQueryRepositories.exchange.find_all(source=ASSET_SOURCE)

    exchange_mapping = DbQueryRepositories.mappings.get_mappings(
        source=ASSET_SOURCE, product="exchange", field="exchange_code"
    )

    joined = exchanges.join(exchange_mapping[["source_value", "uid"]], left_on="mic", right_on="uid", how="left")

    # todo extract

    exchange_list = joined.filter(pl.col("source_value").is_not_null())["source_value"].to_list()
    logger.mapping.warn(
        event=logger_events.mapping.MissingRecords(job="GeneralMapping", product="exchange"),
        extra={"missing": joined.filter(pl.col("source_value").is_null())[["mic", "name"]].to_dicts()},
    )
    logger.job.info(event=logger_events.job.Success(job=JOB_NAME))
    # return ["OTCQX", "NASDAQ", "XETRA", "SW", "NYSE"]
    return exchange_list[:5]


def ingest(exchange_code: str):
    """
    Retrieves listed securities for a given exchange code.
    """
    JOB_NAME = "IngestEodSecurityForExchange"

    logger.job.info(event=logger_events.job.Init(job=JOB_NAME), extra={"exchange_code": exchange_code})

    securities = EodHistoricalDataApiClient.get_securities_listed_at_exchange(exhange_code=exchange_code)

    logger.job.info(
        event=logger_events.job.Init(job=JOB_NAME),
        extra={"exchange_code": exchange_code, "securities_count": len(securities)},
    )

    return securities


def transform(data: pl.DataFrame):
    """
    Apply the following transformations:
    - Map security_type_id
    - Map exchange_uid
    """

    JOB_NAME = "TransformEodSecurity"

    logger.job.info(event=logger_events.job.Init(job=JOB_NAME))

    from shared.clients.db.postgres.repositories import DbQueryRepositories
    from shared.clients.duck.client import duck

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
        logger.transform.warn(
            "Multiple exchanges detected",
            event=logger_events.transform.MultipleRecords(),
            extra={"exchanges": exchange_uids},
        )

    # check if security type is missing mapped security_type_id
    missing_security_type = data.filter(pl.col("security_type_id").is_null())
    if len(missing_security_type) > 0:
        logger.transform.error(
            msg="Misisng security_type_id mapping",
            event=logger_events.transform.MissingValue(job=JOB_NAME, data=missing_security_type.to_dicts()),
        )

    # success
    logger.job.info(event=logger_events.job.Success(job=JOB_NAME))

    return data.filter(pl.col("security_type_id").is_not_null())


def map_figi(data: pl.DataFrame):
    """
    Map figi to securities.
    """

    JOB_NAME = "MapFigiToEodSecurity"
    logger.job.info(event=logger_events.job.Init(job=JOB_NAME))

    from dags.security.open_figi.jobs import map_figi_to_securities

    mapped = map_figi_to_securities(data)
    logger.job.info(event=logger_events.job.Success(job=JOB_NAME))

    return mapped


def transform_post_figi(data: pl.DataFrame):
    """
    Additional transformation which adds
    - security_uid depending on share class or composite figi
    - flag if security_uid is share class or composite
    - security_ticker_uid
    """
    security = duck.query(
        "./sql/transform_post_figi.sql",
        security=data,
    ).pl()

    return security


def map_surrogate_key(data: pl.DataFrame):
    """ """
    JOB_NAME = "MapSurrogateKeyToEodSecurity"
    logger.job.info(event=logger_events.job.Init(job=JOB_NAME))

    from shared.jobs.surrogate_keys.jobs import map_surrogate_keys

    # security key
    data = map_surrogate_keys(data=data, product="security", uid_col_name="security_uid", id_col_name="security_id")

    # security_ticker key
    data = map_surrogate_keys(
        data=data, product="security_ticker", uid_col_name="security_ticker_uid", id_col_name="security_ticker_id"
    )

    # security listing key
    data = map_surrogate_keys(
        data=data, product="security_listing", uid_col_name="figi", id_col_name="security_listing_id"
    )

    # exchange key
    data = map_surrogate_keys(data=data, product="exchange", uid_col_name="exchange_mic", id_col_name="exchange_id")

    return data


def extract_security(security: pl.DataFrame):
    """
    Extract unique securities from mapping results.


    Unique securities have
    - unique ISIN (if available)
    - unique share class or composite FIGI
    - security type
    - cases have shown that same share class or composite FIGI can have different name, for this we have integrated name

    A unique security then can have multiple tickers and exchange listings, these
    are extracted with the get_security_ticker() and get_security_listing() methods.


    """

    JOB_NAME = "ExtractEodSecurity"
    logger.job.info(event=logger_events.job.Init(job=JOB_NAME))

    security = duck.query(
        "./sql/aggregate_security.sql",
        security=security,
    ).pl()

    logger.job.info(event=logger_events.job.Success(job=JOB_NAME))

    return security


def extract_security_ticker(security_ticker: pl.DataFrame):
    """
    Extract unique security tickers from mapping results.
    """
    JOB_NAME = "ExtractEodSecurityTicker"

    security_ticker = duck.query(
        "./sql/aggregate_security_ticker.sql",
        security_ticker=security_ticker,
    ).pl()

    logger.job.info(event=logger_events.job.Success(job=JOB_NAME))

    return security_ticker


def extract_security_listing(security_listing: pl.DataFrame):
    """
    Extract unique security listings from mapping results and save records to database.

    Security listings have relationship to an exchange.
    """
    JOB_NAME = "ExtractEodSecurityListing"
    logger.job.info(event=logger_events.job.Init(job=JOB_NAME))

    security_listing = duck.query(
        "./sql/aggregate_security_listing.sql",
        security_listing=security_listing,
    ).pl()

    logger.job.info(event=logger_events.job.Success(job=JOB_NAME))

    return security_listing


def load_security_into_database(security: pl.DataFrame):
    """
    Add security records to database.
    """
    JOB_NAME = "LoadEodSecurityIntoDatabase"
    logger.job.info(event=logger_events.job.Init(job=JOB_NAME))

    from shared.clients.db.postgres.repositories import DbQueryRepositories

    added = DbQueryRepositories.security.add(security)

    logger.job.info(event=logger_events.job.Success(job=JOB_NAME))

    return added


def load_security_ticker_into_database(security: pl.DataFrame):
    """
    Add security ticker records to database.
    """
    JOB_NAME = "LoadEodSecurityTickerIntoDatabase"
    logger.job.info(event=logger_events.job.Init(job=JOB_NAME))

    from shared.clients.db.postgres.repositories import DbQueryRepositories

    added = DbQueryRepositories.security_ticker.add(security)

    logger.job.info(event=logger_events.job.Success(job=JOB_NAME))

    return added


def load_security_listing_into_database(security: pl.DataFrame):
    """
    Add security listing records to database.
    """
    JOB_NAME = "LoadEodSecurityListingIntoDatabase"
    logger.job.info(event=logger_events.job.Init(job=JOB_NAME))

    from shared.clients.db.postgres.repositories import DbQueryRepositories

    added = DbQueryRepositories.security_listing.add(security)

    logger.job.info(event=logger_events.job.Success(job=JOB_NAME))

    return added
