import polars as pl
from shared.clients.duck.client import duck
from shared.clients.api.eod.client import EodHistoricalDataApiClient
from shared.loggers import logger, events as logger_events
from shared.jobs.surrogate_keys.jobs import map_surrogate_keys
from shared.utils.dataset.validate import ValidateDataset

ASSET_SOURCE = EodHistoricalDataApiClient.client_key


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

    exchange_list = (
        ValidateDataset(
            dataset=joined,
            options={"dataset_name": "ExtractEodExchangeCodes", "job_name": "GeneralMapping", "product": "exchange"},
        )
        .is_not_null(column="source_value", id_column=["mic", "name"], message="No exchange code mapping found.")
        .return_dataset()["source_value"]
        .to_list()
    )
    return exchange_list[:20]


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


def transform_pre_figi(data: pl.DataFrame):
    """
    Apply the following transformations:
    - Map security_type_id
    - Map exchange_uid
    """

    def validate_unique_exchange(data: pl.DataFrame):
        exchange_uids = list(data["exchange_uid"].unique())
        if len(exchange_uids) > 1:
            logger.transform.warn(
                "Multiple exchanges for securities detected",
                event=logger_events.transform.MultipleRecords(),
                extra={"exchanges": exchange_uids},
            )

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

    # validation
    validate_unique_exchange(data=data)

    return (
        ValidateDataset(
            dataset=data,
            options={"dataset_name": "TransformEodSecurity", "job_name": "GeneralMapping", "product": "security"},
        )
        .is_not_null(
            column="security_type_id",
            id_column=["ticker", "security_type", "security_type_id"],
            message="Misisng security_type_id mapping.",
        )
        .return_dataset()
    )


def map_figi(data: pl.DataFrame):
    """
    Map figi to securities.
    """

    from dags.security.open_figi.jobs import map_figi_to_security

    mapped = map_figi_to_security(data)

    return mapped


def transform_post_figi(data: pl.DataFrame):
    """
    Additional transformation which adds
    - security_uid depending on share class or composite figi
    - flag if security_uid is share class or composite
    - security_ticker_uid (combination of security_uid and ticker_figi)
    - surrogate key on security level
    """

    from shared.clients.db.postgres.repositories import DbQueryRepositories

    security = duck.query(
        "./sql/transform_post_figi.sql",
        security=data,
        exchange_mapping=DbQueryRepositories.mappings.get_mappings(
            field="exchange_code", product="exchange", source="OpenFigi"
        ),
    ).pl()

    security = (
        ValidateDataset(
            dataset=security,
            options={
                "dataset_name": "TransformSecurityPostFigi",
                "job_name": "TransformSecurityPostFigi",
                "product": "security",
            },
        )
        .is_not_null(
            column="security_uid", id_column=["ticker", "name_figi", "security_uid"], message="No security uid mapped."
        )
        .is_not_null(column="exchange_uid", id_column=["exchange_code_figi"], message="No exchange uid mapped.")
        .return_dataset()
    )

    # security
    security = map_surrogate_keys(
        data=security, product="security", uid_col_name="security_uid", id_col_name="security_id"
    )

    return security


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

    # entity isin
    security = map_surrogate_keys(
        data=security,
        product="entity_isin",
        uid_col_name="isin",
        id_col_name="entity_isin_id",
        add_missing_keys=False,
        optional=True,
    )

    logger.job.info(event=logger_events.job.Success(job=JOB_NAME))

    return security


def extract_security_ticker(security_ticker: pl.DataFrame):
    """
    Extract unique security tickers from mapping results.
    """

    security_ticker = duck.query(
        "./sql/aggregate_security_ticker.sql",
        security_ticker=security_ticker,
    ).pl()

    security_ticker = map_surrogate_keys(
        data=security_ticker,
        product="security_ticker",
        uid_col_name="security_ticker_uid",
        id_col_name="id",
    )

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

    # security_ticker key
    security_listing = map_surrogate_keys(
        data=security_listing,
        product="security_ticker",
        uid_col_name="security_ticker_uid",
        id_col_name="security_ticker_id",
    )

    # security listing key
    security_listing = map_surrogate_keys(
        data=security_listing, product="security_listing", uid_col_name="figi", id_col_name="id"
    )

    # exchange key
    security_listing = map_surrogate_keys(
        data=security_listing,
        product="exchange",
        uid_col_name="exchange_uid",
        id_col_name="exchange_id",
        add_missing_keys=False,
    )

    return security_listing


def load_security_into_database(security: pl.DataFrame):
    """
    Add security records to database.
    """
    JOB_NAME = "LoadEodSecurityIntoDatabase"
    logger.job.info(event=logger_events.job.Init(job=JOB_NAME))


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
