import polars as pl
from pydantic import ValidationError
from shared.clients.api.open_figi.client import OpenFigiApiClient
from shared.clients.api.open_figi.schema import FigiResult
from shared.clients.db.postgres.repositories import DbQueryRepositories
from shared.clients.duck.client import duck
from shared.jobs.figi.mapping.types import SecurityData
from shared.loggers import logger


def get_security_type_mapping():
    df = DbQueryRepositories.security_type.find_all()
    df = df.filter(pl.col("market_sector_figi").is_not_null())
    df = df.with_columns(pl.col("id").cast(pl.Int64))
    return df[["id", "name", "market_sector_figi"]]


def get_exchange_mic_mapping():
    df = DbQueryRepositories.exchange.mic_operating_mic_mapping()
    df = df.with_columns(pl.col("operating_mic").alias("exchange_operating_mic"))
    return df[["mic", "exchange_operating_mic"]]


def get_exchange_mapping():
    """
    Retrieves OpenFigi mapping for exchange codes and performs transformations.
    """
    df = DbQueryRepositories.mappings.get_mappings(source="OpenFigi", product="exchange", field="exchange_code")
    df = df.with_columns(pl.col("uid").alias("exchange_mic"))
    return df[["exchange_mic", "source_value"]]


def build_mapping_records(securities_data: list[dict]):
    """
    Prepares list of securities for FIGI API. Iterates over every security and adds idType
    and idValue field.

    Args:
        securities_data (list[dict]): List of securities to be mapped with FIGI.

    Returns:
        _type_: _description_
    """

    ID_FIELD = "idType"
    VALUE_FIELD = "idValue"

    mappings = []

    for data_item in securities_data:
        record_figi = {}

        # isin priority #1
        if data_item.get("isin", None):
            record_figi[ID_FIELD] = "ID_ISIN"
            record_figi[VALUE_FIELD] = data_item["isin"]

        # ticker symbol, i.e. code as fallback
        elif data_item.get("ticker", None):
            record_figi[ID_FIELD] = "TICKER"
            record_figi[VALUE_FIELD] = data_item["ticker"]

        else:
            # todo logging
            print("no", data_item)
            continue

        if data_item.get("market_sector_figi", None):
            record_figi["marketSecDes"] = data_item["market_sector_figi"]

        mappings.append(record_figi)

    return mappings


def map_securities_to_figi(securities: list, figi_results: list):
    """
    Combine batch of securites with results return from FIGI API.


    Args:
        figi_mapping_items (list): _description_
        securities (list): _description_

    Raises:
        Exception: _description_

    Returns:
        _type_: _description_
    """

    # get mappings
    exchange_mic_operating_mapping = get_exchange_mic_mapping()
    figi_exchange_mapping = get_exchange_mapping()

    # check both lists have same length
    if len(securities) is not len(figi_results):
        logger.mapping.error(
            "OpenFigi API results and submitted securities have different number of records.",
            event=logger.mapping.events.DIFFERENT_SIZE,
            extra={
                "length": {
                    "securities": len(securities),
                    "figi_results": len(figi_results),
                },
                "data": {"figi_results": figi_results, "securities": securities},
            },
        )
        raise Exception("OpenFigi API results and submitted securities have different number of records.")

    mappings = []

    for idx, figi_result in enumerate(figi_results):
        security_data = securities[idx]

        # no result found with FIGI API
        if "data" not in figi_result:
            logger.mapping.warning(
                "No match for FIGI mapping",
                event=logger.mapping.events.NO_MATCH,
                extra={"data": security_data},
                mapper="OpenFigi",
            )

        # result found
        else:
            figi_security_results = figi_result["data"]

            data = []

            for figi_security_result in figi_security_results:
                try:
                    figi_result = FigiResult(**figi_security_result).dict(by_alias=False)
                except ValidationError:
                    logger.mapping.info(extra={"result": figi_security_result, "security": security_data})
                    pass

                data.append({**SecurityData(**security_data).dict(), **figi_result})

            # map figi exchange code to mic (not operating mic)
            df = (
                pl.DataFrame(data)
                .join(
                    figi_exchange_mapping,
                    left_on="exchange_code_figi",
                    right_on="source_value",
                )
                .join(exchange_mic_operating_mapping, how="left", left_on="exchange_mic", right_on="mic")
            )
            # check if source has quote for this figi
            df = df.with_columns(
                pl.when(
                    (pl.col("ticker") == pl.col("ticker_figi"))
                    & (
                        (pl.col("exchange_mic") == pl.col("source_exchange_uid"))
                        | (pl.col("exchange_operating_mic") == pl.col("source_exchange_uid"))
                    )
                )
                .then(pl.col("source"))
                .otherwise(None)
                .alias("quote_source")
            )
            # add also currency to mapping if a source has been identified
            df = df.with_columns(
                pl.when(pl.col("quote_source").is_not_null())
                .then(pl.lit(security_data.get("currency", None)))
                .otherwise(None)
                .alias("currency")
            )

            data = df.to_dicts()

            # add to mappings list
            if len(data) > 0:
                mappings.extend(data)

    # save in figi table db
    if len(mappings) > 0:
        added = DbQueryRepositories.mapping_figi.add(mappings)
        if len(added) > 0:
            logger.mapping.info(f"{len(added)} mappings added to db")

    return mappings


def map_existing_figi(securities_data: pl.DataFrame) -> pl.DataFrame:
    """
    Join existing figi mappping in database to securities_data.
    """
    return duck.query(
        "./sql/map_existing_figi.sql",
        securities=securities_data,
        mapping_figi=DbQueryRepositories.mapping_figi.find_all(),
    ).pl()


def map_figi_to_securities(securities_data: pl.DataFrame):
    """
    Starts FIGI mapping process. Slices securities_data into batch so that OpenFigi's rate limits are met.
    To reduce API calls, securities are mapped against existing records in database, only non-matches will be added
    via OpenFigi's API.

    Args:
        securities_data (pl.DataFrame): Transformed list of securities.
    """
    BATCH_SIZE = 80
    SCHEMA = {
        "ticker": pl.Utf8,
        "isin": pl.Utf8,
        "name": pl.Utf8,
        "security_type": pl.Utf8,
        "security_type_id": pl.Int64,
        "country": pl.Utf8,
        "currency": pl.Utf8,
        "exchange_source_code": pl.Utf8,
        "exchange_uid": pl.Utf8,
        "source": pl.Utf8,
    }

    logger.mapping.info(f"{len(securities_data)} to be mapped.", extra={"count": len(securities_data)})

    # ensure correct data types
    securities_data = pl.DataFrame(
        securities_data.to_dicts(),
        schema=SCHEMA,
    )

    results: list[dict] = []

    # map figi already existing in database
    # securities_with_existing_figi_mapped = open_figi_jobs.map_existing_figi(securities_data)

    # return securities_with_existing_figi_mapped

    # logger.mapping.info(f"{len(results)} already mapped via database")

    # continue with those w/o match
    # securities_data = pl.DataFrame(
    #     securities_with_existing_figi_mapped.filter(pl.col("figi").is_null()).to_dicts(), schema=SCHEMA
    # )
    # securities_with_existing_figi = pl.DataFrame(
    #     securities_with_existing_figi_mapped.filter(pl.col("figi").is_not_null()).to_dicts(), schema=SCHEMA
    # )

    # logger.mapping.info(
    #     extra={
    #         "new_mapping_count": len(securities_data),
    #         "existing_mapping_count": len(securities_with_existing_figi),
    #     }
    # )

    # return securities_data

    # map market_sector_figi to all securities
    # securities_data = securities_data.with_columns(pl.col("security_type_id").cast(pl.Int64))
    security_type_mapping = get_security_type_mapping()
    securities_data = securities_data.join(security_type_mapping, how="left", left_on="security_type_id", right_on="id")

    securities_dict = securities_data.to_dicts()

    logger.mapping.info(f"length {len(securities_dict)}")

    batches = [securities_dict[i : i + BATCH_SIZE] for i in range(0, len(securities_dict), BATCH_SIZE)]

    for batch in batches:
        records = build_mapping_records(batch)
        # call api
        figi_results = OpenFigiApiClient.get_mapping(records)
        mapped = map_securities_to_figi(securities=batch, figi_results=figi_results)

        results.extend(mapped)

    # add previous matches to results
    # results.extend(securities_with_existing_figi.filter(pl.col("figi").is_not_null()).to_dicts())

    return pl.DataFrame(
        results,
        schema={
            "ticker_figi": pl.Utf8,
            "isin": pl.Utf8,
            "name_figi": pl.Utf8,
            "figi": pl.Utf8,
            "composite_figi": pl.Utf8,
            "share_class_figi": pl.Utf8,
            "security_type_id": pl.Int64,
            "exchange_mic": pl.Utf8,
            "quote_source": pl.Utf8,
            "currency": pl.Utf8,
        },
    )
