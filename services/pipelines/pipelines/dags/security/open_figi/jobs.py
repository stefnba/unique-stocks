import polars as pl
from dags.security.open_figi.types import MappingPrep
from pydantic import ValidationError
from shared.clients.api.open_figi.client import OpenFigiApiClient
from shared.clients.api.open_figi.schema import FigiResult
from shared.clients.api.open_figi.types import MappingArgs
from shared.clients.db.postgres.repositories import DbQueryRepositories
from shared.config import CONFIG
from shared.loggers import logger
from shared.utils.conversion.converter import model_to_polars_schema


def get_security_type_mapping():
    df = DbQueryRepositories.security_type.find_all()
    df = df.filter(pl.col("market_sector_figi").is_not_null())
    df = df.with_columns(pl.col("id").cast(pl.Int64))
    return df[["id", "name", "market_sector_figi"]]


def get_exchange_mic_mapping():
    df = DbQueryRepositories.exchange.mic_operating_mic_mapping()
    df = df.with_columns(pl.col("operating_mic").alias("ex_mp_operating_mic"))
    df = df.with_columns(pl.col("mic").alias("ex_mp_mic"))
    return df[["ex_mp_operating_mic", "ex_mp_mic"]]


def get_exchange_mapping():
    """
    Retrieves OpenFigi mapping for exchange codes and performs transformations.
    """
    df = DbQueryRepositories.mappings.get_mappings(source="OpenFigi", product="exchange", field="exchange_code")
    df = df.with_columns(pl.col("uid").alias("exchange_mic_figi"))
    df = df.with_columns(pl.col("source_value").alias("exchange_code_figi"))
    return df[["exchange_mic_figi", "exchange_code_figi"]]


def post_processing(securities_data: pl.DataFrame) -> pl.DataFrame:
    """

    - Determine if quote_source is available
    - Determine if currency is available (only if quote_source is available)
    """

    securities_data = securities_data.join(
        get_exchange_mic_mapping(), how="left", left_on="exchange_mic_figi", right_on="ex_mp_mic"
    )

    # check if source has quote for this figi
    securities_data = securities_data.with_columns(
        pl.when(
            (pl.col("ticker") == pl.col("ticker_figi"))
            & (
                (pl.col("exchange_mic_figi") == pl.col("exchange_uid"))
                | (pl.col("ex_mp_operating_mic") == pl.col("exchange_uid"))
            )
        )
        .then(pl.col("source"))
        .otherwise(None)
        .alias("quote_source")
    )
    # add also currency to mapping if a source has been identified
    securities_data = securities_data.with_columns(
        pl.when(pl.col("quote_source").is_not_null()).then(pl.col("currency")).otherwise(None).alias("currency")
    )

    # make exchange_mic_figi to mic
    securities_data = securities_data.with_columns(pl.col("exchange_mic_figi").alias("exchange_mic"))

    return securities_data[
        [
            "ticker",
            "ticker_figi",
            "isin",
            "security_type_id",
            "name_figi",
            "currency",
            "figi",
            "composite_figi",
            "share_class_figi",
            "exchange_mic",
            "quote_source",
        ]
    ]


def build_mapping_records(securities_data: list[dict]):
    """
    Prepares list of securities for FIGI API. Iterates over every security and adds idType
    and idValue field.

    Args:
        securities_data (list[dict]): List of securities to be mapped with FIGI.

    Returns:
        _type_: _description_
    """
    mappings: list[MappingArgs] = []

    for data_item in securities_data:
        record_figi: MappingArgs

        # isin priority #1
        if data_item.get("isin", None):
            record_figi = {"idType": "ID_ISIN", "idValue": data_item["isin"]}

        # ticker symbol, i.e. code as fallback
        elif data_item.get("ticker", None):
            record_figi = {"idType": "TICKER", "idValue": data_item["ticker"]}

        else:
            # todo logging
            print("no", data_item)
            continue

        if data_item.get("market_sector_figi", None):
            record_figi["marketSecDes"] = data_item["market_sector_figi"]

        mappings.append(record_figi)

    logger.mapping.info(f"Mapping length: {len(mappings)}")
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
    no_matches = []  # for second attempt

    for idx, figi_result in enumerate(figi_results):
        security_data = securities[idx]

        # no result found with FIGI API
        if "data" not in figi_result:
            # second attempt w/o isin but ticker instead
            if security_data.get("isin", None):
                security_data["isin"] = None
                no_matches.append(security_data)

            logger.mapping.warning(
                "No match for FIGI mapping",
                event=logger.mapping.events.NO_MATCH,
                extra={"data": security_data},
                mapper="OpenFigi",
            )

        # result found
        else:
            figi_security_results = figi_result["data"]

            figi_results_mapped_with_securities = []

            for figi_security_result in figi_security_results:
                try:
                    figi_result = FigiResult(**figi_security_result).dict(by_alias=False)

                except ValidationError as err:
                    logger.mapping.error(str(err), extra={"result": figi_security_result, "security": security_data})
                    continue

                figi_results_mapped_with_securities.append({**security_data, **figi_result})

            df = pl.DataFrame(
                figi_results_mapped_with_securities, schema={**model_to_polars_schema(FigiResult), **SECURITY_SCHEMA}
            )

            df_missing_exchange = df.filter(pl.col("exchange_code_figi").is_null())
            df = df.filter(pl.col("exchange_code_figi").is_not_null())

            # exclude composite exchanges

            # df = df.filter((~pl.col("exchange_code_figi").is_in(CONFIG.data.exchanges.composite_exchanges)))

            # map figi exchange code to mic (not operating mic)
            df = df.join(
                get_exchange_mapping(),
                how="left",
                on="exchange_code_figi",
            )

            # warn about missing exchange_code_figi to mic mappings
            missing_figi_exchange_mapping = df.filter(pl.col("exchange_mic_figi").is_null())
            if len(missing_figi_exchange_mapping) > 0:
                logger.mapping.warning(
                    "Missing exchange_code_figi to mic mappings",
                    event=logger.mapping.events.NO_MATCH,
                    extra={"codes": list(missing_figi_exchange_mapping["exchange_code_figi"].unique())},
                )

            # only proceed with records that have exchange code mapping to mic
            df = df.filter(pl.col("exchange_mic_figi").is_not_null())

            data = df.to_dicts()

            # add to mappings list
            if len(data) > 0:
                mappings.extend(data)

    # second attempt
    if len(no_matches) > 0:
        second_attempt = process_batches(no_matches)
        if len(second_attempt) > 0:
            logger.mapping.info("Matches from second attemp w/o ISIN", extra={"securities": second_attempt})
            mappings.extend(second_attempt)

    # save in figi table db
    if len(mappings) > 0:
        logger.mapping.info(f"{len(mappings)} FOUND")
        added = DbQueryRepositories.mapping_figi.add(mappings)
        if len(added) > 0:
            logger.mapping.info(f"{len(added)} mappings added to db")
        elif len(mappings) > 0 and len(added) == 0:
            logger.mapping.info(
                f"{len(mappings)} mappings found but not added to database", extra={"securites": mappings}
            )

    return mappings


def process_batches(securities_data: list[dict]):
    BATCH_SIZE = 50

    batches = [securities_data[i : i + BATCH_SIZE] for i in range(0, len(securities_data), BATCH_SIZE)]

    results: list[dict] = []

    for batch in batches:
        logger.mapping.info(f"Batch size {len(batch)}")
        records = build_mapping_records(batch)

        # call api
        figi_results = OpenFigiApiClient.get_mapping(records)
        logger.mapping.info(f"Figi results length: {len(figi_results)}")

        # map figi results to securities
        mapped = map_securities_to_figi(securities=batch, figi_results=figi_results)
        if len(mapped) > 0:
            results.extend(mapped)

    logger.mapping.info(f"Results: {len(results)}")

    return results


SECURITY_SCHEMA = {
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

FIGI_RESULT_SCHEMA = {
    "figi": pl.Utf8,
    "share_class_figi": pl.Utf8,
    "composite_figi": pl.Utf8,
    "exchange_mic_figi": pl.Utf8,
    "ticker_figi": pl.Utf8,
    "name_figi": pl.Utf8,
    # "isin": pl.Utf8,
    # "security_type_id": pl.Int64,
    # "quote_source": pl.Utf8,
    # "currency": pl.Utf8,
}


def prepare_processing(securities_data: pl.DataFrame) -> MappingPrep:
    cols = ["figi", "share_class_figi", "composite_figi", "exchange_mic_figi", "ticker_figi", "name_figi"]

    # ensure correct data types
    securities_data = pl.DataFrame(
        securities_data.to_dicts(),
        schema=SECURITY_SCHEMA,
    )

    existing_figi = DbQueryRepositories.mapping_figi.find_all()

    # existing mapping based on isin
    tmp_df = securities_data.join(
        existing_figi.filter(pl.col("isin").is_not_null())[["isin", *cols]],
        how="left",
        on="isin",
    )
    existing_matches_isin = tmp_df.filter(pl.col("figi").is_not_null())

    logger.mapping.info(f"{len(existing_matches_isin['ticker'].unique())} matched with existing records based on ISIN")

    tmp_df = tmp_df.filter(pl.col("figi").is_null())

    # existing mapping based on ticker
    tmp_df = tmp_df.drop(cols).join(
        existing_figi.filter(pl.col("ticker").is_not_null())[["ticker", *cols]],
        # existing_figi.filter(pl.col("ticker").is_not_null())[["ticker", "figi", "share_class_figi", "composite_figi"]],
        how="left",
        on="ticker",
    )
    existing_matches_ticker = tmp_df.filter(pl.col("figi").is_not_null())

    logger.mapping.info(
        f"{len(existing_matches_ticker['ticker'].unique())} matched with existing records based on Ticker"
    )

    # missing mappings
    missing_mappings = tmp_df.filter(pl.col("figi").is_null()).drop(cols)

    logger.mapping.info(f"{len(missing_mappings['ticker'].unique())} not matched with existing records.")

    return {
        "missing": missing_mappings,
        "existing": pl.concat([existing_matches_isin, existing_matches_ticker], rechunk=True),
    }


def map_figi_to_securities(securities_data: pl.DataFrame):
    """
    Starts FIGI mapping process. Slices securities_data into batch so that OpenFigi's rate limits are met.
    To reduce API calls, securities are mapped against existing records in database, only non-matches will be added
    via OpenFigi's API.

    Args:
        securities_data (pl.DataFrame): Transformed list of securities.
    """

    logger.mapping.info(f"{len(securities_data)} to be mapped with OpenFigi", extra={"count": len(securities_data)})

    prep_data = prepare_processing(securities_data)

    # continued with missing securities data
    securities_data = prep_data["missing"]
    securities_data = securities_data.join(
        get_security_type_mapping(), how="left", left_on="security_type_id", right_on="id"
    )
    logger.mapping.info(f"Unique tickers {len(securities_data['ticker'].unique())}")

    # main processing of missing mappings
    results = process_batches(securities_data.to_dicts())

    return post_processing(
        pl.concat(
            [
                pl.DataFrame(
                    results,
                    schema={**SECURITY_SCHEMA, **FIGI_RESULT_SCHEMA},
                ),
                prep_data["existing"],
            ],
            rechunk=True,
        )
    )
