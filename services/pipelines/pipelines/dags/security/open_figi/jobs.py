import polars as pl
from shared.clients.duck.client import duck
from shared.clients.db.postgres.repositories import DbQueryRepositories
from shared.clients.api.open_figi.client import OpenFigiApiClient
from shared.loggers.logger import mapping as logger
from shared.loggers.events import mapping as log_events
from dags.security.open_figi.types import MapResultDict
from shared.utils.dataset.validate import ValidateDataset

SLICE_LENGTH = 60
FIGI_COLUMNS = ["idValue", "idType"]

schema_response = {
    "figi": pl.Utf8,
    "name": pl.Utf8,
    "ticker": pl.Utf8,
    "exchCode": pl.Utf8,
    "compositeFIGI": pl.Utf8,
    "securityType": pl.Utf8,
    "marketSector": pl.Utf8,
    "shareClassFIGI": pl.Utf8,
    "securityType2": pl.Utf8,
    "securityDescription": pl.Utf8,
}

schema = {
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

security_columns = [
    "ticker",
    "isin",
    "name",
    "security_type",
    "security_type_id",
    "country",
    "currency",
    "exchange_source_code",
    "exchange_uid",
    "source",
]


def map_figi_to_security(data: pl.DataFrame) -> pl.DataFrame:
    """Map figi to security dataset"""

    total_length = len(data)
    logger.info(
        msg=f"Initiate OpenFigi mapping for {total_length} securities.",
        event=log_events.InitMapping(job="OpenFigi", size=total_length, product="security"),
    )

    # ensure correct schema
    data = data.with_columns([pl.col(name).cast(dtype).keep_name() for (name, dtype) in schema.items()])

    # check existing records
    map_result = _map_to_existing_records(df=data)

    # map and get missing mapping
    _map_and_transform(df=map_result["missing"])

    # get all with newly added records
    all = _map_to_existing_records(df=data)["matched"]

    # todo validate

    all_length = len(all["ticker"].unique())
    logger.info(
        msg=f"Successfully mapped {all_length} securities with figi.",
        event=log_events.MappingSuccess(job="OpenFigi", size=all_length, product="security"),
    )

    return all


def _map_to_existing_records(df: pl.DataFrame) -> MapResultDict:
    """Map dataset to existing records in database."""

    mapping = duck.query(
        "sql/map_to_existing_records.sql",
        data=df,
        mapping_figi=DbQueryRepositories.mapping_figi.find_all(),
    ).pl()

    missing = mapping.filter(pl.col("figi").is_null()).select(security_columns)
    missing_length = len(missing)
    missing_security = missing["ticker"].unique().to_list()
    matched = mapping.filter(pl.col("figi").is_not_null())
    matched_length = len(matched["ticker"].unique())

    logger.info(
        msg=f"Existing figi records matched {matched_length} security records. {missing_length} are missing.",
        event=log_events.MappingResult(matched=matched_length, missing=missing_length, job="OpenFigi"),
        extra={"missing_security": missing_security},
    )

    return {
        "matched": matched,
        "missing": missing,
    }


def _prepare_figi_body(df: pl.DataFrame) -> pl.DataFrame:
    """"""
    return duck.query(
        "sql/prepare_figi_body.sql",
        df=df,
    ).pl()


def _prepare_figi_body_second_attempt(df: pl.DataFrame):
    """Second attempt only builds records with ISIN that were not found and uses ticker instead."""
    return duck.query(
        "sql/prepare_figi_body_second_attempt.sql",
        df=df,
    ).pl()


def _map_and_transform(df: pl.DataFrame, attempt=1):
    total_length = len(df)

    if attempt == 1:
        df = _prepare_figi_body(df)
    if attempt == 2:
        df = _prepare_figi_body_second_attempt(df)

    for idx, frame in enumerate(df.iter_slices(n_rows=SLICE_LENGTH)):
        logger.info(
            msg=f"Iterating over {len(frame)} records for slice {idx}.",
            event=log_events.MappingIteration(job="OpenFigi", iteration=idx, size=total_length),
            extra={"slice_length": len(frame), "attempt": attempt},
        )

        figi_mapping = OpenFigiApiClient.get_mapping(frame.select(FIGI_COLUMNS).to_dicts())
        security_df = (
            frame.select(col for col in frame.columns if col not in FIGI_COLUMNS)
            .with_row_count(name="key")
            .with_columns(pl.lit(idx).alias("iteration"))
        )

        if "data" in figi_mapping.columns:
            mapped_df = (
                security_df.join(figi_mapping.with_row_count(name="key"), how="left", on="key")
                .rename({"name": "name_source", "ticker": "ticker_source"})
                .explode("data")
                .unnest("data")
                .rename(
                    {
                        "compositeFIGI": "composite_figi",
                        "shareClassFIGI": "share_class_figi",
                        "ticker": "ticker_figi",
                        "exchCode": "exchange_code_figi",
                        "name": "name_figi",
                        "securityType": "security_type_figi",
                        "securityType2": "security_type2_figi",
                        "marketSector": "market_sector_figi",
                        "securityDescription": "security_description_figi",
                    }
                )
            )

            # add new matches to db
            DbQueryRepositories.mapping_figi.add(mapped_df.filter(pl.col("figi").is_not_null()))

            # if warning exists, no match for security was found
            if "warning" in mapped_df.columns:
                no_match = (
                    ValidateDataset(
                        dataset=mapped_df, options={"job_name": "OpenFigi", "dataset_name": "NoFigiMappingFound"}
                    )
                    .is_not_null(column="warning")
                    .return_dataset()
                )

                # second attempt
                if attempt == 1:
                    _map_and_transform(
                        no_match.rename({"ticker_source": "ticker", "name_source": "name"}).select(security_columns),
                        attempt=2,
                    )
