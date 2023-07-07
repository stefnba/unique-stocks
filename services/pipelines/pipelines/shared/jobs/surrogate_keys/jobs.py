import polars as pl
from shared.clients.db.postgres.repositories import DbQueryRepositories
from shared.clients.duck.client import duck
from shared.loggers import logger, events as logger_events


def get_existing_surrogate_keys(product: str):
    """
    Get existing active surrogate keys from database.
    """
    keys = DbQueryRepositories.mapping_surrogate_key.find_all(product=product)
    return keys[["surrogate_key", "uid"]]


def get_data(data: str | pl.DataFrame) -> pl.DataFrame:
    """
    Get data from data lake file path if a file path is specified (must be .parquet file).
    """

    if isinstance(data, str):
        return duck.get_data(data, handler="azure_abfs", format="parquet").pl()
    if isinstance(data, pl.DataFrame):
        return data


def _map_to_data(data: pl.DataFrame, product: str, uid_col_name: str) -> pl.DataFrame:
    """
    Map surrogate keys from database to a dataset.

    Args:
        data (pl.DataFrame): Dataset.
        product (str): Data product.
        uid_col_name (str): _description_

    Returns:
        pl.DataFrame: _description_
    """
    keys = get_existing_surrogate_keys(product)

    if len(keys) == 0:
        return data.with_columns(pl.lit(None).alias("surrogate_key"))

    return data.join(keys, how="left", left_on=uid_col_name, right_on="uid")


def map_surrogate_keys(data: str | pl.DataFrame, product: str, uid_col_name: str = "uid", id_col_name: str = "id"):
    """
    Maps exisiting surrogate keys to a dataset or creates new ones if no keys exists for a given uid.

    Approach for generating surrogate keys:
    - get polars df for currently existing keys for given product
    - map to data (also polars df) and filter where no match (these one must be created)
    - create missing keys

    Args:
        data (str | pl.DataFrame): Dataset to be mapped.
        product (str): Data product for which to retrieve and save surrogate keys.
        uid_col_name (str, optional): Column name for lookup of surrogate key mapping. Defaults to "uid".
        id_col_name (str, optional): Column name for surrogate key. Defaults to "id".
    """

    _data = get_data(data)

    logger.mapping.info(
        event=logger_events.mapping.InitMapping(job="SurrogateKey", product=product, size=len(_data)),
        extra={"uid_col_name": uid_col_name, "id_col_name": id_col_name},
    )

    # Idenfity and then add missing keys, based on data and existing keys
    data_missing_keys = (
        _map_to_data(data=_data, product=product, uid_col_name=uid_col_name).filter(pl.col("surrogate_key").is_null())
        # .to_dicts()
    )
    # add data product as column, required for adding missing keys to db
    data_missing_keys = data_missing_keys.with_columns(pl.lit(product).alias("product"))

    logger.mapping.info(
        "Missing surrogate keys",
        event=logger_events.mapping.MissingRecords(job="SurrogateKey", product=product, size=len(data_missing_keys)),
        extra={"uid_col_name": uid_col_name, "id_col_name": id_col_name, "data": data_missing_keys.to_dicts()},
    )

    # add previously missing keys to database
    if len(data_missing_keys) > 0:
        logger.mapping.info(
            f"{len(data_missing_keys)} missing surrogate keys will be added to database",
            extra={"data": data_missing_keys.to_dicts()},
        )
        added = DbQueryRepositories.mapping_surrogate_key.add(
            data=data_missing_keys.to_dicts(), uid_col_name=uid_col_name
        )
        if len(added) > 0:
            logger.mapping.info(f"{len(added)} missing surrogate keys added to database")

    mapped_data = _map_to_data(_data, product, uid_col_name)
    mapped_data = mapped_data.with_columns(pl.col("surrogate_key").alias(id_col_name)).drop("surrogate_key")
    return mapped_data


__all__ = ["map_surrogate_keys"]
