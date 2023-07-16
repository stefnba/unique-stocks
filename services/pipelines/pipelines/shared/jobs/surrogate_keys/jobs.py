import polars as pl
from shared.clients.db.postgres.repositories import DbQueryRepositories
from shared.clients.duck.client import duck
from shared.loggers import logger, events as logger_events


def map_surrogate_keys(
    data: pl.LazyFrame | pl.DataFrame, product: str, uid_col_name: str = "uid", id_col_name: str = "id"
):
    """Map surrogate existing keys to dataset. If no keys exists, they'll be added to database. To reduce memory,
    LayFrame will be used."""

    if isinstance(data, pl.DataFrame):
        df = data.lazy()
    else:
        df = data

    existing_keys = DbQueryRepositories.mapping_surrogate_key.find_all(product=product)

    missing_keys = df.join(
        other=existing_keys,
        left_on=uid_col_name,
        right_on="uid",
        how="left",
    ).filter(pl.col("surrogate_key").is_null())

    missing_length = missing_keys.select(pl.count()).collect()[0, 0]
    if missing_length > 0:
        print("add", missing_length)
        DbQueryRepositories.mapping_surrogate_key.bulk_add(
            data=missing_keys.rename({uid_col_name: "uid"})
            .select("uid")
            .with_columns(pl.lit(product).alias("product"))
            .collect()
        )

    all_keys = df.join(
        other=DbQueryRepositories.mapping_surrogate_key.find_all(product=product)
        if missing_length > 0
        else existing_keys,
        left_on=uid_col_name,
        right_on="uid",
        how="left",
    ).rename({"surrogate_key": id_col_name})

    if all_keys.filter(pl.col("surrogate_key").is_null()).select(pl.count()).collect()[0, 0] > 0:
        raise Exception("Missing keys")

    return all_keys.collect()


def get_data(data: str | pl.DataFrame) -> pl.DataFrame:
    """
    Get data from data lake file path if a file path is specified (must be .parquet file).
    """

    if isinstance(data, str):
        return duck.get_data(data, handler="azure_abfs", format="parquet").pl()
    if isinstance(data, pl.DataFrame):
        return data


# def _map_to_data(data: pl.DataFrame, product: str, uid_col_name: str) -> pl.DataFrame:
#     """
#     Map surrogate keys from database to a dataset.

#     Args:
#         data (pl.DataFrame): Dataset.
#         product (str): Data product.
#         uid_col_name (str): _description_

#     Returns:
#         pl.DataFrame: _description_
#     """
#     logger.mapping.info(event=logger_events.mapping.MapData(job="SurrogateKey", product=product, size=len(data)))

#     keys = get_existing_surrogate_keys(product)

#     if len(keys) == 0:
#         logger.mapping.warn(
#             msg="No existing keys found in database.",
#             event=logger_events.mapping.MissingRecords(job="SurrogateKey", product=product),
#         )
#         return data.with_columns(pl.lit(None).alias("surrogate_key"))

#     return data.join(keys, how="left", left_on=uid_col_name, right_on="uid")


# def map_surrogate_keys(data: str | pl.DataFrame, product: str, uid_col_name: str = "uid", id_col_name: str = "id"):
#     """
#     Maps exisiting surrogate keys to a dataset or creates new ones if no keys exists for a given uid.

#     Approach for generating surrogate keys:
#     - get polars df for currently existing keys for given product
#     - map to data (also polars df) and filter where no match (these one must be created)
#     - create missing keys

#     Args:
#         data (str | pl.DataFrame): Dataset to be mapped.
#         product (str): Data product for which to retrieve and save surrogate keys.
#         uid_col_name (str, optional): Column name for lookup of surrogate key mapping. Defaults to "uid".
#         id_col_name (str, optional): Column name for surrogate key. Defaults to "id".
#     """

#     logger.mapping.info(
#         event=logger_events.mapping.InitMapping(job="SurrogateKey", product=product),
#         extra={"uid_col_name": uid_col_name, "id_col_name": id_col_name},
#     )

#     _data = get_data(data)
#     mapped_data = _map_to_data(data=_data, product=product, uid_col_name=uid_col_name)

#     # Idenfity and then add missing keys, based on data and existing keys
#     data_missing_keys = mapped_data.filter(pl.col("surrogate_key").is_null())

#     # no missing key, return mapped_data
#     if len(data_missing_keys) == 0:
#         mapped_data = mapped_data.with_columns(pl.col("surrogate_key").alias(id_col_name)).drop("surrogate_key")
#         logger.mapping.info(
#             msg="No keys were missing.",
#             event=logger_events.mapping.MappingSuccess(job="SurrogateKey", product=product, size=len(mapped_data)),
#         )
#         return mapped_data

#     # add previously missing keys to database
#     logger.mapping.info(
#         f" {len(data_missing_keys)} surrogate keys missing and will be added to database.",
#         event=logger_events.mapping.MissingRecords(job="SurrogateKey", product=product, size=len(data_missing_keys)),
#         extra={"uid_col_name": uid_col_name, "id_col_name": id_col_name, "data": data_missing_keys.to_dicts()},
#     )

#     # add data product as column, required for adding missing keys to db
#     data_missing_keys = data_missing_keys.with_columns(pl.lit(product).alias("product"))

#     added = DbQueryRepositories.mapping_surrogate_key.add(data=data_missing_keys.to_dicts(), uid_col_name=uid_col_name)
#     if len(added) > 0:
#         logger.mapping.info(
#             f"{len(added)} missing surrogate keys added to database.",
#             event=logger_events.mapping.RecordsCreated(job="SurrogateKey", product=product),
#             extra={
#                 "missing_size": len(data_missing_keys),
#                 "added_size": len(added),
#             },
#         )
#     else:
#         logger.mapping.info(
#             "Missing surrogate keys were not added to database.",
#             event=logger_events.mapping.RecordsNotCreated(job="SurrogateKey", product=product),
#             extra={
#                 "missing_size": len(data_missing_keys),
#             },
#         )

#     mapped_data = _map_to_data(_data, product, uid_col_name)
#     mapped_data = mapped_data.with_columns(pl.col("surrogate_key").alias(id_col_name)).drop("surrogate_key")
#     logger.mapping.info(
#         event=logger_events.mapping.MappingSuccess(job="SurrogateKey", product=product, size=len(mapped_data)),
#     )
#     return mapped_data


__all__ = ["map_surrogate_keys"]
