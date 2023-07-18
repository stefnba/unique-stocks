import polars as pl
from shared.clients.db.postgres.repositories import DbQueryRepositories
from shared.loggers import logger, events as logger_events


def map_surrogate_keys(
    data: pl.LazyFrame | pl.DataFrame,
    product: str,
    uid_col_name: str = "uid",
    id_col_name: str = "id",
    add_missing_keys=True,
    optional=False,
):
    """Map surrogate existing keys to dataset. If no keys exists, they'll be added to database. To reduce memory,
    LayFrame will be used.


    Args:
        data (pl.LazyFrame | pl.DataFrame): Dataset.
        product (str): Data product.
        uid_col_name (str, optional): Column from dataset used for mapping. Defaults to "uid".
        id_col_name (str, optional): Name of mapped surrogate key column. Defaults to "id".
        add_missing_keys (bool, optional): If True, create new surrogate keys and add them to database.
        Defaults to True.
        optional (bool, optional): If True, return entire provided database, even if no keys are mapped.
        Defaults to False.
    """

    # for logging
    args = {
        "uid_col_name": uid_col_name,
        "id_col_name": id_col_name,
        "add_missing_keys": add_missing_keys,
        "optional": optional,
    }

    def convert_data(data: pl.LazyFrame | pl.DataFrame):
        if isinstance(data, pl.DataFrame):
            return data.lazy()
        else:
            return data

    def handle_missing_keys(data: pl.LazyFrame, add_missing_keys: bool) -> int:
        """

        Args:
            data (pl.LazyFrame): Dataset.
            add_missing_keys (bool): Whether to add keys to database.

        Returns:
            int: Number of keys added to database.
        """
        missing_keys = data.join(
            other=existing_keys,
            left_on=uid_col_name,
            right_on="uid",
            how="left",
        ).filter(pl.col("surrogate_key").is_null())

        # how many keys are missing
        missing_length = missing_keys.select(pl.count()).collect()[0, 0]

        if missing_length == 0:
            logger.mapping.info(
                msg=f"All surrogate keys for '{product}' already found in database.",
                event=logger_events.mapping.NoMissingRecords(job="SurrogateKey", product=product),
                arguments=args,
            )
            return 0

        if missing_length > 0 and not add_missing_keys:
            logger.mapping.info(
                msg=f"{missing_length} surrogate keys are missing for '{product}'. Adding to database is skipped.",
                event=logger_events.mapping.MissingRecords(job="SurrogateKey", product=product, size=missing_length),
                arguments=args,
            )
            return 0

        # add keys if missing
        if missing_length > 0:
            logger.mapping.info(
                msg=f"{missing_length} surrogate keys are missing for '{product}' and will be added to database.",
                event=logger_events.mapping.MissingRecords(job="SurrogateKey", product=product, size=missing_length),
                arguments=args,
            )

            added_length = DbQueryRepositories.mapping_surrogate_key.add(
                data=missing_keys.rename({uid_col_name: "uid"})
                .select("uid")
                .with_columns(pl.lit(product).alias("product"))
                .collect()
            )

            if added_length > 0:
                delta = missing_length - added_length
                logger.mapping.info(
                    msg=f"{added_length} surrogate keys added to database.",
                    event=logger_events.mapping.RecordsCreated(job="SurrogateKey", product=product),
                    arguments=args,
                    length={"missing": missing_length, "added": added_length, "delta": delta},
                )
                return added_length

        return 0

    logger.mapping.info(
        event=logger_events.mapping.InitMapping(job="SurrogateKey", product=product),
        arguments=args,
    )

    _data = convert_data(data)
    existing_keys = DbQueryRepositories.mapping_surrogate_key.find_all(product=product)
    missing_length = handle_missing_keys(_data, add_missing_keys)

    # refetch if new keys were added to database
    if missing_length > 0:
        existing_keys = DbQueryRepositories.mapping_surrogate_key.find_all(product=product)

    all_keys = _data.join(
        other=existing_keys,
        left_on=uid_col_name,
        right_on="uid",
        how="left",
    ).rename({"surrogate_key": id_col_name})

    logger.mapping.info(
        event=logger_events.mapping.MappingSuccess(job="SurrogateKey", product=product),
        arguments=args,
    )

    # return all records, event w/o mapped keys if optional=True
    if optional:
        return all_keys.collect()

    # return only records w/ mapped keys
    return all_keys.filter(pl.col("surrogate_key").is_not_null()).collect()
