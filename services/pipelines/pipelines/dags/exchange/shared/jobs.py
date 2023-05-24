from typing import TypedDict

import polars as pl


class MergeDataFrames(TypedDict):
    eod_historical_data: pl.DataFrame
    market_stack: pl.DataFrame
    iso_mic: pl.DataFrame


def merge(data: MergeDataFrames):
    from shared.clients.duck.client import duck

    df = duck.query(
        "./sql/merge.sql",
        data_iso=data["iso_mic"],
        data_eod=data["eod_historical_data"],
        data_msk=data["market_stack"],
    ).pl()

    df = df.with_columns(pl.col("is_virtual").cast(pl.Boolean).keep_name())

    return df


def add_surr_keys(data: pl.DataFrame):
    from shared.jobs.surrogate_keys.jobs import map_surrogate_keys

    # add to mic
    data = map_surrogate_keys(data=data, product="exchange", id_col_name="id", uid_col_name="mic")

    # # add to operating mic
    data = map_surrogate_keys(
        data=data, product="exchange", uid_col_name="operating_mic", id_col_name="operating_exchange_id"
    )

    # remove operating mic from exchanges that are the operating entities
    data = data.with_columns(
        pl.when(pl.col("id") == pl.col("operating_exchange_id"))
        .then(None)
        .otherwise(pl.col("operating_exchange_id"))
        .alias("operating_exchange_id")
    )

    return data


# operating_exchange_id = operating_exchange_id.with_columns(
#     pl.when(pl.col("id") == pl.col("operating_exchange_id"))
#     .then(None)
#     .otherwise(pl.col("operating_exchange_id"))
#     .keep_name()
# )


def load_into_db(data: pl.DataFrame):
    """
    Load exchange data into database.
    """
    from shared.clients.db.postgres.repositories import DbQueryRepositories

    return DbQueryRepositories.exchange.add(data)
