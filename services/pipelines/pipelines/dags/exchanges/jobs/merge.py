from typing import TypedDict

import duckdb
from shared.clients.datalake.azure.azure_datalake import datalake_client
from shared.clients.datalake.azure.file_system import abfs_client, build_abfs_path
from shared.config import config


class ExchangeSources(TypedDict):
    eod_exchange_path: str
    iso_exchange_path: str
    ms_exchange_path: str


def merge_exchanges(file_paths: ExchangeSources):
    """


    Args:
        processed_files (ExchangeSources): _description_
    """

    db = duckdb.connect()
    db.register_filesystem(abfs_client)

    data_eod = db.read_parquet(build_abfs_path(file_paths["eod_exchange_path"]))
    data_iso = db.read_parquet(build_abfs_path(file_paths["iso_exchange_path"]))
    data_ms = db.read_parquet(build_abfs_path(file_paths["ms_exchange_path"]))

    merged = db.sql(
        """
        --sql
        SELECT
            data_iso.operating_mic AS mic,
            COALESCE(data_ms.app_id, data_eod.app_id) AS app_id,
            COALESCE(data_ms.name, data_eod.name, data_iso.legal_name) AS name,
            COALESCE(data_ms.acronym, data_iso.acronym) AS acronym,
            COALESCE(data_ms.website, data_eod.website, data_iso.website) AS website,
            COALESCE(data_ms.city, data_eod.city, data_iso.city) AS city,
            COALESCE(data_ms.country, data_eod.country, data_iso.country) AS country,
            COALESCE(data_ms.currency, data_eod.currency) AS currency,
            COALESCE(data_ms.timezone, data_eod.timezone) AS timezone,
            COALESCE(data_eod.data_source, data_ms.data_source) AS data_source,
            COALESCE(data_eod.source_code, data_ms.source_code) AS source_code,
            COALESCE(data_eod.is_virtual, data_ms.is_virtual) AS is_virtual,
            COALESCE(data_iso.comments) AS comments,
            COALESCE(data_iso.market_name_institution) AS market_name_institution,
        FROM data_iso
        LEFT JOIN data_eod
            ON data_iso.mic = data_eod.mic
        LEFT JOIN data_ms
            ON data_iso.mic = data_ms.mic
        ;
        """
    )

    # datalake destination
    uploaded_file = datalake_client.upload_file(
        remote_file="curated/product=exchanges/exchanges.parquet",
        file_system=config.azure.file_system,
        local_file=merged.df().to_parquet(),
    )

    return uploaded_file.file_path
