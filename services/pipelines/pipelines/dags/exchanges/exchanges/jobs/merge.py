from typing import TypedDict

import duckdb
from shared.clients.datalake.azure.azure_datalake import datalake_client
from shared.clients.datalake.azure.file_system import abfs_client
from shared.utils.path.datalake.builder import DatalakePathBuilder


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

    data_eod = db.read_parquet(DatalakePathBuilder.build_abfs_path(file_paths["eod_exchange_path"]))
    data_iso = db.read_parquet(DatalakePathBuilder.build_abfs_path(file_paths["iso_exchange_path"]))
    data_msk = db.read_parquet(DatalakePathBuilder.build_abfs_path(file_paths["ms_exchange_path"]))

    merged = db.sql(
        """
        --sql
        SELECT
            data_iso.operating_mic AS mic,
            COALESCE(data_msk.app_id, data_eod.app_id) AS app_id,
            COALESCE(data_msk.name, data_eod.name, data_iso.legal_name) AS name,
            COALESCE(data_msk.acronym, data_iso.acronym) AS acronym,
            COALESCE(data_msk.website, data_eod.website, data_iso.website) AS website,
            COALESCE(data_msk.city, data_eod.city, data_iso.city) AS city,
            COALESCE(data_msk.country, data_eod.country, data_iso.country) AS country,
            COALESCE(data_msk.currency, data_eod.currency) AS currency,
            COALESCE(data_msk.timezone, data_eod.timezone) AS timezone,
            COALESCE(data_eod.data_source, data_msk.data_source) AS data_source,
            COALESCE(data_eod.source_code, data_msk.source_code) AS source_code,
            COALESCE(data_eod.is_virtual, data_msk.is_virtual) AS is_virtual,
            COALESCE(data_iso.comments) AS comments,
            COALESCE(data_iso.market_name_institution) AS market_name_institution,
        FROM data_iso
        LEFT JOIN data_eod
            ON data_iso.mic = data_eod.mic
        LEFT JOIN data_msk
            ON data_iso.mic = data_msk.mic
        ;
        """
    )

    # datalake destination
    uploaded_file = datalake_client.upload_file(
        destination_file_path="curated/product=exchanges/exchanges.parquet",
        file=merged.df().to_parquet(),
    )

    return uploaded_file.file.full_path
