from typing import Literal

from pydantic import BaseModel

from include.utils import build_file_path, path_with_dateime

FileExtTypes = Literal["csv", "json"]


class Assets(BaseModel):
    asset = "exchange_list"


def build_remote_location_exchange_list(
    asset_source: str, file_extension: FileExtTypes = "csv"
):
    """
    Builds remote location for exchange list files

    Args:
        asset_source (str): _description_
        file_extension (FileExtTypes, optional): Format of file to save.
        Defaults to "csv".

    Returns:
        _type_: _description_
    """
    asset_category = "exchanges"
    asset = "exchange_list"

    return build_file_path(
        directory=["raw", asset_category, asset, asset_source, path_with_dateime("%Y")],
        filename=f'{path_with_dateime("%Y%m%d")}_{asset_source}_{asset}',
        extension=file_extension,
    )


def build_remote_location_exchange_details(
    asset_source: str, exchange: str, file_extension: FileExtTypes = "csv"
):
    """_summary_

    Args:
        asset_source (str): _description_
        exchange (str): _description_
        file_extension (FileExtTypes, optional): _description_. Defaults to "csv".

    Returns:
        _type_: _description_
    """
    asset_category = "exchanges"
    asset = "exchange_details"

    return build_file_path(
        directory=[
            "raw",
            asset_category,
            asset,
            asset_source,
            exchange,
            path_with_dateime("%Y"),
        ],
        filename=f'\
            {path_with_dateime("%Y%m%d")}_{asset_source}_{exchange}_{asset}',
        extension=file_extension,
    )
