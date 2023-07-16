import polars as pl


def unzip_convert(file_path: str) -> str:
    """
    Unpack zip file, convert to parquet file and then remove unziped file.
    """
    from shared.utils.file.unzip import unzip_file
    import os
    from shared.utils.path.container.file_path import container_file_path

    csv_path = unzip_file(file_path, delete_zip_file=True)
    df = pl.scan_csv(csv_path)

    parquet_path = container_file_path("parquet")

    df.sink_parquet(parquet_path)
    os.remove(csv_path)

    return parquet_path
