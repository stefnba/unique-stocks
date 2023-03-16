import io
from pathlib import Path

import polars as pl
from services.clients.datalake.azure.azure_datalake import datalake_client
from services.config import config


class MappingJobs:
    @staticmethod
    def upload():
        csv_file_dir_path = Path(__file__).resolve().parent

        mapping_table = pl.read_csv(Path(csv_file_dir_path, "../data/mapping.csv").resolve().as_posix())

        print(mapping_table)

        # upload to datalake
        uploaded_file = datalake_client.upload_file(
            remote_file="references/raw/mapping/mapping_reference.csv",
            file_system=config.azure.file_system,
            local_file=bytes(mapping_table.write_csv().encode()),
        )
        return uploaded_file.file_path

    @staticmethod
    def process(file_path: str):
        #  download file
        file_content = datalake_client.download_file_into_memory(
            file_system=config.azure.file_system, remote_file=file_path
        )

        df = pl.read_csv(io.BytesIO(file_content))

        # upload to datalake
        uploaded_file = datalake_client.upload_file(
            remote_file="references/processed/mapping/mapping_reference.parquet",
            file_system=config.azure.file_system,
            local_file=df.to_pandas().to_parquet(),
        )
        return uploaded_file.file_path
