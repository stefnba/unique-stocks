import io

import polars as pl
from shared.clients.datalake.azure.azure_datalake import datalake_client
from shared.config import config
from shared.utils.path.builder import PathBuilder


class MappingJobs:
    @staticmethod
    def upload():
        mapping_table = pl.read_csv(PathBuilder.build_relative_to_base(base_path=__file__, path="../data/mapping.csv"))

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
