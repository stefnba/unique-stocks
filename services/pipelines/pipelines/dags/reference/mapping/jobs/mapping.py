import io

import polars as pl
from dags.reference.mapping.jobs.config import MappingPath
from shared.clients.datalake.azure.azure_datalake import datalake_client
from shared.utils.path.builder import FilePathBuilder


class MappingJobs:
    @staticmethod
    def upload():
        mapping_table = pl.read_csv(
            FilePathBuilder.build_relative_file_path(base_path=__file__, path="../data/mapping.csv")
        )

        # upload to datalake
        uploaded_file = datalake_client.upload_file(
            destination_file_path=MappingPath(zone="raw", file_type="csv"),
            file=bytes(mapping_table.write_csv().encode()),
        )
        return uploaded_file.file.full_path

    @staticmethod
    def process(file_path: str):
        #  download file
        file_content = datalake_client.download_file_into_memory(file_path=file_path)

        df = pl.read_csv(io.BytesIO(file_content))

        # upload to datalake
        uploaded_file = datalake_client.upload_file(
            destination_file_path=MappingPath(zone="processed"),
            file=df.to_pandas().to_parquet(),
        )
        return uploaded_file.file.full_path
