from shared.types import DataLakeDatasetFileTypes


def get_dataset_format(file_name: str) -> DataLakeDatasetFileTypes:
    """Extracts dataset format from a path."""
    if file_name.endswith(".parquet"):
        return "parquet"
    if file_name.endswith(".csv"):
        return "csv"
    if file_name.endswith(".json"):
        return "json"
    raise Exception("File Type not supported.")
