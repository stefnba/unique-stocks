from utils.filesystem.data_lake.base import DataLakePathBase


def parseDataLakePath(path: DataLakePathBase):
    """Helper to convert DataLakePath to a string path, otherwise return string as provided."""
    if isinstance(path, str):
        return path

    return path.path
