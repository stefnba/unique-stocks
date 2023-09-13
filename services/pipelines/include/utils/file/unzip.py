import zipfile
import os
from utils.filesystem.path import TempDirPath


def unzip_file(path: str, delete_zip_file=False) -> str:
    """
    Unzip a file on a local file system.
    """

    destination_path = TempDirPath.create()

    with zipfile.ZipFile(path, allowZip64=True) as zip_archive:
        file_to_unzip = zip_archive.filelist[0]

        unzipped_file_path = zip_archive.extract(file_to_unzip, path=destination_path)

    if delete_zip_file:
        os.remove(path)

    return unzipped_file_path
