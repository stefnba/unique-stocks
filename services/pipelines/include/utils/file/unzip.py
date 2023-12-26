import os
import zipfile

from utils.filesystem.path import LocalPath, Path, PathInput


def unzip_file(path: PathInput, delete_zip_file=False) -> Path:
    """
    Unzip a file on a local file system.
    """

    destination_path = LocalPath.create_temp_dir_path()
    path = LocalPath.create(path)

    with zipfile.ZipFile(path.uri, allowZip64=True) as zip_archive:
        file_to_unzip = zip_archive.filelist[0]

        unzipped_file_path = zip_archive.extract(file_to_unzip, path=destination_path.uri)

    if delete_zip_file:
        os.remove(path.uri)

    return LocalPath.create(unzipped_file_path)
