import gzip
import os
import zipfile
from pathlib import Path as P
from uuid import uuid4

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


def compress_with_gzip(path: str, chunk_size=8192, delete_source_file=True) -> str:
    """
    Compress a file on a local file system using gzip.
    The compressed file is saved in the same directory as the source file.

    Args:
        path (str): Path to local uncompressed file.
        chunk_size (int, optional): _description_. Defaults to 8,192.
        delete_source_file (bool, optional): _description_. Defaults to True.

    Returns:
        str: Absolute path to local compressed file.
    """

    filename = uuid4().hex
    ext = P(path).suffix.replace(".", "")

    zip_path = P(path).parent / f"{filename}.{ext}.gz"

    with open(path, "rb") as f_in, gzip.open(zip_path, "wb") as f_out:
        while True:
            content = f_in.read(chunk_size)
            if not content:
                break
            f_out.write(content)

    if delete_source_file:
        os.remove(path)

    return zip_path.absolute().as_posix()
