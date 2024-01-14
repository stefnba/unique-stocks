import os
import typing
from dataclasses import dataclass
from pathlib import Path


@dataclass
class DirFile:
    path: str
    filename: str
    rel_path: str


def scan_dir_files(dir: str, include_sub_dirs=True) -> typing.Generator[DirFile, None, None]:
    """
    Creates generator of files residing in a directory.

    Args:
        dir (str):
            Path to directory.
        include_sub_dirs (bool, optional):
            Whether to also walk into sub directories or not.

    Yields:
        DirFile: Dataclass containiner path, filename and path relative to `dir` arg.
    """
    if include_sub_dirs:
        for root, _, files in os.walk(dir):
            for file in files:
                path = os.path.join(root, file)
                yield DirFile(path=path, filename=file, rel_path=str(Path(path).relative_to(dir)))
    else:
        for filename in os.scandir(dir):
            if filename.is_file() and not filename.name.startswith("."):
                yield DirFile(path=filename.path, filename=filename.name, rel_path=filename.name)
