import typing as t

import requests
from airflow.hooks.base import BaseHook
from shared.path import LocalStoragePath
from utils.filesystem.storage.types import FileTypes


class HttpHook(BaseHook):
    def stream_to_local_file(
        self,
        url: str,
        file_path: t.Optional[str] = None,
        file_type: t.Optional[FileTypes] = None,
        chunk_size: int = 8192,
    ) -> str:
        """
        Read a file from a URL and stream it to a local file.

        Args:
            url (str): URL to read from.
            file_path (str): Local file path to write to.

        Returns:
            str: Local file path.
        """

        if not file_path:
            if not file_type:
                raise ValueError("File type must be specified if file path is empty.")
            p: str = LocalStoragePath.create_file(type=file_type).full_path

        else:
            p = file_path

        r = requests.get(url, stream=True)

        with open(p, "wb") as f:
            for chunk in r.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)

        return p
