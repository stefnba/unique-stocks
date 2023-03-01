# pylint: disable=R0913:too-many-arguments
import os
from io import BytesIO
from typing import Optional, cast

import requests
from requests.exceptions import HTTPError, JSONDecodeError, Timeout, TooManyRedirects

from include.utils.path import build_path

from .types import Methods, RequestFileBytes, RequestFileDisk


class Api:
    """
    Base Class for calling APIs
    """

    client_key: str
    _base_url: str | None
    _base_params: dict = {}
    _base_headers: dict = {}

    def _request_text(
        self,
        endpoint: str,
        method: Methods = "GET",
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        json: Optional[dict] = None,
    ):
        response = self._make_request(
            endpoint=endpoint,
            method=method,
            params=params,
            headers=headers,
            json=json,
            stream=True,
        )
        return response.text

    def _download_file_to_bytes(
        self,
        endpoint: str,
        method: Methods = "GET",
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        json: Optional[dict] = None,
    ) -> RequestFileBytes:
        """
        Make a request to and return downloaded file as bytes
        """

        response = self._make_request(
            endpoint=endpoint,
            method=method,
            params=params,
            headers=headers,
            json=json,
            stream=True,
        )
        filename, file_extension = os.path.splitext(endpoint)
        memory = BytesIO()
        memory.write(response.content)

        return RequestFileBytes(
            content=memory.getbuffer().tobytes(),
            extension=file_extension,
            name=filename,
        )

    def _download_file_to_disk(
        self,
        endpoint: str,
        file_destination: str,
        method: Methods = "GET",
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        json: Optional[dict] = None,
    ) -> RequestFileDisk:
        """
        Make a request to download a file and save it to disk
        """
        response = self._make_request(
            endpoint=endpoint,
            method=method,
            params=params,
            headers=headers,
            json=json,
            stream=True,
        )
        with open(file_destination, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        filename, file_extension = os.path.splitext(file_destination)

        return RequestFileDisk(
            extension=file_extension, path=file_destination, name=filename
        )

    def _request_file(
        self,
        endpoint: str,
        method: Methods = "GET",
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        json: Optional[dict] = None,
    ):
        response = self._make_request(
            endpoint=endpoint,
            method=method,
            params=params,
            headers=headers,
            json=json,
            stream=True,
        )
        return response

    def request_json(
        self,
        endpoint: str,
        method: Methods = "GET",
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        json: Optional[dict] = None,
    ) -> dict:
        """
        Make a request to return json object

        Args:
            endpoint (str): _description_
            method (Methods, optional): _description_. Defaults to "GET".
            params (Optional[dict], optional): _description_. Defaults to None.
            headers (Optional[dict], optional): _description_. Defaults to None.
            json (Optional[dict], optional): _description_. Defaults to None.

        Returns:
            _type_: _description_
        """
        response = self._make_request(
            endpoint=endpoint, method=method, params=params, headers=headers, json=json
        )
        try:
            return cast(dict, response.json())
        except JSONDecodeError:
            print("JSON not possible")
            raise

    def _make_request(
        self,
        endpoint: str,
        method: Methods = "GET",
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        json: Optional[dict] = None,
        stream=False,
    ):
        """
        Make API request
        """

        url = build_path(self._base_url, endpoint)

        if isinstance(params, dict):
            params = {**self._base_params, **params}
        else:
            params = self._base_params

        if isinstance(headers, dict):
            headers = {**self._base_headers, **headers}
        else:
            headers = self._base_headers

        try:
            response = requests.request(
                method=method,
                params=params,
                url=url,
                headers=headers,
                timeout=(2, 5),
                json=json,
                stream=stream,
            )

            print(response.url)

            response.raise_for_status()

            return response

        except Timeout:
            print("The request timed out")
            raise

        except TooManyRedirects:
            print("The request timed out")
            raise

        except HTTPError as error:
            print(error.response.status_code)
            print("The request timed out")
            raise
