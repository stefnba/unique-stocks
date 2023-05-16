import os
from io import BytesIO
from typing import Any, Mapping, Optional, Sequence, cast

import requests
from requests.exceptions import HTTPError, JSONDecodeError, Timeout, TooManyRedirects
from shared.hooks.api.types import EndpointParam, JsonResponse, Methods, RequestFileBytesReturn, RequestFileDiskReturn
from shared.loggers import logger
from shared.utils.path.builder import FilePathBuilder, UrlBuilder


class ApiHook:
    """
    Hook for calling APIs.
    """

    client_key: str
    _base_url: str | None
    _base_params: dict = {}
    _base_headers: dict = {}

    def _request_text(
        self,
        endpoint: EndpointParam,
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
        endpoint: EndpointParam,
        method: Methods = "GET",
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        json: Optional[dict] = None,
    ) -> RequestFileBytesReturn:
        """
        Make a request to and return downloaded file as bytes.
        """

        response = self._make_request(
            endpoint=endpoint,
            method=method,
            params=params,
            headers=headers,
            json=json,
            stream=True,
        )
        file = FilePathBuilder.parse_file_path(endpoint)

        memory = BytesIO()
        memory.write(response.content)

        return RequestFileBytesReturn(
            content=memory.getbuffer().tobytes(),
            extension=file.extension,
            name=file.stem_name,
        )

    def _download_file_to_disk(
        self,
        endpoint: EndpointParam,
        file_destination: str,
        method: Methods = "GET",
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        json: Optional[dict] = None,
    ) -> RequestFileDiskReturn:
        """
        Make a request to download a file and save it to disk.
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

        return RequestFileDiskReturn(extension=file_extension, path=file_destination, name=filename)

    def _request_file(
        self,
        endpoint: EndpointParam,
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
        endpoint: EndpointParam,
        method: Methods = "GET",
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        json: Optional[Mapping[Any, Any] | Sequence[Mapping[Any, Any]]] = None,
        response_type: Optional[JsonResponse] = None,
    ) -> JsonResponse:
        """
        Make a request to return json object.

        Args:
            endpoint (str): _description_
            method (Methods, optional): _description_. Defaults to "GET".
            params (Optional[dict], optional): _description_. Defaults to None.
            headers (Optional[dict], optional): _description_. Defaults to None.
            json (Optional[dict], optional): _description_. Defaults to None.

        Returns:
            _type_: _description_
        """
        response = self._make_request(endpoint=endpoint, method=method, params=params, headers=headers, json=json)
        try:
            return cast(JsonResponse, response.json())
        except JSONDecodeError:
            print("Failed to parse JSON")
            raise

    def _make_request(
        self,
        endpoint: EndpointParam,
        method: Methods = "GET",
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        json: Optional[Mapping | Sequence[Mapping]] = None,
        stream=False,
    ):
        """
        Handler for making API requests.
        """

        url = UrlBuilder.build_url(self._base_url, endpoint)
        params = {**self._base_params, **params} if isinstance(params, dict) else self._base_params
        headers = {**self._base_headers, **headers} if isinstance(headers, dict) else self._base_headers

        try:
            logger.api.info("", event=logger.api.events.REQUEST_INIT, extra={"url": url, "method": method})

            response = requests.request(
                method=method,
                params=params,
                url=url,
                headers=headers,
                timeout=(2, 5),
                json=json,
                stream=stream,
            )

            response.raise_for_status()

            logger.api.info("", event=logger.api.events.SUCCESS, extra={"url": url, "method": method})

            return response

        except Timeout as error:
            logger.api.error(str(error), event=logger.api.events.TIMEOUT, extra={"url": url, "method": method})
            raise

        except TooManyRedirects:
            print("The request timed out")
            raise

        except HTTPError as error:
            logger.api.error(
                str(error),
                event=logger.api.events.ERROR,
                extra={"url": url, "method": method, "status": error.response.status_code, "message": str(error)},
            )
            raise

        except Exception as error:
            logger.api.error(
                str(error), event=logger.api.events.ERROR, extra={"url": url, "method": method, "message": str(error)}
            )
            raise
