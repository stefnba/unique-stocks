from typing import Any, Literal, Optional
from airflow.providers.http.hooks.http import HttpHook
from airflow.hooks.base import BaseHook
import asyncio
import aiohttp
from string import Template
from pathlib import Path
from utils.filesystem.path import LocalPath, AdlsPath, AdlsDatasetPath
from aiolimiter import AsyncLimiter
from pyarrow import dataset as ds
from custom.providers.azure.hooks.dataset import AzureDatasetHook
from custom.providers.azure.hooks.handlers.write.azure import AzureDatasetWriteArrowHandler
from custom.providers.azure.hooks.data_lake_storage import AzureDataLakeStorageHook
from dataclasses import dataclass
from shared.types import DataLakeDataFileTypes

HttpMethods = Literal["POST", "GET", "PUT", "DELETE"]


@dataclass
class BulkApiAdlsUploadConfig:
    path: type[AdlsDatasetPath]
    conn_id: str
    record_mapping: Optional[dict[str, str]] = None


class BulkApiHook(BaseHook):
    """
    Hook to make parallel requests to an API endpoint using `asyncio` and `aiohttp` packages.
    Rates limits are also followed through `aiolimiter`.
    """

    AdlsUploadConfig = BulkApiAdlsUploadConfig

    hook_name = "BulkApiHook"
    conn_name_attr = "conn_id"
    default_conn_name = "http_default"

    base_url: str
    method: HttpMethods
    token: Optional[str]
    semaphore: asyncio.Semaphore
    rate_limit = AsyncLimiter(999, 60)
    max_parallel_requests = 30  # Semaphore

    response_format: DataLakeDataFileTypes
    local_download_dir: LocalPath
    adls_upload: Optional[BulkApiAdlsUploadConfig]

    errors: list[dict[str, Any]] = []

    counter: int = 0
    total: int = 0

    def __init__(
        self,
        conn_id: str = default_conn_name,
        response_format: DataLakeDataFileTypes = "json",
        adls_upload: Optional[BulkApiAdlsUploadConfig] = None,
        method: HttpMethods = "GET",
    ) -> None:
        self.conn_id = conn_id
        self.get_conn()
        self.method = method
        self.adls_upload = adls_upload
        self.response_format = response_format

        self.local_download_dir = LocalPath.create_temp_dir_path()

        if adls_upload:
            self.hook = AzureDataLakeStorageHook(conn_id=adls_upload.conn_id)

    def get_conn(self):
        """Set `base_url` and `token`."""
        conn = self.get_connection(self.conn_id)

        if conn.host is not None and "://" in conn.host:
            self.base_url = conn.host
        else:
            # schema defaults to HTTP
            schema = conn.schema if conn.schema is not None else "http"
            host = conn.host if conn.host is not None else ""
            self.base_url = schema + "://" + host

        if conn.port is not None:
            self.base_url = self.base_url + ":" + str(conn.port)

        if conn.password is not None:
            self.token = conn.password

    async def make_request(self, endpoint: str, args: dict[str, str]) -> bytes:
        """Make api call and return response."""

        endpoint = Template(endpoint).safe_substitute(args).lstrip("/")
        url = self.base_url.rstrip("/") + "/" + endpoint

        data = {"api_token": self.token}

        async with aiohttp.ClientSession(raise_for_status=True) as session:
            await self.semaphore.acquire()
            async with self.rate_limit:
                self.counter += 1

                if self.counter % 100 == 0:
                    print(
                        f"API Session in progress: {self.counter:,}/{self.total:,} ({(self.counter/self.total):.0%}) requests done."
                    )

                try:
                    if self.method == "GET":
                        async with session.get(url, params=data) as resp:
                            # self.log.info(f"Successful request to {url}")
                            content = await resp.read()
                            self.semaphore.release()
                            return content

                except aiohttp.ClientResponseError as err:
                    self.log.error(f"Request '{url}' failed ({err.status}) with '{err.message}'! \n\t{args}")

                    self.errors.append(
                        {
                            "message": err.message,
                            "messastatusge": err.status,
                            "payload": args,
                            "url": url,
                        }
                    )
                    raise

                raise ValueError("Method not implemented")

    def upload_response(self, data: bytes, id: str, record: dict[str, str]) -> None:
        """Save data from response to a Azure Data Lake Storage."""

        if not self.adls_upload:
            return

        adls_upload = self.adls_upload

        add_info = {}

        if adls_upload.record_mapping:
            add_info = {k: record.get(v) for k, v in adls_upload.record_mapping.items()}

        path = adls_upload.path.raw(
            format=self.response_format,
            source="EodHistoricalData",
            **add_info,
        )

        self.hook.upload(
            **path.to_dict(),
            data=data,
        )

        # self.log.info(f"{Template(id).safe_substitute(record)} Uploaded to '{path}'.")

        return

    async def save_response_local(self, data: bytes, id: str, record: dict[str, str]) -> None:
        """Save data from response to a local file."""

        id = id.replace("/", "_")

        dir = Path(self.local_download_dir.uri) / Path(*[f"{k}={v}" for k, v in record.items()])
        filename = Template(id).safe_substitute(record).strip("/") + f".{self.response_format}"

        # check if dir exists, if not create
        dir.mkdir(parents=True, exist_ok=True)

        full_path = dir / filename

        with open(full_path, "wb") as f:
            f.write(data)

    async def task(self, endpoint: str, args: dict[str, str]) -> None:
        """Task to be performed for each request."""

        try:
            res = await self.make_request(endpoint=endpoint, args=args)
            await self.save_response_local(data=res, record=args, id=endpoint)

            if self.adls_upload:
                self.upload_response(data=res, record=args, id=endpoint)

        except Exception:
            pass

    async def _run(self, endpoint: str, arg_seq: list[dict[str, str]]) -> None:
        """Initiate asyncio tasks."""

        tasks = []
        self.semaphore = asyncio.Semaphore(value=self.max_parallel_requests)

        self.total = len(arg_seq)

        self.log.info(f"Starting bulk api session ({len(arg_seq)} calls)")

        for arg in arg_seq:
            tasks.append(self.task(endpoint=endpoint, args=arg))
        await asyncio.wait(tasks)

        self.log.info(f"Finished bulk api session. {len(arg_seq)} calls done, {len(self.errors)} errors occured.")

        if len(self.errors) > 0:
            print(self.errors)

    def run(self, endpoint: str, items: list[dict[str, str]]) -> None:
        """Entrypoint to start bulk api calls."""
        asyncio.run(self._run(endpoint=endpoint, arg_seq=items))

    def read_dataset(self):
        """Read local files into Arrow Dataset."""

        return ds.dataset(
            source=self.local_download_dir.uri,
            format=self.response_format,
            partitioning="hive",
        )

    def upload_dataset(self, conn_id: str) -> AdlsPath:
        """Read local files into Arrow Dataset and write dataset to temp container on Adls."""
        dataset = self.read_dataset()
        destination_path = AdlsPath.create_temp_dir_path()
        hook = AzureDatasetHook(conn_id=conn_id)

        hook.write(
            dataset=dataset,
            destination_path=destination_path,
            handler=AzureDatasetWriteArrowHandler,
            existing_data_behavior="overwrite_or_ignore",
            basename_template="{i}" + AdlsPath.create_temp_file_path().name,
            format="parquet",
        )

        return destination_path  # type: ignore


class SharedApiHook(HttpHook):
    _base_params: dict

    conn_name_attr = "http_conn_id"
    default_conn_name = "http_default"
    conn_type = "http"
    hook_name = "SharedApiHook"

    def __init__(
        self,
        method: HttpMethods = "POST",
        http_conn_id: str = default_conn_name,
        tcp_keep_alive: bool = True,
        tcp_keep_alive_idle: int = 120,
        tcp_keep_alive_count: int = 20,
        tcp_keep_alive_interval: int = 30,
    ) -> None:
        super().__init__(
            method=method,
            http_conn_id=http_conn_id,
            tcp_keep_alive=tcp_keep_alive,
            tcp_keep_alive_idle=tcp_keep_alive_idle,
            tcp_keep_alive_count=tcp_keep_alive_count,
            tcp_keep_alive_interval=tcp_keep_alive_interval,
        )

    def run(
        self,
        endpoint: str | None = None,
        data: dict[str, Any] | str | None = None,
        headers: dict[str, Any] | None = None,
        extra_options: dict[str, Any] | None = None,
        response_format: Literal["raw", "json"] = "json",
        **request_kwargs: Any,
    ) -> Any:
        if self.method == "GET" and self._base_params:
            data = {**self._base_params, **(data if isinstance(data, dict) else {})}
        r = super().run(endpoint, data, headers, extra_options, **request_kwargs)

        if response_format == "json":
            return r.json()

        if response_format == "raw":
            return r.text
