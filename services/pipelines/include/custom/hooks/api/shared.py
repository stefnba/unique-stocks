from typing import Any, Literal, Optional
from airflow.providers.http.hooks.http import HttpHook
from airflow.hooks.base import BaseHook
import asyncio
import aiohttp
from string import Template
from pathlib import Path

from aiolimiter import AsyncLimiter

HttpMethods = Literal["POST", "GET", "PUT", "DELETE"]


class BulkApiHook(BaseHook):
    """
    Hook to make parallel requests to an API endpoint using `asyncio` and `aiohttp` packages.
    Rates limits are also followed through `aiolimiter`.
    """

    hook_name = "BulkApiHook"
    conn_name_attr = "conn_id"
    default_conn_name = "http_default"

    base_url: str
    method: HttpMethods
    token: Optional[str]
    semaphore: asyncio.Semaphore
    rate_limit = AsyncLimiter(999, 60)
    max_parallel_requests = 30  # Semaphore

    base_dir: str
    filename: Optional[str]
    container: Optional[str]

    errors: list[dict[str, Any]] = []

    counter: int = 0
    total: int = 0

    def __init__(
        self,
        conn_id: str = default_conn_name,
        *,
        base_dir: str,
        filename: Optional[str] = None,
        container: Optional[str] = None,
        method: HttpMethods = "GET",
    ) -> None:
        self.base_dir = base_dir
        self.container = container
        self.filename = filename
        self.conn_id = conn_id
        self.get_conn()
        self.method = method

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
                    print(f"{self.counter}/{self.total} () requests done.")

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

    # async def upload_response(self, data: bytes, filename: str, args: dict[str, str]) -> None:
    #     filename = Template(filename).safe_substitute(args).lstrip("/")
    #     dir = Template(self.base_dir).safe_substitute(args).rstrip("/")

    #     full_path = Path(dir) / Path(filename + ".csv")

    #     from custom.providers.azure.hooks.data_lake_storage import AzureDataLakeStorageHook

    #     AzureDataLakeStorageHook(conn_id="azure_data_lake").upload(
    #         container="temp", blob_path=full_path.as_posix(), data=data
    #     )

    async def save_response(self, data: bytes, filename: str, args: dict[str, str]) -> None:
        filename = Template(filename).safe_substitute(args).lstrip("/")
        dir = Template(self.base_dir).safe_substitute(args).rstrip("/")

        # check if dir exists, if not create
        Path(dir).mkdir(parents=True, exist_ok=True)

        full_path = Path(dir) / Path(filename + ".csv")

        with open(full_path, "wb") as f:
            f.write(data)

    async def task(self, endpoint: str, args: dict[str, str]) -> None:
        """Task to be performed for each request."""

        try:
            res = await self.make_request(endpoint=endpoint, args=args)
            await self.save_response(data=res, args=args, filename=self.filename or endpoint.replace("/", "_"))
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

    def run(self, endpoint: str, args: list[dict[str, str]]):
        """Entrypoint to start bulk api calls."""
        asyncio.run(self._run(endpoint=endpoint, arg_seq=args))


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
