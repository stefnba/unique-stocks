from typing import Any, Literal
from airflow.providers.http.hooks.http import HttpHook


HttpMethods = Literal["POST", "GET", "PUT", "DELETE"]


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
