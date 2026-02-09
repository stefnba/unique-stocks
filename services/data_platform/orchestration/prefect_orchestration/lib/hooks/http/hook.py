from typing import Generic, Literal, Optional, Type, TypeVar, overload
from urllib.parse import urlencode, urljoin

import polars as pl
import requests
from pydantic import BaseModel, ValidationError


class HttpJSONResponseModel(BaseModel):
    pass


T = TypeVar("T", bound=BaseModel)


class HttpModelResponse(BaseModel, Generic[T]):
    """Response model for HTTP hooks."""

    data: T
    status_code: int
    response_model: Type[T]

    def to_json(self) -> str:
        """Convert response to JSON."""
        return self.data.model_dump_json()

    def to_dict(self) -> dict | list[dict]:
        """Convert the response to a dictionary."""

        return self.data.model_dump()

    def to_polars(self, schema=None) -> pl.DataFrame:
        """Convert the response to a Polars DataFrame."""

        # if not isinstance(self.data, list):
        #     raise ValueError("Only list of records can be converted to Polars DataFrame.")

        return pl.DataFrame(self.to_dict(), schema=schema)


class BaseHttpHook:

    base_url: str
    base_query_params: dict = {}

    def _build_url(
        self,
        endpoint: str,
        path_params: dict = {},
        query_params: dict = {},
    ) -> str:

        url = urljoin(self.base_url, endpoint)

        if path_params:
            url = url.format(**path_params)

        if query_params or self.base_query_params:
            query_params = {**self.base_query_params, **query_params}
            url = f"{url}?{urlencode(query_params)}"

        return url

    @overload
    def make_request(
        self,
        endpoint: str,
        path_params: dict = {},
        query_params: dict = {},
        method: str = "GET",
        response_type: Literal["json"] = "json",
        *,
        response_model: Type[T],
    ) -> HttpModelResponse[T]: ...

    @overload
    def make_request(
        self,
        endpoint: str,
        path_params: dict = {},
        query_params: dict = {},
        method: str = "GET",
        response_type: Literal["json", "raw"] = "json",
        response_model: Optional[Type[T]] = None,
    ): ...

    def make_request(
        self,
        endpoint: str,
        path_params: dict = {},
        query_params: dict = {},
        method: str = "GET",
        response_type: Literal["json", "raw"] = "json",
        response_model: Optional[Type[T]] = None,
    ):
        url = self._build_url(endpoint=endpoint, path_params=path_params, query_params=query_params)

        r = requests.get(url)

        if not r.ok:
            r.raise_for_status()

        if response_type == "json":
            r_json = r.json()

            if response_model:
                try:
                    return HttpModelResponse(
                        data=response_model.model_validate(r_json),
                        status_code=r.status_code,
                        response_model=response_model,
                    )
                except ValidationError as e:
                    print(e)
                    raise

            return r_json

        return r.raw
