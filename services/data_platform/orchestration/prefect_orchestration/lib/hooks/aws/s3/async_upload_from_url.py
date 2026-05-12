import asyncio
import time
from io import BytesIO
from pathlib import Path
from typing import Callable, Iterable, Optional

from aiobotocore.session import AioSession
from aiohttp import ClientResponseError, ClientSession
from lib.hooks.http.asynchronous import Counter, RateLimiter, create_async_http_client
from prefect_aws import AwsCredentials
from pydantic import BaseModel
from types_aiobotocore_s3.client import S3Client


class UploadItem(BaseModel):
    url: str
    bucket: str
    key: str | Path
    id: str


class S3BatchUploadFromUrlHook:
    s3_credentials: AwsCredentials
    counter: Counter
    limiter: RateLimiter

    errors: list[Exception] = []

    transform_func: Optional[Callable[[bytes | str, str], bytes | str | BytesIO | None]] = None

    def __init__(self, s3_credentials: AwsCredentials) -> None:
        self.s3_credentials = s3_credentials

    async def transform_data(self, data):
        return data

    async def _upload_one_item_from_url(self, item: UploadItem, s3_client: S3Client, http_client: ClientSession):
        """Retrieve data from URL and upload to S3 for one upload item."""

        # print(f"Uploading item with id: '{item.id}' to '{item.bucket}/{item.key}'")
        async with self.counter, self.limiter:

            data = None

            try:
                async with http_client.get(item.url) as response:
                    data = await response.json()

                    # transform data
                    if self.transform_func:
                        data = self.transform_func(data, item.id)

                    if data is not None:
                        await s3_client.put_object(
                            Bucket=item.bucket,
                            Key=item.key.as_posix() if isinstance(item.key, Path) else item.key,
                            Body=data,
                        )
                    else:
                        print("Data is None.")

            except ClientResponseError as e:
                self.errors.append(e)

    async def _upload_items_from_url(self, items: Iterable[UploadItem]):

        self.counter = Counter(step=100)
        self.limiter = RateLimiter(rate_limit=2_000)

        async with (
            create_async_http_client() as http_client,
            self.create_s3_client() as s3_client,
        ):
            await asyncio.gather(
                *[
                    self._upload_one_item_from_url(
                        item=item,
                        s3_client=s3_client,
                        http_client=http_client,
                    )
                    for item in items
                ]
            )

    def upload_from_url(self, items: list[UploadItem]):
        """Upload items from URL to S3 in parallel."""

        s = time.perf_counter()
        print(f"Uploading {len(items)} items to S3.")

        asyncio.run(self._upload_items_from_url(items=items))
        print(f"Execution time: {(time.perf_counter() - s):0.2f} seconds. {len(self.errors)} errors occured.")

    def create_s3_client(self):
        """Create an async client to interact with S3."""

        session = AioSession()

        if self.s3_credentials.aws_secret_access_key is None:
            raise ValueError("AWS Secret Access Key is required.")

        return session.create_client(
            "s3",
            aws_access_key_id=self.s3_credentials.aws_access_key_id,
            aws_secret_access_key=self.s3_credentials.aws_secret_access_key.get_secret_value(),
            endpoint_url=self.s3_credentials.aws_client_parameters.endpoint_url,
        )
