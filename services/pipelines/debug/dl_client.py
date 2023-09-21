# %%
import duckdb
from adlfs import AzureBlobFileSystem
import polars as pl
import os

account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
client = os.getenv("AZURE_CLIENT_ID")
tenant = os.getenv("AZURE_TENANT_ID")
secret = os.getenv("AZURE_CLIENT_SECRET")

storage_options = {
    "account_name": account_name,
    "client_id": client,
    "client_secret": secret,
    "tenant_id": tenant,
}
filesystem = AzureBlobFileSystem(account_name="uniquestocksdatalake", anon=False)

# %%
# filesystem.glob("curated/exchange/**")

duckdb.register_filesystem(filesystem=filesystem)

duckdb.read_parquet("abfs://temp/20230906-152609_a6871cfdcf254662a5a2db8add8fff6f/").count("*")
# duckdb.read_parquet("abfs://curated/security_quote/**.parquet", hive_partitioning=True).filter("security_code = 'AAPL'")
# duckdb.read_parquet("abfs://curated/security/**.parquet").pl()
# duckdb.read_parquet("abfs://curated/exchange/**.parquet")

# %%

from deltalake import DeltaTable

dt = DeltaTable("abfs://curated/fundamental", storage_options=storage_options, version=2)


# dt.file_uris([("exchange_code", "=", "US")])
pl.from_arrow(
    dt.to_pyarrow_dataset(
        # partitions=[
        #     # ("exchange_code", "=", "XETRA"),
        #     ("exchange_code", "in", ["XETRA", "NASDAQ", "NYSE"]),
        #     ("type", "=", "common_stock"),
        # ]
    ).to_batches()
)

# %%
filesystem.ls("curated/security_quote")

# %%

from pyarrow.fs import PyFileSystem, FSSpecHandler

pa_fs = PyFileSystem(FSSpecHandler(filesystem))


duckdb.register_filesystem(filesystem=filesystem)
duckdb.read_csv(
    "abfs://raw/AAPL.US.csv",
).pl().with_columns(pl.col("Date").dt.year().alias("year")).write_delta(
    "abfs://raw/test_delta",
    mode="overwrite",
    storage_options=storage_options,
    delta_write_options={
        "partition_by": ["year"],
        # "filesystem": pa_fs
    },
)

# %%
pl.scan_delta("abfs://raw/test_delta", storage_options=storage_options)

# %%
import deltalake

dt = deltalake.DeltaTable("abfs://temp/None", storage_options=storage_options)
# duckdb.from_arrow(dt.to_pyarrow_dataset())
# %%

# duckdb.register_filesystem(filesystem)

# data = duckdb.read_parquet(
#     "/Users/stefanjakobbauer/Development/projects/unique-stocks/services/pipelines/testing/airflow/temp/20230829-195412_ccea78bfd6284ef0b28bbd92beb680f3.parquet"
# )


path = "/data-lake/zone=raw/product=security_quote/security=AAPL/exchange=US/source=EodHistoricalData/version=history/year=2023/month=8/day=29/zone=raw__product=security_quote__security=AAPL__exchange=US__source=EodHistoricalData__version=history__year=2023__month=8__day=29__hour=21__minute=53__second=55.json"
# data = duckdb.read_json(f"abfs://{path}", format="array", hive_partitioning=False)
# data.pl()
duckdb.sql(f"SELECT * FROM read_json_auto('abfs://{path}', hive_partitioning=false)")

# %%
from pyarrow import dataset as ds

# from pyarrow._dataset import FileSystemDataset

# from pyarrow.dataset import FileSystemDataset

d = ds.dataset(
    "temp/20230906-152609_a6871cfdcf254662a5a2db8add8fff6f/",
    filesystem=filesystem,
    format="parquet",
    # schema=schema,
)

# da = 2393
# if isinstance(d, (ds.FileSystemDataset, ds.UnionDataset)):
#     print("yes")
#     print(d.to_table())


# %%

from pyarrow import json
from deltalake.writer import write_deltalake

import pyarrow as pa

from pyarrow.dataset import Dataset

# json.read_json()

schema = pa.schema(
    [
        pa.field("date", pa.string()),
        pa.field("open", pa.float64()),
        pa.field("high", pa.float64()),
        pa.field("low", pa.float64()),
        pa.field("close", pa.float64()),
        pa.field("adjusted_close", pa.float64()),
        pa.field("volume", pa.int64()),
        pa.field("exchange_code", pa.string()),
        pa.field("security_code", pa.string()),
    ]
)

ds = ds.dataset(
    "temp/20230903-183004_5bc94e35bbb14769bf51f3ed95da82bc/",
    filesystem=filesystem,
    format="parquet",
    schema=schema,
)


# %%

write_deltalake(
    # "abfs://curated/security_quote",
    "testlake",
    # overwrite_schema=True,
    schema=ds.schema,
    data=ds.to_batches(),
    # storage_options=storage_options,
    partition_by=["security_code", "exchange_code"],
    mode="overwrite",
)


# duckdb.sql("SELECT DISTINCT security_code, exchange_code FROM ds")
