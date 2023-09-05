# %%
import duckdb
from adlfs import AzureBlobFileSystem
import polars as pl

filesystem = AzureBlobFileSystem(account_name="uniquestocksdatalake", anon=False)

storage_options = {
    "account_name": "uniquestocksdatalake",
    "client_id": "9f4b477f-d48d-44f5-865d-f9290c7266c5",
    "client_secret": "W_K8Q~nYMEAgb4-h-3-Mje-YFGxlS04otyo8obOX",
    "tenant_id": "b7db8837-d6cf-499f-a354-05bdb6e15170",
}

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
    "temp/20230903-183004_5bc94e35bbb14769bf51f3ed95da82bc/",
    filesystem=filesystem,
    format="parquet",
    # schema=schema,
)
da = 2393
if isinstance(d, (ds.FileSystemDataset, ds.UnionDataset)):
    print("yes")
    print(d.to_table())


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

# %%
from deltalake import DeltaTable

dt = DeltaTable("testlake")

dt.optimize()
# %%
# duckdb.from_arrow(dt.to_pyarrow_dataset()).pl()


from deltalake import DeltaTable

dt = DeltaTable("abfs://curated/security_quote", storage_options=storage_options)


# %%
# dt = DeltaTable("abfs://curated/security_quote", storage_options=storage_options)

dt.load_version(version=0)


dt.load_with_datetime()

# DeltaTable("abfs://curated/security_quote", storage_options=storage_options).version()
# %%
ds = DeltaTable("abfs://curated/security_quote", storage_options=storage_options).to_pyarrow_dataset()

ds.count_rows()
# print("done")
# pl.scan_pyarrow_dataset(source=ds).collect()
# pl.scan_pyarrow_dataset(source=ds).filter(pl.col("security_code") == "MHL").collect()


# .to_pandas()["security_code"].unique()
