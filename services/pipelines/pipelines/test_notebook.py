# %%


# %%
from dags.security.eod_historical_data.jobs import ingest, transform
from shared.hooks.data_lake import data_lake_hooks


raw = data_lake_hooks.checkout(func=lambda: ingest("OL"), commit_path="temp/eod.json")

data_lake_hooks.checkout(checkout_path=raw, func=lambda data: transform(data), commit_path="temp/eod.parquet")


# %%

import polars as pl
from dags.quote.eod_historical_data import jobs as eod_quote_jobs

securities = eod_quote_jobs.extract()

for security in securities:
    data = eod_quote_jobs.ingest(security)
    transformed = eod_quote_jobs.transform(pl.DataFrame(data), security)
    print(transformed)


# %%

from dags.exchange.eod_historical_data import jobs as eod_jobs
from dags.exchange.iso_mic import jobs as iso_mic_jobs
from dags.exchange.market_stack import jobs as ms_jobs
from dags.exchange.shared.jobs import merge
from shared.hooks.data_lake import data_lake_hooks

data_lake_hooks.checkout(func=eod_jobs.ingest, commit_path="eod_raw.json")

data_lake_hooks.download("eod_raw.json").to_polars_df()


# %%

data_lake_hooks.checkout(
    checkout_path="eod_raw.json",
    func=lambda data: eod_jobs.transform(data),
    commit_path="eod_transformed1.parquet",
)

data_lake_hooks.download("eod_transformed1.parquet").to_polars_df()


# %%

data_lake_hooks.checkout(func=iso_mic_jobs.ingest, commit_path="iso_mic_raw.csv")
data_lake_hooks.checkout(
    checkout_path="iso_mic_raw.csv",
    func=lambda data: iso_mic_jobs.transform(data),
    commit_path="iso_mic_transformed.parquet",
)


data_lake_hooks.checkout(func=ms_jobs.ingest, commit_path="ms_raw.json")
data_lake_hooks.checkout(
    checkout_path="ms_raw.json",
    func=lambda data: ms_jobs.transform(data),
    commit_path="ms_transformed.parquet",
)

# %%

import polars as pl

eod_historical_data = data_lake_hooks.download("eod_transformed1.parquet").to_polars_df()
market_stack = data_lake_hooks.download("ms_transformed.parquet").to_polars_df()
iso_mic = data_lake_hooks.download("iso_mic_transformed.parquet").to_polars_df()

df = merge(
    {
        "eod_historical_data": eod_historical_data,
        "market_stack": market_stack,
        "iso_mic": iso_mic,
    }
)

# df.filter((pl.col("source") == "EodHistoricalData"))

df.filter((pl.col("operating_mic").is_null()))

# .filter((pl.col("source") == "EodHistoricalData") & (pl.col("is_virtual") == "1"))

# %%

eod_historical_data


# %%
from dags.exchange.market_stack import jobs
from shared.hooks.data_lake import data_lake_hooks

data_lake_hooks.download(
    "/zone=processed/product=index/source=EodHistoricalData/year=2023/month=05/day=23/ts=20230523_195807____product=index__source=EodHistoricalData__zone=processed.parquet"
).to_json()

# path = data_lake_hooks.checkout(func=jobs.ingest, commit_path="hello1.json")


# data_lake_hooks.checkout(checkout_path=path, func=lambda data: jobs.transform(data))


# %%
from shared.clients.db.postgres.repositories import DbQueryRepositories
from shared.hooks.data_lake import data_lake_hooks

path = data_lake_hooks.checkout(func=jobs.ingest, commit_path="hello1.csv")


transformed_path = data_lake_hooks.checkout(
    checkout_path=path, func=lambda data: jobs.transform(data), commit_path="never22.parquet"
)

data_lake_hooks.checkout(checkout_path=transformed_path, func=lambda data: merge(data))


# %%
df = data_lake_hooks.download(
    # "/zone=raw/product=security/exchange=OTCQX/source=EodHistoricalData/year=2023/month=05/day=23/20230523_131208__EodHistoricalData__security__OTCQX__raw.json"
    "/zone=curated/product=exchange/version=current/exchange__current.parquet"
).to_polars_df()

DbQueryRepositories.exchange.add(df)


# %%

exchange_code = "XETRA"
# exchange_code = "NYSE"


def ingest(exchange: str):
    from dags.security.eod_historical_data.jobs import ASSET_SOURCE, ingest
    from dags.security.path import SecurityPath
    from shared.hooks.data_lake import data_lake_hooks

    return data_lake_hooks.checkout(
        func=lambda: ingest(exchange),
        # commit_path=SecurityPath.raw(source=ASSET_SOURCE, bin=exchange_code, file_type="json"),
    )


def transform(file_path: str):
    from dags.security.eod_historical_data.jobs import ASSET_SOURCE, transform
    from dags.security.path import SecurityPath
    from shared.hooks.data_lake import data_lake_hooks

    return data_lake_hooks.checkout(
        checkout_path=file_path,
        func=lambda data: transform(data),
        commit_path=SecurityPath.processed(source=ASSET_SOURCE, bin=exchange_code),
    )


path = ingest(exchange_code)

import polars as pl
from dags.security.eod_historical_data.jobs import map_figi
from dags.security.eod_historical_data.jobs import transform as transform_job

df = pl.DataFrame(path)

df = transform_job(df)

# %%


from dags.security.open_figi.jobs import map_figi_to_securities

map_figi_to_securities(df)

# .filter(pl.col("quote_source").is_not_null())


# %%


from typing import Dict, Type

import polars as pl
from pydantic import BaseModel
from shared.clients.db.postgres.repositories.exhange.schema import Exchange


class Test(BaseModel):
    name: str
    test: int
    nein: bool


PolarsSchema = Dict[str, Type[pl.Utf8] | Type[pl.Int64] | Type[pl.Boolean] | Type[pl.Datetime]]
import datetime


def convert_model_to_polars_schema(model: Type[BaseModel]) -> PolarsSchema:
    schema: PolarsSchema = {}
    for field, model_type in model.__fields__.items():
        # type = model_type._type_display()

        field_type = model_type.type_
        print(field_type)

        if issubclass(model_type.type_, str):
            schema[field] = pl.Utf8
        elif issubclass(model_type.type_, int):
            schema[field] = pl.Int64
        elif issubclass(model_type.type_, bool):
            schema[field] = pl.Boolean
        # elif issubclass(model_type.type_, datetime.datetime):
        #     schema[field] = pl.Datetime
        else:
            raise Exception(f"Schema conversion not possible for {field} {field_type}")

    return schema


convert_model_to_polars_schema(Exchange)

# pl.DataFrame([], schema=convert_model_to_polars_schema(Test))


# %%

import pandas as pd

pd.read_xml("20230525-gleif-concatenated-file-lei2.xml")


# %%


from shared.clients.api.eod.client import EodHistoricalDataApiClient

# EodHistoricalDataApiClient.get_fundamentals("10AI", "XETRA")


# %%

import polars as pl


data = EodHistoricalDataApiClient.get_securities_listed_at_exchange("OL")


df = pl.DataFrame(data)

df.filter(pl.col("Code") == "AMSC")


# %%


import xml.etree.ElementTree as ET

tree = ET.parse("local/20230525-gleif-concatenated-file-lei2.xml")
root = tree.getroot()


# %%


BASE_TAG = root.tag.replace("LEIData", "")

import polars as pl


def build_tag(tag: str):
    return f"{BASE_TAG}{tag}"


header = root.find(build_tag("LEIHeader"))

records = root.find(build_tag("LEIRecords"))

print(len(records))

records = records[:20000]

json_records = []

for record in records:
    json_record = {}

    # lei
    lei_tag = record.find(build_tag("LEI"))
    if lei_tag is not None:
        json_record["lei"] = lei_tag.text
    else:
        print("none")
        continue

    # entity
    entity = record.find(build_tag("Entity"))
    if entity is None:
        continue

    # for tag in entity.find(build_tag("LegalAddress")):
    #     print(tag.tag)

    # name
    name = entity.find(build_tag("LegalName"))
    name_tag = entity.find(build_tag("LegalName"))
    if name_tag is None:
        continue
    json_record["legal_name"] = name_tag.text

    # legal form
    legal_form_tag = entity.find(build_tag("LegalForm"))
    if legal_form_tag is not None:
        legal_form_code_tag = legal_form_tag.find(build_tag("EntityLegalFormCode"))
        if legal_form_code_tag is not None:
            json_record["legal_form_id"] = legal_form_code_tag.text

    # legal address
    legal_address_tag = entity.find(build_tag("LegalAddress"))
    if legal_address_tag is not None:
        # street
        legal_address_street_tag = legal_address_tag.find(build_tag("FirstAddressLine"))
        if legal_address_street_tag is not None:
            json_record["legal_address_street"] = legal_address_street_tag.text

        # city
        legal_address_city_tag = legal_address_tag.find(build_tag("City"))
        if legal_address_city_tag is not None:
            city = legal_address_city_tag.text
            json_record["legal_address_city"] = city.title() if city is not None else city

        # zip code
        legal_address_zip_tag = legal_address_tag.find(build_tag("PostalCode"))
        if legal_address_zip_tag is not None:
            json_record["legal_address_zip_code"] = legal_address_zip_tag.text

        # country
        legal_address_country_tag = legal_address_tag.find(build_tag("Country"))
        if legal_address_country_tag is not None:
            json_record["legal_address_country"] = legal_address_country_tag.text

    json_records.append(json_record)


pl.DataFrame(json_records)

# %%

for a in root:
    print(a)

# %%

header = root.find("{http://www.gleif.org/data/schema/leidata/2016}LEIHeader")
header = root.find("{http://www.gleif.org/data/schema/leidata/2016}LEIHeader")

record_count = header.find("{http://www.gleif.org/data/schema/leidata/2016}RecordCount").text
print(record_count)

# %%

for h in header:
    print(h)
# %%


header = root[0]

print(header.findall(".//RecordCount"))
# %%


records = root[1]


for r in root:
    print({x.tag for x in root.findall(r.tag + "/*")})

# %%
import xml.etree.ElementTree as ET

with open("person.xml", "wb") as f:
    ET.ElementTree(records[:2]).write(f)


for record in records[:2]:
    entity = record[1]

    # print(entity)

    # print(entity.keys("LegalName"))


# %%

root[0].text


# %%

import polars as pl

from shared.clients.data_lake.azure.azure_data_lake import dl_client


dl_client.upload_file(
    file="20230529-0800-gleif-goldencopy-lei2-golden-copy.csv", destination_file_path="atesdfasdfs.csv"
)

# df = pl.read_csv("20230529-0800-gleif-goldencopy-lei2-golden-copy.csv")


# %%

import requests

from azure.storage.blob import BlobClient, BlobServiceClient
from azure.identity import DefaultAzureCredential


blob_client = BlobClient(
    credential=DefaultAzureCredential(),
    account_url="https://uniquestocks.dfs.core.windows.net/",
    container_name="data-lake",
    blob_name="NEU_UPLOAD.csv",
)

blob_client.create_append_blob()

# blob_client.set_http_headers("x-ms-blob-type")

blob_client.upload_blob_from_url(
    "https://leidata-preview.gleif.org/storage/golden-copy-files/2023/05/29/789258/20230529-0800-gleif-goldencopy-lei2-golden-copy.csv.zip#"
)

# if blob_client.exists:
# blob_client.delete_blob()

# blob_client.create_append_blob()

# blob_client.append_block_from_url()


# blob_service_client = BlobServiceClient(
#     credential=DefaultAzureCredential(),
#     account_url="https://uniquestocks.dfs.core.windows.net/",
# )
# container_client = blob_service_client.get_container_client("data-lake")

# container_client.upload_blob()


# %%


def download_file(url):
    local_filename = url.split("/")[-1]

    with requests.get(url, stream=True) as r:
        r.raise_for_status()

        with open(local_filename, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                print(chunk)
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                # if chunk:
                f.write(chunk)
    return local_filename


download_file(
    "https://leidata-preview.gleif.org/storage/golden-copy-files/2023/05/29/789258/20230529-0800-gleif-goldencopy-lei2-golden-copy.csv.zip#"
)


# %%

from azure.storage.filedatalake import (
    DataLakeDirectoryClient,
    DataLakeFileClient,
    DataLakeServiceClient,
    FileSystemClient,
    PathProperties,
)

client = FileSystemClient(
    credential=DefaultAzureCredential(),
    account_url="https://uniquestocks.dfs.core.windows.net/",
    file_system_name="data-lake",
)

FILE = "20230529-0800-gleif-goldencopy-lei2-golden-copy.csv.zip"

file_client = client.get_file_client(FILE)

file_client.create_file()

chunk_size = 1024 * 1024 * 100

# with open("20230529-0800-gleif-goldencopy-lei2-golden-copy.csv", "rb") as stream:
# with open("test.json", "rb") as stream:
with open(FILE, "rb") as stream:
    length = 0

    while True:
        read_data = stream.read(chunk_size)
        if not read_data:
            break  # done

        data_size = len(read_data)
        file_client.append_data(read_data, offset=length, length=data_size)
        length = length + data_size

    print(length)
    file_client.flush_data(offset=length)


# %%

from shared.clients.data_lake.azure.azure_data_lake import dl_client


url = "https://leidata-preview.gleif.org/storage/golden-copy-files/2023/05/29/789258/20230529-0800-gleif-goldencopy-lei2-golden-copy.csv.zip"


dl_client.stream_from_url(url=url)

# %%


from dags.entity.gleif.jobs import uncompress

# file_name = "test.json"
file_name = "20230529-0800-gleif-goldencopy-lei2-golden-copy.csv.zip"
# file_name = "lei-records.json.zip"
with open(file_name, "rb") as file:
    print(uncompress(file.read()))


# %%


import polars as pl
from dags.entity.gleif.jobs import transform as gleif_transform

df = pl.read_csv("20230529-0800-gleif-goldencopy-lei2-golden-copy.csv")
# %%
transformed = gleif_transform(df)
# transformed = transformed.head(10000)
transformed

# %%

from shared.jobs.surrogate_keys.jobs import map_surrogate_keys

data = map_surrogate_keys(data=transformed, product="entity", uid_col_name="lei")
data
# %%
from shared.clients.db.postgres.repositories import DbQueryRepositories

DbQueryRepositories.entity.add(data.to_dicts())


# %%
import polars as pl

df = pl.read_csv("lei-isin-20230531T070401.csv")


df.head()
# %%


data = transform(df)

# %%
from shared.clients.db.postgres.repositories import DbQueryRepositories

DbQueryRepositories.mappings.add(data.to_dicts())


# %%


import pandas as pd
import polars as pl
from shared.clients.db.postgres.client import db_client

data = [
    {"id": 22, "name": "test"},
    {"id": 244, "name": "test"},
]

df = pl.DataFrame(data)


# print(str(data))
# %%

db_client.copy_to_table("COPY data.entity_type (id, name) FROM STDIN (FORMAT csv, HEADER true, DELIMITER ',')", data=df)
