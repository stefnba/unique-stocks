# %%
import sys
import os

sys.path.append("..")
os.chdir("..")

# %%


def ingest_gleif():
    from dags.entity.gleif import jobs

    return jobs.ingest()


raw = ingest_gleif()

# %%


def unzip(file_path: str):
    from shared.hooks.data_lake import data_lake_hooks
    from dags.entity.path import EntityPath
    from dags.entity.gleif import jobs

    from shared.clients.data_lake.azure.azure_data_lake import dl_client

    df = jobs.unzip(dl_client.download_file_into_memory(file_path))
    return data_lake_hooks.upload(df, EntityPath.temp(), format="parquet")


def transform(file_path: str):
    from shared.hooks.data_lake import data_lake_hooks
    from dags.entity.path import EntityPath
    from dags.entity.gleif import jobs

    return data_lake_hooks.checkout(
        checkout_path=file_path,
        func=lambda data: jobs.transform(data),
        commit_path=EntityPath.processed(source=jobs.ASSET_SOURCE),
    )


def add_surr_keys(file_path: str):
    from dags.entity.path import EntityPath
    from dags.entity.gleif import jobs
    from shared.hooks.data_lake import data_lake_hooks

    return data_lake_hooks.checkout(
        checkout_path=file_path, func=lambda data: jobs.add_surrogate_key(data), commit_path=EntityPath.temp()
    )


url = "zone=raw/product=entity/source=GlobalLegalEntityIdentifierFoundation/year=2023/month=06/day=01/ts=20230601_144751____product=entity__source=GlobalLegalEntityIdentifierFoundation__zone=raw.zip"
temp_url = unzip(url)

# %%

transformed = transform(temp_url)
add_surr_keys(transformed)


# %%

from shared.hooks.data_lake import data_lake_hooks
from shared.clients.db.postgres.repositories import DbQueryRepositories

url = "/zone=temp/20230601_170219__0c02621d82b0402dbee6bac5acfcc610.parquet"

df = data_lake_hooks.download(url).to_polars_df()
df = df.head(1000)


# %%
df.columns
# %%

DbQueryRepositories.entity.bulk_add(df)
