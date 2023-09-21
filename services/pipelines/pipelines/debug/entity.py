# %%
import sys
import os

sys.path.append("..")
os.chdir("..")

# %%

from dags.entity.gleif import jobs_entity, jobs_entity_isin
from dags.entity.gleif import jobs_shared
from shared.clients.api.gleif.client import GleifApiClient
import polars as pl
from shared.clients.db.postgres.repositories import DbQueryRepositories

# %%


def gleif_entity():
    path = GleifApiClient.get_entity_list("downloads/gleif.zip")
    unziped_path = jobs_shared.unzip_convert(path)
    # return jobs_entity.transform(pl.scan_parquet(unziped_path))
    return unziped_path


def gleif_entity_isin():
    path = GleifApiClient.get_entity_isin_mapping("downloads/gleif_isin.zip")
    unziped_path = jobs_shared.unzip_convert(path)
    return jobs_entity_isin.transform(pl.scan_parquet(unziped_path))


# %%

gleif_isin_df = gleif_entity_isin()
gleif_isin_df.collect()

# %%

gleif_entity_df = gleif_entity()
gleif_entity_df
# gleif_entity_df.collect()


# %%

data = pl.scan_parquet("downloads/20230718-090039_8fa4495cb4734de3ac3bcea4f91bf052.parquet")

data = data.with_columns(
    [pl.when(pl.col(pl.Utf8).str.lengths() == 0).then(None).otherwise(pl.col(pl.Utf8)).keep_name()]
)

data.collect()
