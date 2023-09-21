# %%
# ruff: noqa: E402

import sys
import os

sys.path.append("..")
os.chdir("..")


# %%
from shared.hooks.data_lake.data_lake_hooks import download

df = download(
    "zone=processed/product=entity_isin/source=GlobalLegalEntityIdentifierFoundation/year=2023/month=07/day=17/ts=20230717_132959____product=entity_isin__source=GlobalLegalEntityIdentifierFoundation__zone=processed.parquet"
).to_polars_df()

df = df[["lei", "isin"]].limit(10 * 1000)

# %%

from shared.jobs.surrogate_keys.jobs import map_surrogate_keys

map_surrogate_keys(data=df, product="entity_isin", id_col_name="test", uid_col_name="isin")
