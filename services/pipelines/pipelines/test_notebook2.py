# %%

from dags.exchanges.exchanges.jobs.eod import EodExchangeJobs
from dags.indices.indices.jobs.indices import IndexJobs

# %%

path = IndexJobs.download()
path = IndexJobs.transform(path)
path = IndexJobs.curate(path)

# %%
