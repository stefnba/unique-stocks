# %%

from dags.exchange.exchange.jobs.eod import EodExchangeJobs
from dags.index.index.jobs.index import IndexJobs

# %%

path = IndexJobs.download()
path = IndexJobs.transform(path)
path = IndexJobs.curate(path)

# %%
