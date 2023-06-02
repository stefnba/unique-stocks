# %%
import sys
import os

sys.path.append("..")
os.chdir("..")

# %%
from dags.exchange.eod_historical_data import jobs


def test():
    return jobs.ingest()


test()
