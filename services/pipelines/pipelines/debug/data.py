# %%
import sys
import os

sys.path.append("..")
os.chdir("..")

# %%

from shared.clients.duck.client import duck

url = "/zone=temp/20230710_122425__f2cdd515f2804b379b5635a8f9b9b277.parquet"
# url = "/zone=temp/20230710_123213__47c78e955929431d88367b49a0f79196.parquet"

data = duck.get_data(url, handler="azure_abfs")

data.pl()

# %%

data.filter("mic = 'DRSP'").pl()
