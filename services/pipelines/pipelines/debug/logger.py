# %%
import sys
import os

sys.path.append("..")
os.chdir("..")


# %%


from shared.loggers import logger, events as log_events


logger.db.error(event=log_events.database.QueryExecution(query="SELECT sadf"))
