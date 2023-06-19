# %%
import sys
import os

sys.path.append("..")
os.chdir("..")


# %%


from shared.loggers import logger, events as logEvents

logger.logger.error("x", event=logEvents.mapping.CustomEvent(hallo="hallo"))
