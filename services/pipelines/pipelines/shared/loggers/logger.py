from shared.loggers import types as Types
from shared.loggers.handlers import console_handler, file_handler, http_handler
from shared.utils.logging.logger import Logger

from shared.loggers import events

logger = Logger()
logger.add_handler(console_handler)
logger.add_handler(file_handler)
logger.add_handler(http_handler)


transform = Logger("transform")
transform.add_handler(handler=console_handler)
transform.add_handler(handler=file_handler)
logger.add_handler(http_handler)


# mapping = Logger[Events.Mapping, dict]("mapping")
# mapping.add_handler(handler=console_handler)
# mapping.add_handler(handler=file_handler)
# # mapping.add_handler(handler=http_handler)
# mapping.add_events(Events.Mapping)


# airflow = Logger[Events.Airflow, dict]("airflow")
# airflow.add_handler(handler=console_handler)
# airflow.add_handler(handler=file_handler)
# airflow.add_events(Events.Airflow)


# db = Logger[Events.Database, Types.DatabaseExtra]("database")
# db.add_handler(handler=console_handler)
# db.add_handler(handler=file_handler)
# db.add_events(Events.Database)


# api = Logger[Events.Api, Types.ApiExtra]("api")
# api.add_handler(handler=console_handler)
# api.add_handler(handler=file_handler)
# api.add_events(Events.Api)


# datalake = Logger[Events.DataLake, Types.DataLakeExtra]("datalake")
# datalake.add_handler(handler=console_handler)
# datalake.add_handler(handler=file_handler)
# datalake.add_events(Events.DataLake)
