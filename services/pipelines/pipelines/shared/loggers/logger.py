from shared.loggers import events as Events
from shared.loggers import types as Types
from shared.loggers.handlers import console_handler, file_handler
from shared.utils.logging import Logger

logger = Logger[str, dict]()
logger.add_handler(handler=console_handler)
logger.add_handler(handler=file_handler)


transform = Logger[Events.Transform, dict]("transform")
transform.add_handler(handler=console_handler)
transform.add_handler(handler=file_handler)
transform.add_events(Events.Transform)


mapping = Logger[Events.Mapping, dict]("mapping")
mapping.add_handler(handler=console_handler)
mapping.add_handler(handler=file_handler)
mapping.add_events(Events.Mapping)


airflow = Logger[Events.Airflow, dict]("airflow")
airflow.add_handler(handler=console_handler)
airflow.add_handler(handler=file_handler)
airflow.add_events(Events.Airflow)


db = Logger[Events.Database, Types.DatabaseExtra]("database")
db.add_handler(handler=console_handler)
db.add_handler(handler=file_handler)
db.add_events(Events.Database)


api = Logger[Events.Api, Types.ApiExtra]("api")
api.add_handler(handler=console_handler)
api.add_handler(handler=file_handler)
api.add_events(Events.Api)
