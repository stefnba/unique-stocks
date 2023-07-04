from shared.loggers.handlers import console_handler, file_handler, http_handler
from shared.utils.logging.logger import Logger

from shared.loggers import events

logger = Logger()
logger.add_handler(handler=console_handler)
logger.add_handler(handler=file_handler)
logger.add_handler(handler=http_handler)


transform = Logger("transform")
transform.add_handler(handler=console_handler)
transform.add_handler(handler=file_handler)
transform.add_handler(handler=http_handler)


mappging = Logger("mapping")
mappging.add_handler(handler=console_handler)
mappging.add_handler(handler=file_handler)
mappging.add_handler(handler=http_handler)


airflow = Logger("airflow")
airflow.add_handler(handler=console_handler)
airflow.add_handler(handler=file_handler)
airflow.add_handler(handler=http_handler)


db = Logger("database")
db.add_handler(handler=console_handler)
db.add_handler(handler=file_handler)
db.add_handler(handler=http_handler)


api = Logger("api")
api.add_handler(handler=console_handler)
api.add_handler(handler=file_handler)
api.add_handler(handler=http_handler)


datalake = Logger("datalake")
datalake.add_handler(handler=console_handler)
datalake.add_handler(handler=file_handler)
datalake.add_handler(handler=http_handler)
