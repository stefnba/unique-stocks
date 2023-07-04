from copy import deepcopy
from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG, LOG_FORMAT

LOGGING_CONFIG = deepcopy(DEFAULT_LOGGING_CONFIG)


LOGGING_CONFIG["formatters"]["airflow_json"] = {
    "class": "shared.utils.logging.formatter.JsonFormatterAirflow",
    "format": LOG_FORMAT,
}

LOGGING_CONFIG["handlers"]["http_log_service"] = {
    "class": "shared.utils.logging.handlers.CustomHttp",
    "formatter": "airflow_json",
    "filters": ["mask_secrets"],
}

LOGGING_CONFIG["loggers"]["airflow.task"]["handlers"] = [
    "task",
    "http_log_service",
]
