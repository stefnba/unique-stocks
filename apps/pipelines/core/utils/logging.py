"""Central logging configuration for the pipelines app."""

from __future__ import annotations

import logging
import os
import sys
from collections.abc import Callable, Mapping, MutableMapping
from typing import Any, Final, Literal, cast

import structlog

from core.utils.redaction import (
    REDACTED_LOG_VALUE,
    REDACTED_QUERY_VALUE,
    SENSITIVE_KEY_PATTERN,
    redact_sensitive_query_params,
)

type LogFormat = Literal["auto", "console", "json"]
type LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR"]

_PIPELINE_SERVICE: Final[str] = "pipelines"
_PIPELINE_HANDLER_NAME: Final[str] = "pipeline-console"
_DEFAULT_LOG_LEVEL: Final[LogLevel] = "INFO"
_DEFAULT_LOG_FORMAT: Final[LogFormat] = "auto"

_configured = False


def configure_logging(*, settings: object | None = None, force: bool = False) -> None:
    """Configure standard logging and structlog for the current process.

    Args:
        settings: Optional settings-like object. When omitted, app settings are loaded lazily.
        force: Rebuild logging configuration even if it has already been configured.
    """
    global _configured

    if _configured and not force:
        return

    settings = settings or object()
    level = _log_level(settings)
    log_format = _resolved_log_format(settings)
    renderer = _renderer(log_format)
    shared_processors = [
        structlog.contextvars.merge_contextvars,
        _add_app_context(settings),
        _add_prefect_context,
        _redact_event_dict,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    _configure_stdlib_logging(level=level, renderer=renderer, shared_processors=shared_processors, force=force)
    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    _configured = True


def _configure_stdlib_logging(
    *,
    level: LogLevel,
    renderer: Callable[..., str | bytes],
    shared_processors: list[Callable[..., Any]],
    force: bool,
) -> None:
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
    )

    if force:
        for handler in list(root_logger.handlers):
            if _is_pipeline_handler(handler):
                root_logger.removeHandler(handler)

    handler = _pipeline_handler(root_logger.handlers)
    if handler is None:
        handler = logging.StreamHandler(sys.stdout)
        handler.set_name(_PIPELINE_HANDLER_NAME)
        root_logger.addHandler(handler)
    handler.setLevel(level)
    handler.setFormatter(formatter)

    logging.getLogger("httpx").setLevel("WARNING")
    logging.getLogger("httpcore").setLevel("WARNING")


def _pipeline_handler(handlers: list[logging.Handler]) -> logging.Handler | None:
    for handler in handlers:
        if _is_pipeline_handler(handler):
            return handler
    return None


def _is_pipeline_handler(handler: logging.Handler) -> bool:
    return handler.get_name() == _PIPELINE_HANDLER_NAME


def _log_level(settings: object) -> LogLevel:
    value = str(getattr(settings, "pipeline_log_level", os.getenv("PIPELINE_LOG_LEVEL", _DEFAULT_LOG_LEVEL))).upper()
    if value not in {"DEBUG", "INFO", "WARNING", "ERROR"}:
        return _DEFAULT_LOG_LEVEL
    return cast(LogLevel, value)


def _resolved_log_format(settings: object) -> Literal["console", "json"]:
    value = str(getattr(settings, "pipeline_log_format", os.getenv("PIPELINE_LOG_FORMAT", _DEFAULT_LOG_FORMAT))).lower()
    if value == "json":
        return "json"
    if value == "console":
        return "console"
    environment = str(getattr(settings, "environment", os.getenv("ENVIRONMENT", "dev")))
    return "json" if environment == "prod" else "console"


def _renderer(log_format: Literal["console", "json"]) -> Callable[..., str | bytes]:
    if log_format == "json":
        return structlog.processors.JSONRenderer(sort_keys=True)
    return structlog.dev.ConsoleRenderer(colors=False)


def _add_app_context(settings: object) -> Callable[[Any, str, MutableMapping[str, Any]], MutableMapping[str, Any]]:
    environment = str(getattr(settings, "environment", os.getenv("ENVIRONMENT", "dev")))
    code_version = _code_version()

    def add_app_context(_: Any, __: str, event_dict: MutableMapping[str, Any]) -> MutableMapping[str, Any]:
        event_dict.setdefault("service", _PIPELINE_SERVICE)
        event_dict.setdefault("environment", environment)
        if code_version:
            event_dict.setdefault("code_version", code_version)
        return event_dict

    return add_app_context


def _add_prefect_context(_: Any, __: str, event_dict: MutableMapping[str, Any]) -> MutableMapping[str, Any]:
    for key, value in _prefect_context().items():
        event_dict.setdefault(key, value)
    return event_dict


def _prefect_context() -> dict[str, str]:
    context: dict[str, str] = {}
    try:
        from prefect.runtime import flow_run, task_run
    except ImportError:
        return context

    for prefix, runtime in (("prefect_flow_run", flow_run), ("prefect_task_run", task_run)):
        for attr in ("id", "name"):
            try:
                value = getattr(runtime, attr, None)
            except Exception:
                value = None
            if value:
                context[f"{prefix}_{attr}"] = str(value)
    return context


def _redact_event_dict(_: Any, __: str, event_dict: MutableMapping[str, Any]) -> MutableMapping[str, Any]:
    return {str(key): _redact_value(key, value) for key, value in event_dict.items()}


def _redact_value(key: object, value: object) -> object:
    if _is_sensitive_key(str(key)):
        return REDACTED_LOG_VALUE
    if isinstance(value, str):
        return _redact_string(value)
    if isinstance(value, Mapping):
        return {str(item_key): _redact_value(item_key, item_value) for item_key, item_value in value.items()}
    if isinstance(value, list):
        return [_redact_value(key, item) for item in value]
    if isinstance(value, tuple):
        return tuple(_redact_value(key, item) for item in value)
    return value


def _is_sensitive_key(key: str) -> bool:
    return SENSITIVE_KEY_PATTERN.search(key) is not None


def _redact_string(value: str) -> str:
    return redact_sensitive_query_params(value)


def _code_version() -> str | None:
    for name in ("GIT_SHA", "SOURCE_COMMIT", "COMMIT_SHA", "IMAGE_TAG"):
        if value := os.getenv(name):
            return value
    return None


__all__ = [
    "LogFormat",
    "LogLevel",
    "REDACTED_LOG_VALUE",
    "REDACTED_QUERY_VALUE",
    "configure_logging",
    "redact_sensitive_query_params",
]
