import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        # Attach common extras if present
        for key in ("dataset", "version", "stage", "run_dir"):
            if key in record.__dict__:
                payload[key] = record.__dict__[key]
        return json.dumps(payload, ensure_ascii=False)


def get_logger(name: str = "synthetic_data_platform") -> logging.Logger:
    """Return a JSON-logging logger.

    Logging is optional and controlled via environment variables:
    - SDP_LOGGING_ENABLED: "1"/"true"/"yes" to enable (default: enabled)
    - SDP_LOG_LEVEL: log level name (default: INFO)
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # already configured

    enabled = os.environ.get("SDP_LOGGING_ENABLED", "true").lower() in {"1", "true", "yes"}

    if not enabled:
        # No-op logger
        logger.addHandler(logging.NullHandler())
        logger.setLevel(logging.WARNING)
        logger.propagate = False
        return logger

    level_name = os.environ.get("SDP_LOG_LEVEL", "INFO").upper()
    try:
        level = getattr(logging, level_name)
    except AttributeError:
        level = logging.INFO

    handler = logging.StreamHandler()  # stderr by default
    handler.setFormatter(JsonFormatter())

    logger.setLevel(level)
    logger.addHandler(handler)
    logger.propagate = False
    return logger
