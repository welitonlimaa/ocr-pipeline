import logging
import sys
import os
import traceback
from datetime import datetime, timezone
from typing import Any

try:
    import json

    _JSON_AVAILABLE = True
except ImportError:
    _JSON_AVAILABLE = False


# ---------------------------------------------------------------------------
# Formatter JSON
# ---------------------------------------------------------------------------


class JSONFormatter(logging.Formatter):
    """
    Emite cada linha de log como um objeto JSON em uma única linha.
    Campos fixos: timestamp, level, logger, message, service.
    Campos extras são mesclados no nível raiz se passados via `extra={}`.
    Exceções são serializadas em `error.type`, `error.message`, `error.stacktrace`.
    """

    SERVICE_NAME = os.getenv("SERVICE_NAME", "ocr-pipeline")
    ENV = os.getenv("ENV", "production")

    def format(self, record: logging.LogRecord) -> str:
        log_entry: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "status_message": record.getMessage(),
            "service": self.SERVICE_NAME,
            "env": self.ENV,
        }

        _RESERVED = {
            "name",
            "msg",
            "args",
            "levelname",
            "levelno",
            "pathname",
            "filename",
            "module",
            "exc_info",
            "exc_text",
            "stack_info",
            "lineno",
            "funcName",
            "created",
            "msecs",
            "relativeCreated",
            "thread",
            "threadName",
            "processName",
            "process",
            "message",
            "taskName",
        }
        for key, value in record.__dict__.items():
            if key not in _RESERVED and not key.startswith("_"):
                log_entry[key] = value

        if record.levelno >= logging.WARNING:
            log_entry["src"] = f"{record.filename}:{record.lineno}"

        if record.exc_info and record.exc_info[0] is not None:
            exc_type, exc_value, exc_tb = record.exc_info
            log_entry["error"] = {
                "type": exc_type.__name__,
                "message": str(exc_value),
                "stacktrace": traceback.format_exception(exc_type, exc_value, exc_tb),
            }

        try:
            return json.dumps(log_entry, ensure_ascii=False, default=str)
        except (TypeError, ValueError):
            log_entry["_serialize_error"] = True
            return json.dumps({k: str(v) for k, v in log_entry.items()})


class HumanFormatter(logging.Formatter):
    """Formato legível para desenvolvimento local."""

    COLORS = {
        "DEBUG": "\033[36m",
        "INFO": "\033[32m",
        "WARNING": "\033[33m",
        "ERROR": "\033[31m",
        "CRITICAL": "\033[35m",
    }
    RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        color = self.COLORS.get(record.levelname, "")
        ts = datetime.fromtimestamp(record.created, tz=timezone.utc).strftime(
            "%H:%M:%S"
        )
        base = f"{color}[{ts}] {record.levelname:<8}{self.RESET} {record.name}: {record.getMessage()}"
        if record.exc_info:
            base += "\n" + self.formatException(record.exc_info)
        return base


# ---------------------------------------------------------------------------
# Setup global
# ---------------------------------------------------------------------------

_configured = False


def configure_logging(level: str = None) -> None:
    """
    Configura o sistema de logging global.
    Deve ser chamado uma única vez na inicialização da aplicação (main / lifespan).

    - ENV=production  → JSON em stdout
    - ENV=development → formato human-readable colorido
    """
    global _configured
    if _configured:
        return
    _configured = True

    env = os.getenv("ENV", "production").lower()
    log_level = getattr(
        logging, (level or os.getenv("LOG_LEVEL", "INFO")).upper(), logging.INFO
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(log_level)

    if env == "development":
        handler.setFormatter(HumanFormatter())
    else:
        handler.setFormatter(JSONFormatter())

    root = logging.getLogger()
    root.setLevel(log_level)
    root.handlers.clear()
    root.addHandler(handler)

    for noisy in ("boto3", "botocore", "urllib3", "s3transfer"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    logging.getLogger("celery").setLevel(logging.INFO)
    logging.getLogger("celery.task").setLevel(logging.INFO)


def get_logger(name: str) -> logging.Logger:
    """
    Retorna logger nomeado.
    """
    configure_logging()
    return logging.getLogger(name)
