"""Structured JSON logger for Glue jobs.

CloudWatch captures stdout from Glue, so we emit one JSON object per line.
Every record includes ``job_name`` and ``execution_id`` so log searches can
filter by Step Functions execution. Each job is expected to set
``os.environ["EXECUTION_ID"]`` immediately after parsing its arguments — when
the env var is missing, the logger falls back to the literal string ``"local"``
(useful for unit tests and ad-hoc local invocation).
"""

from __future__ import annotations

import json
import logging
import os
import sys
from typing import Any

_DEFAULT_EXECUTION_ID = "local"


def get_logger(job_name: str) -> logging.Logger:
    """Return a configured logger for ``job_name`` (idempotent across calls)."""
    logger = logging.getLogger(job_name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return logger


def log(logger: logging.Logger, level: str, msg: str, **kwargs: Any) -> None:
    """Emit a single JSON log record at ``level`` with ``msg`` and extra context."""
    record: dict[str, Any] = {
        "level": level,
        "job_name": logger.name,
        "execution_id": os.environ.get("EXECUTION_ID", _DEFAULT_EXECUTION_ID),
        **kwargs,
        "message": msg,
    }
    method = getattr(logger, level)
    method(json.dumps(record, default=str))
