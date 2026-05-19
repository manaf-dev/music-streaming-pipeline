import json
import logging
import os

import pytest

from src.utils.logger import get_logger, log


def test_get_logger_returns_logger_with_name() -> None:
    logger = get_logger("job-a")
    assert isinstance(logger, logging.Logger)
    assert logger.name == "job-a"


def test_get_logger_is_idempotent_does_not_duplicate_handlers() -> None:
    first = get_logger("job-idempotent")
    handler_count = len(first.handlers)
    second = get_logger("job-idempotent")
    assert second is first
    assert len(second.handlers) == handler_count


def test_log_emits_valid_json_with_required_keys(
    capsys: pytest.CaptureFixture[str],
) -> None:
    os.environ.pop("EXECUTION_ID", None)
    logger = get_logger("job-json")
    log(logger, "info", "hello world")
    captured = capsys.readouterr().out.strip().splitlines()
    assert captured, "expected at least one log line on stdout"
    record = json.loads(captured[-1])
    assert record["level"] == "info"
    assert record["job_name"] == "job-json"
    assert record["execution_id"] == "local"  # default when EXECUTION_ID not set
    assert record["message"] == "hello world"


def test_log_reads_execution_id_from_environment(
    capsys: pytest.CaptureFixture[str],
) -> None:
    os.environ["EXECUTION_ID"] = "exec-abc-123"
    try:
        logger = get_logger("job-env")
        log(logger, "info", "with execution id")
        record = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
        assert record["execution_id"] == "exec-abc-123"
    finally:
        os.environ.pop("EXECUTION_ID", None)


def test_log_includes_extra_kwargs(capsys: pytest.CaptureFixture[str]) -> None:
    logger = get_logger("job-kwargs")
    log(logger, "info", "structured", rows=42, status="ok")
    record = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert record["rows"] == 42
    assert record["status"] == "ok"


def test_log_routes_warning_level(capsys: pytest.CaptureFixture[str]) -> None:
    logger = get_logger("job-levels")
    log(logger, "warning", "watch out")
    record = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert record["level"] == "warning"
