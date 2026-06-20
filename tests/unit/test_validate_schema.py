"""Unit tests for the schema-validation Glue job.

Tests target the public ``validate(bucket, s3_key, *, s3_client)`` helper so
they can run without the ``awsglue.utils`` runtime (which is only available
inside the Glue executor). The Glue entrypoint ``main()`` is exercised by the
integration tests in Phase 8.
"""

import logging
from collections.abc import Generator

import boto3
import pytest
from moto import mock_aws

from src.glue_jobs.validate_schema import validate
from src.utils.schema_registry import REFERENCE_DATA_KEYS

_BUCKET = "test-music-streaming"
_KEY = "raw/streams/streams1.csv"

_VALID_CSV = (
    b"user_id,track_id,listen_time\n"
    b"u1,t1,2026-05-18T10:00:00\n"
    b"u2,t2,2026-05-18T10:05:00\n"
    b"u3,t3,2026-05-18T10:10:00\n"
)

_MISSING_TRACK_ID_CSV = (
    b"user_id,listen_time\n" b"u1,2026-05-18T10:00:00\n" b"u2,2026-05-18T10:05:00\n"
)

_EMPTY_CSV = b"user_id,track_id,listen_time\n"  # header only, zero data rows

_UNPARSEABLE_LISTEN_TIME_CSV = (
    b"user_id,track_id,listen_time\n" b"u1,t1,not-a-date\n" b"u2,t2,also-not-a-date\n"
)


@pytest.fixture(autouse=True)
def _reset_validate_schema_logger() -> Generator[None, None, None]:
    """Drop cached handlers so each test re-binds to its own capsys stdout."""
    logging.getLogger("validate_schema").handlers.clear()
    yield
    logging.getLogger("validate_schema").handlers.clear()


@pytest.fixture
def s3_client(aws_credentials: None) -> Generator[boto3.client, None, None]:
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=_BUCKET)
        yield client


def _put(client: boto3.client, body: bytes) -> None:
    client.put_object(Bucket=_BUCKET, Key=_KEY, Body=body)


def _put_reference_data(client: boto3.client) -> None:
    for key in REFERENCE_DATA_KEYS:
        client.put_object(Bucket=_BUCKET, Key=key, Body=b"placeholder\n")


def test_valid_csv_returns_without_raising(s3_client: boto3.client) -> None:
    _put_reference_data(s3_client)
    _put(s3_client, _VALID_CSV)
    # No SystemExit -> success
    validate(bucket=_BUCKET, s3_key=_KEY, s3_client=s3_client)


def test_missing_required_column_exits_with_code_1(s3_client: boto3.client) -> None:
    _put_reference_data(s3_client)
    _put(s3_client, _MISSING_TRACK_ID_CSV)
    with pytest.raises(SystemExit) as exc:
        validate(bucket=_BUCKET, s3_key=_KEY, s3_client=s3_client)
    assert exc.value.code == 1


def test_empty_csv_exits_with_code_1(s3_client: boto3.client) -> None:
    _put_reference_data(s3_client)
    _put(s3_client, _EMPTY_CSV)
    with pytest.raises(SystemExit) as exc:
        validate(bucket=_BUCKET, s3_key=_KEY, s3_client=s3_client)
    assert exc.value.code == 1


def test_unparseable_listen_time_exits_with_code_1(s3_client: boto3.client) -> None:
    _put_reference_data(s3_client)
    _put(s3_client, _UNPARSEABLE_LISTEN_TIME_CSV)
    with pytest.raises(SystemExit) as exc:
        validate(bucket=_BUCKET, s3_key=_KEY, s3_client=s3_client)
    assert exc.value.code == 1


def test_failure_log_records_which_columns_are_missing(
    s3_client: boto3.client, capsys: pytest.CaptureFixture[str]
) -> None:
    _put_reference_data(s3_client)
    _put(s3_client, _MISSING_TRACK_ID_CSV)
    with pytest.raises(SystemExit):
        validate(bucket=_BUCKET, s3_key=_KEY, s3_client=s3_client)
    captured = capsys.readouterr().out
    assert "track_id" in captured
    assert "missing" in captured.lower() or "required" in captured.lower()


def test_missing_reference_data_exits_with_code_1(s3_client: boto3.client) -> None:
    _put(s3_client, _VALID_CSV)
    with pytest.raises(SystemExit) as exc:
        validate(bucket=_BUCKET, s3_key=_KEY, s3_client=s3_client)
    assert exc.value.code == 1
