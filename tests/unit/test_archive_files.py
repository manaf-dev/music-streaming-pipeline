"""Unit tests for the archive Glue job.

Exercise both the pure key-mapping helper and the S3-wired archive flow
through ``archive()`` against moto. The Glue entrypoint ``main()`` is
integration-tested in Phase 8.
"""

from __future__ import annotations

import logging
from collections.abc import Generator

import boto3
import pytest
from moto import mock_aws

from src.glue_jobs.archive_files import archive, derive_archive_key

_BUCKET = "test-music-streaming"
_RAW_KEY = "raw/streams/streams1.csv"
_ARCHIVED_KEY = "archive/streams/streams1.csv"
_CSV_BODY = b"user_id,track_id,listen_time\nu1,t1,2026-05-18T10:00:00\n"


@pytest.fixture(autouse=True)
def _reset_archive_logger() -> Generator[None, None, None]:
    """Re-bind handlers each test so capsys sees emitted records."""
    logging.getLogger("archive_files").handlers.clear()
    yield
    logging.getLogger("archive_files").handlers.clear()


@pytest.fixture
def s3_client(aws_credentials: None) -> Generator[boto3.client, None, None]:
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=_BUCKET)
        yield client


# ---------------------------------------------------------------------------
# derive_archive_key — pure mapping
# ---------------------------------------------------------------------------
def test_derive_archive_key_replaces_raw_with_archive_prefix() -> None:
    assert derive_archive_key("raw/streams/streams1.csv") == "archive/streams/streams1.csv"


def test_derive_archive_key_preserves_nested_subdirs() -> None:
    assert derive_archive_key("raw/streams/2024/01/file.csv") == "archive/streams/2024/01/file.csv"


def test_derive_archive_key_rejects_non_raw_prefix() -> None:
    with pytest.raises(ValueError):
        derive_archive_key("processed/something.csv")


# ---------------------------------------------------------------------------
# archive — moto-backed happy path + idempotency
# ---------------------------------------------------------------------------
def test_archive_copies_then_deletes_source(s3_client: boto3.client) -> None:
    s3_client.put_object(Bucket=_BUCKET, Key=_RAW_KEY, Body=_CSV_BODY)
    archive(bucket=_BUCKET, s3_key=_RAW_KEY, s3_client=s3_client)

    archived = s3_client.get_object(Bucket=_BUCKET, Key=_ARCHIVED_KEY)
    assert archived["Body"].read() == _CSV_BODY

    listing = s3_client.list_objects_v2(Bucket=_BUCKET, Prefix=_RAW_KEY)
    assert listing.get("KeyCount", 0) == 0


def test_archive_is_idempotent_when_source_already_deleted(
    s3_client: boto3.client,
    capsys: pytest.CaptureFixture[str],
) -> None:
    s3_client.put_object(Bucket=_BUCKET, Key=_RAW_KEY, Body=_CSV_BODY)
    # First archive succeeds normally
    archive(bucket=_BUCKET, s3_key=_RAW_KEY, s3_client=s3_client)
    capsys.readouterr()  # drain

    # Re-run — source key is already gone. archive() must not raise; it should
    # log a warning and return cleanly so Step Functions can succeed.
    archive(bucket=_BUCKET, s3_key=_RAW_KEY, s3_client=s3_client)

    captured = capsys.readouterr().out
    assert "warning" in captured.lower() or "already" in captured.lower()
