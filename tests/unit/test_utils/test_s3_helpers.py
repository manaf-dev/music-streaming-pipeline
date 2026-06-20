from collections.abc import Generator

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from src.utils.s3_helpers import (
    copy_s3_object,
    delete_s3_object,
    derive_archive_key,
    get_s3_client,
    object_exists,
    read_csv_bytes,
)

_BUCKET = "test-music-streaming"
_CSV_BODY = b"user_id,track_id,listen_time\nu1,t1,2026-05-18T10:00:00\n"


@pytest.fixture
def s3_client(aws_credentials: None) -> Generator[boto3.client, None, None]:
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=_BUCKET)
        yield client


def test_get_s3_client_returns_client(aws_credentials: None) -> None:
    with mock_aws():
        client = get_s3_client()
        assert hasattr(client, "get_object")


def test_read_csv_bytes_returns_exact_payload(s3_client: boto3.client) -> None:
    s3_client.put_object(Bucket=_BUCKET, Key="raw/streams/streams1.csv", Body=_CSV_BODY)
    body = read_csv_bytes(s3_client, _BUCKET, "raw/streams/streams1.csv")
    assert body == _CSV_BODY


def test_copy_s3_object_creates_identical_destination(s3_client: boto3.client) -> None:
    s3_client.put_object(Bucket=_BUCKET, Key="raw/streams/streams1.csv", Body=_CSV_BODY)
    copy_s3_object(s3_client, _BUCKET, "raw/streams/streams1.csv", "archive/streams/streams1.csv")
    archived = s3_client.get_object(Bucket=_BUCKET, Key="archive/streams/streams1.csv")
    assert archived["Body"].read() == _CSV_BODY


def test_copy_s3_object_is_idempotent_on_repeat(s3_client: boto3.client) -> None:
    s3_client.put_object(Bucket=_BUCKET, Key="raw/streams/streams1.csv", Body=_CSV_BODY)
    copy_s3_object(s3_client, _BUCKET, "raw/streams/streams1.csv", "archive/streams/streams1.csv")
    # second copy must not raise
    copy_s3_object(s3_client, _BUCKET, "raw/streams/streams1.csv", "archive/streams/streams1.csv")
    archived = s3_client.get_object(Bucket=_BUCKET, Key="archive/streams/streams1.csv")
    assert archived["Body"].read() == _CSV_BODY


def test_delete_s3_object_removes_key(s3_client: boto3.client) -> None:
    s3_client.put_object(Bucket=_BUCKET, Key="raw/streams/streams1.csv", Body=_CSV_BODY)
    delete_s3_object(s3_client, _BUCKET, "raw/streams/streams1.csv")
    with pytest.raises(ClientError) as exc:
        s3_client.head_object(Bucket=_BUCKET, Key="raw/streams/streams1.csv")
    assert exc.value.response["Error"]["Code"] in {"404", "NoSuchKey"}


def test_object_exists_returns_true_when_key_present(s3_client: boto3.client) -> None:
    s3_client.put_object(Bucket=_BUCKET, Key="reference/songs/songs.csv", Body=b"x")
    assert object_exists(s3_client, _BUCKET, "reference/songs/songs.csv") is True


def test_object_exists_returns_false_when_key_missing(s3_client: boto3.client) -> None:
    assert object_exists(s3_client, _BUCKET, "reference/songs/songs.csv") is False


def test_derive_archive_key_maps_raw_prefix_to_archive() -> None:
    assert derive_archive_key("raw/streams/streams1.csv") == "archive/streams/streams1.csv"
