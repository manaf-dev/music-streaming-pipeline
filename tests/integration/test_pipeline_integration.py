"""End-to-end-ish integration tests for each Glue job + a multi-file scenario.

Each test wires the job's pure logic against moto-backed AWS services (and the
session-scoped SparkSession from conftest.py for the transform). The real
``main()`` Glue entrypoints aren't exercised here — they read awsglue.utils
which only exists inside the Glue runtime; those paths are covered by CI deploy
+ smoke runs.
"""

from __future__ import annotations

import logging
from collections.abc import Generator
from datetime import datetime
from typing import TYPE_CHECKING

import boto3
import pytest
from boto3.dynamodb.conditions import Key
from moto import mock_aws

from src.glue_jobs.ingest_to_dynamodb import merge_partials
from src.glue_jobs.transform_kpis import (
    build_enriched,
    compute_genre_partials,
    compute_song_partials,
)
from src.glue_jobs.validate_schema import validate
from src.utils.s3_helpers import copy_s3_object, delete_s3_object, derive_archive_key
from src.utils.schema_registry import REFERENCE_DATA_KEYS

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_BUCKET = "integration-music-streaming"
_TABLE = "integration-kpi-table"
_VALID_CSV = (
    b"user_id,track_id,listen_time\n" b"u1,t1,2026-05-18T10:00:00\n" b"u2,t2,2026-05-18T10:05:00\n"
)
_INVALID_CSV = b"user_id,listen_time\nu1,2026-05-18T10:00:00\n"  # missing track_id


@pytest.fixture
def s3_bucket(aws_credentials: None) -> Generator[boto3.client, None, None]:
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=_BUCKET)
        yield client


@pytest.fixture
def kpi_table(aws_credentials: None) -> Generator[boto3.resource, None, None]:
    with mock_aws():
        resource = boto3.resource("dynamodb", region_name="us-east-1")
        resource.create_table(
            TableName=_TABLE,
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        yield resource.Table(_TABLE)


@pytest.fixture(autouse=True)
def _reset_named_loggers() -> Generator[None, None, None]:
    """Re-bind logger handlers each test so capsys-based assertions stay reliable."""
    for name in ("validate_schema", "transform_kpis", "ingest_to_dynamodb"):
        logging.getLogger(name).handlers.clear()
    yield


def _get(table, pk, sk):
    return table.get_item(Key={"pk": pk, "sk": sk}).get("Item")


def _query(table, pk):
    return table.query(KeyConditionExpression=Key("pk").eq(pk))["Items"]


def _put_reference_data(client: boto3.client) -> None:
    for key in REFERENCE_DATA_KEYS:
        client.put_object(Bucket=_BUCKET, Key=key, Body=b"placeholder\n")


# ===========================================================================
# 1. validate_schema integration
# ===========================================================================
def test_validate_schema_integration(s3_bucket: boto3.client) -> None:
    _put_reference_data(s3_bucket)
    s3_bucket.put_object(Bucket=_BUCKET, Key="raw/streams/valid.csv", Body=_VALID_CSV)
    s3_bucket.put_object(Bucket=_BUCKET, Key="raw/streams/invalid.csv", Body=_INVALID_CSV)

    validate(bucket=_BUCKET, s3_key="raw/streams/valid.csv", s3_client=s3_bucket)

    with pytest.raises(SystemExit) as exc:
        validate(bucket=_BUCKET, s3_key="raw/streams/invalid.csv", s3_client=s3_bucket)
    assert exc.value.code == 1


# ===========================================================================
# 2. transform_kpis integration — per-file partials produced from raw inputs
# ===========================================================================
def test_transform_kpis_integration(spark: SparkSession) -> None:
    streams = spark.createDataFrame(
        [
            ("u1", "t1", datetime(2026, 5, 18, 10, 0, 0)),
            ("u2", "t1", datetime(2026, 5, 18, 11, 0, 0)),
            ("u1", "t2", datetime(2026, 5, 18, 12, 0, 0)),
        ],
        schema=["user_id", "track_id", "listen_time"],
    )
    songs = spark.createDataFrame(
        [
            ("t1", "Song1", "ArtistA", "pop", 180_000),
            ("t2", "Song2", "ArtistB", "pop", 200_000),
        ],
        schema=["track_id", "track_name", "artists", "track_genre", "duration_ms"],
    )
    users = spark.createDataFrame([("u1",), ("u2",)], schema=["user_id"])

    enriched = build_enriched(streams, songs, users)
    assert enriched.count() == 3

    genre = compute_genre_partials(enriched).collect()[0]
    assert genre["listen_count"] == 3
    assert set(genre["user_ids"]) == {"u1", "u2"}

    songs_p = compute_song_partials(enriched)
    assert set(songs_p.columns) == {
        "track_genre",
        "listen_date",
        "track_id",
        "track_name",
        "artists",
        "play_count",
    }


# ===========================================================================
# 3. transform -> ingest integration on Spark + moto
# ===========================================================================
def test_transform_then_ingest_integration(spark: SparkSession, kpi_table: boto3.resource) -> None:
    streams = spark.createDataFrame(
        [
            ("u1", "t1", datetime(2024, 1, 15, 10, 0, 0)),
            ("u2", "t1", datetime(2024, 1, 15, 11, 0, 0)),
        ],
        schema=["user_id", "track_id", "listen_time"],
    )
    songs = spark.createDataFrame(
        [("t1", "Hit", "Star", "pop", 200_000)],
        schema=["track_id", "track_name", "artists", "track_genre", "duration_ms"],
    )
    users = spark.createDataFrame([("u1",), ("u2",)], schema=["user_id"])
    enriched = build_enriched(streams, songs, users)

    merge_partials(
        kpi_table,
        compute_genre_partials(enriched).toPandas(),
        compute_song_partials(enriched).toPandas(),
        execution_id="exec-1",
    )

    item = _get(kpi_table, "GENRE_KPI#pop#2024-01-15", "METADATA")
    assert int(item["listen_count"]) == 2
    assert int(item["unique_listeners"]) == 2
    assert int(item["total_listening_time_ms"]) == 400_000

    pks = {i["pk"] for i in kpi_table.scan()["Items"]}
    assert "GENRE_KPI#pop#2024-01-15" in pks
    assert "TOP_SONGS#pop#2024-01-15" in pks
    assert "TOP_GENRES#2024-01-15" in pks


# ===========================================================================
# 4. S3 archival helpers — copy + delete (Step Functions uses the AWS SDK)
# ===========================================================================
def test_s3_archive_flow_integration(s3_bucket: boto3.client) -> None:
    raw_key = "raw/streams/streams1.csv"
    archive_key = derive_archive_key(raw_key)
    s3_bucket.put_object(Bucket=_BUCKET, Key=raw_key, Body=_VALID_CSV)

    copy_s3_object(s3_bucket, _BUCKET, raw_key, archive_key)
    delete_s3_object(s3_bucket, _BUCKET, raw_key)

    archived = s3_bucket.get_object(Bucket=_BUCKET, Key=archive_key)
    assert archived["Body"].read() == _VALID_CSV
    assert s3_bucket.list_objects_v2(Bucket=_BUCKET, Prefix="raw/streams/").get("KeyCount", 0) == 0


# ===========================================================================
# 5. Multiple files for the SAME day aggregate into one daily KPI
# ===========================================================================
def test_two_files_same_day_aggregate(spark: SparkSession, kpi_table: boto3.resource) -> None:
    songs = spark.createDataFrame(
        [
            ("t1", "Song1", "A", "pop", 100_000),
            ("t2", "Song2", "B", "pop", 100_000),
        ],
        schema=["track_id", "track_name", "artists", "track_genre", "duration_ms"],
    )
    users = spark.createDataFrame([("u1",), ("u2",), ("u3",)], schema=["user_id"])

    # File 1 (day 2024-01-15): u1, u2 each play t1
    streams1 = spark.createDataFrame(
        [
            ("u1", "t1", datetime(2024, 1, 15, 10, 0, 0)),
            ("u2", "t1", datetime(2024, 1, 15, 11, 0, 0)),
        ],
        schema=["user_id", "track_id", "listen_time"],
    )
    e1 = build_enriched(streams1, songs, users)
    merge_partials(
        kpi_table,
        compute_genre_partials(e1).toPandas(),
        compute_song_partials(e1).toPandas(),
        execution_id="exec-1",
    )

    # File 2 (SAME day): u2 plays t1, u3 plays t2 — overlapping user u2
    streams2 = spark.createDataFrame(
        [
            ("u2", "t1", datetime(2024, 1, 15, 12, 0, 0)),
            ("u3", "t2", datetime(2024, 1, 15, 13, 0, 0)),
        ],
        schema=["user_id", "track_id", "listen_time"],
    )
    e2 = build_enriched(streams2, songs, users)
    merge_partials(
        kpi_table,
        compute_genre_partials(e2).toPandas(),
        compute_song_partials(e2).toPandas(),
        execution_id="exec-2",
    )

    item = _get(kpi_table, "GENRE_KPI#pop#2024-01-15", "METADATA")
    # listen_count = 2 (file1) + 2 (file2) = 4
    assert int(item["listen_count"]) == 4
    # unique = union of {u1,u2} and {u2,u3} = {u1,u2,u3} = 3 (NOT 2+2)
    assert int(item["unique_listeners"]) == 3
    assert int(item["total_listening_time_ms"]) == 400_000

    # Top songs: t1 = 2 (file1) + 1 (file2) = 3, t2 = 1 -> t1 first
    songs_ranked = sorted(_query(kpi_table, "TOP_SONGS#pop#2024-01-15"), key=lambda r: r["sk"])
    assert (songs_ranked[0]["track_id"], int(songs_ranked[0]["play_count"])) == ("t1", 3)
