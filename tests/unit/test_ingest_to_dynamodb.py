"""Unit tests for the DynamoDB ingest job (partials + recompute merge).

Drives ``merge_partials`` against a moto-mocked table and asserts that daily
KPIs aggregate correctly across multiple files for the same day, that the
unique-listener count is a true union, and that re-running an execution is
idempotent. The Glue entrypoint ``main()`` and the S3 read are integration-tested.
"""

from __future__ import annotations

from collections.abc import Generator
from decimal import Decimal

import boto3
import pandas as pd
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from src.glue_jobs.ingest_to_dynamodb import merge_partials

_TABLE = "test-kpi-table"
_DAY = "2024-01-15"


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


def _genre_df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(
        rows,
        columns=[
            "track_genre",
            "listen_date",
            "listen_count",
            "total_listening_time_ms",
            "user_ids",
        ],
    )


def _song_df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(
        rows,
        columns=["track_genre", "listen_date", "track_id", "track_name", "artists", "play_count"],
    )


def _get(table, pk, sk):
    return table.get_item(Key={"pk": pk, "sk": sk}).get("Item")


def _query(table, pk):
    from boto3.dynamodb.conditions import Key

    return table.query(KeyConditionExpression=Key("pk").eq(pk))["Items"]


# ---------------------------------------------------------------------------
# Single file
# ---------------------------------------------------------------------------
def test_single_file_computes_genre_kpi(kpi_table: boto3.resource) -> None:
    genre = _genre_df(
        [
            {
                "track_genre": "pop",
                "listen_date": _DAY,
                "listen_count": 3,
                "total_listening_time_ms": 300_000,
                "user_ids": ["u1", "u2", "u3"],
            }
        ]
    )
    songs = _song_df(
        [
            {
                "track_genre": "pop",
                "listen_date": _DAY,
                "track_id": "t1",
                "track_name": "S1",
                "artists": "A",
                "play_count": 3,
            }
        ]
    )
    merge_partials(kpi_table, genre, songs, execution_id="e1")

    item = _get(kpi_table, "GENRE_KPI#pop#2024-01-15", "METADATA")
    assert int(item["listen_count"]) == 3
    assert int(item["unique_listeners"]) == 3
    assert int(item["total_listening_time_ms"]) == 300_000
    assert item["avg_listening_time_per_user_ms"] == Decimal("100000")


# ---------------------------------------------------------------------------
# Multiple files for the same day -> aggregate (the core requirement)
# ---------------------------------------------------------------------------
def test_two_files_same_day_aggregate_counts_and_union_listeners(
    kpi_table: boto3.resource,
) -> None:
    # File 1
    merge_partials(
        kpi_table,
        _genre_df(
            [
                {
                    "track_genre": "pop",
                    "listen_date": _DAY,
                    "listen_count": 2,
                    "total_listening_time_ms": 300_000,
                    "user_ids": ["u1", "u2"],
                }
            ]
        ),
        _song_df(
            [
                {
                    "track_genre": "pop",
                    "listen_date": _DAY,
                    "track_id": "t1",
                    "track_name": "S1",
                    "artists": "A",
                    "play_count": 2,
                }
            ]
        ),
        execution_id="e1",
    )
    # File 2 (same day, overlapping user u2)
    merge_partials(
        kpi_table,
        _genre_df(
            [
                {
                    "track_genre": "pop",
                    "listen_date": _DAY,
                    "listen_count": 3,
                    "total_listening_time_ms": 450_000,
                    "user_ids": ["u2", "u3", "u4"],
                }
            ]
        ),
        _song_df(
            [
                {
                    "track_genre": "pop",
                    "listen_date": _DAY,
                    "track_id": "t1",
                    "track_name": "S1",
                    "artists": "A",
                    "play_count": 1,
                },
                {
                    "track_genre": "pop",
                    "listen_date": _DAY,
                    "track_id": "t2",
                    "track_name": "S2",
                    "artists": "B",
                    "play_count": 3,
                },
            ]
        ),
        execution_id="e2",
    )

    item = _get(kpi_table, "GENRE_KPI#pop#2024-01-15", "METADATA")
    assert int(item["listen_count"]) == 5  # 2 + 3
    assert int(item["total_listening_time_ms"]) == 750_000  # 300k + 450k
    # union of {u1,u2} and {u2,u3,u4} = {u1,u2,u3,u4} -> 4 distinct
    assert int(item["unique_listeners"]) == 4
    assert item["avg_listening_time_per_user_ms"] == Decimal("187500")

    # Top songs: t1 = 2+1 = 3, t2 = 3 -> tie broken by track_id asc
    songs = sorted(_query(kpi_table, "TOP_SONGS#pop#2024-01-15"), key=lambda r: r["sk"])
    assert [(s["track_id"], int(s["play_count"])) for s in songs] == [("t1", 3), ("t2", 3)]

    # Top genres for the day
    genres = _query(kpi_table, "TOP_GENRES#2024-01-15")
    assert [(g["genre"], int(g["listen_count"])) for g in genres] == [("pop", 5)]


# ---------------------------------------------------------------------------
# Idempotency — re-running the same execution must not double-count
# ---------------------------------------------------------------------------
def test_rerunning_same_execution_is_idempotent(kpi_table: boto3.resource) -> None:
    args = (
        _genre_df(
            [
                {
                    "track_genre": "pop",
                    "listen_date": _DAY,
                    "listen_count": 4,
                    "total_listening_time_ms": 400_000,
                    "user_ids": ["u1", "u2"],
                }
            ]
        ),
        _song_df(
            [
                {
                    "track_genre": "pop",
                    "listen_date": _DAY,
                    "track_id": "t1",
                    "track_name": "S1",
                    "artists": "A",
                    "play_count": 4,
                }
            ]
        ),
    )
    merge_partials(kpi_table, *args, execution_id="e1")
    merge_partials(kpi_table, *args, execution_id="e1")  # retry of the same execution

    item = _get(kpi_table, "GENRE_KPI#pop#2024-01-15", "METADATA")
    assert int(item["listen_count"]) == 4  # not 8
    assert int(item["unique_listeners"]) == 2


def test_top_genres_ranks_by_listen_count_desc(kpi_table: boto3.resource) -> None:
    merge_partials(
        kpi_table,
        _genre_df(
            [
                {
                    "track_genre": "pop",
                    "listen_date": _DAY,
                    "listen_count": 10,
                    "total_listening_time_ms": 1000,
                    "user_ids": ["u1"],
                },
                {
                    "track_genre": "rock",
                    "listen_date": _DAY,
                    "listen_count": 20,
                    "total_listening_time_ms": 2000,
                    "user_ids": ["u2"],
                },
            ]
        ),
        _song_df([]),
        execution_id="e1",
    )
    genres = sorted(_query(kpi_table, "TOP_GENRES#2024-01-15"), key=lambda r: r["sk"])
    assert [(g["genre"], int(g["listen_count"])) for g in genres] == [("rock", 20), ("pop", 10)]


# ---------------------------------------------------------------------------
# Error propagation
# ---------------------------------------------------------------------------
def test_batch_write_re_raises_unexpected_client_error(aws_credentials: None) -> None:
    """A non-retryable ClientError (e.g. missing table) must propagate so Step
    Functions routes to NotifyFailure rather than silently succeeding."""
    from src.utils.dynamodb_helpers import batch_write_items

    with mock_aws():
        resource = boto3.resource("dynamodb", region_name="us-east-1")
        missing_table = resource.Table("does-not-exist")
        with pytest.raises(ClientError) as exc:
            batch_write_items(missing_table, [{"pk": "X", "sk": "Y", "value": 1}])
        assert exc.value.response["Error"]["Code"] in {
            "ResourceNotFoundException",
            "ValidationException",
        }
