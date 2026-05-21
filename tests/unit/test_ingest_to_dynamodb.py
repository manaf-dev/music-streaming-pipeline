"""Unit tests for the DynamoDB ingest Glue job.

Targets the pure item-builder functions and the batch-write orchestration
through ``ingest()``. The Glue entrypoint ``main()`` and the S3 Parquet read
are exercised in Phase 8 integration tests.
"""

from __future__ import annotations

from collections.abc import Generator
from decimal import Decimal

import boto3
import pandas as pd
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from src.glue_jobs.ingest_to_dynamodb import (
    build_genre_kpi_item,
    build_top_genres_item,
    build_top_songs_item,
    ingest_dataframes,
)

_TABLE = "test-kpi-table"


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


# ---------------------------------------------------------------------------
# Item builders
# ---------------------------------------------------------------------------
def test_build_genre_kpi_item_produces_correct_keys_and_attrs() -> None:
    row = {
        "track_genre": "pop",
        "listen_date": pd.Timestamp("2024-01-15").date(),
        "listen_count": 4521,
        "unique_listeners": 312,
        "total_listening_time_ms": 13_254_678,
        "avg_listening_time_per_user_ms": 42467.56,
    }
    item = build_genre_kpi_item(row)
    assert item["pk"] == "GENRE_KPI#pop#2024-01-15"
    assert item["sk"] == "METADATA"
    assert item["genre"] == "pop"
    assert item["date"] == "2024-01-15"
    assert item["listen_count"] == 4521
    assert item["unique_listeners"] == 312
    assert item["total_listening_time_ms"] == 13_254_678
    assert item["avg_listening_time_per_user_ms"] == 42467.56


def test_build_top_songs_item_produces_zero_padded_sk() -> None:
    row = {
        "track_genre": "pop",
        "listen_date": pd.Timestamp("2024-01-15").date(),
        "rank": 1,
        "track_id": "t-001",
        "track_name": "Song One",
        "artists": "Artist A",
        "play_count": 99,
    }
    item = build_top_songs_item(row)
    assert item["pk"] == "TOP_SONGS#pop#2024-01-15"
    assert item["sk"] == "RANK#01"  # zero-padded to two digits
    assert item["rank"] == 1
    assert item["track_id"] == "t-001"
    assert item["track_name"] == "Song One"
    assert item["artists"] == "Artist A"
    assert item["play_count"] == 99


def test_build_top_songs_item_pads_double_digit_rank() -> None:
    row = {
        "track_genre": "pop",
        "listen_date": pd.Timestamp("2024-01-15").date(),
        "rank": 10,
        "track_id": "t-010",
        "track_name": "Song Ten",
        "artists": "Artist B",
        "play_count": 5,
    }
    assert build_top_songs_item(row)["sk"] == "RANK#10"


def test_build_top_genres_item_omits_genre_from_pk() -> None:
    row = {
        "listen_date": pd.Timestamp("2024-01-15").date(),
        "rank": 2,
        "track_genre": "rock",
        "listen_count": 1200,
    }
    item = build_top_genres_item(row)
    assert item["pk"] == "TOP_GENRES#2024-01-15"
    assert item["sk"] == "RANK#02"
    assert item["genre"] == "rock"
    assert item["listen_count"] == 1200
    assert item["rank"] == 2


def test_no_float_attributes_after_sanitisation() -> None:
    # avg_listening_time_per_user_ms is the only float field; ingest_dataframes
    # routes items through sanitize_for_dynamodb before writing, which converts
    # float -> Decimal. Verify directly via the helper used by the job.
    from src.utils.dynamodb_helpers import sanitize_for_dynamodb

    row = {
        "track_genre": "pop",
        "listen_date": pd.Timestamp("2024-01-15").date(),
        "listen_count": 1,
        "unique_listeners": 1,
        "total_listening_time_ms": 100,
        "avg_listening_time_per_user_ms": 100.5,
    }
    item = build_genre_kpi_item(row)
    sanitised = sanitize_for_dynamodb(item)
    assert isinstance(sanitised["avg_listening_time_per_user_ms"], Decimal)
    # Verify no naked floats remain anywhere
    assert not any(isinstance(v, float) for v in sanitised.values())


# ---------------------------------------------------------------------------
# ingest_dataframes — orchestration over moto
# ---------------------------------------------------------------------------
def _make_genre_kpis_df(n: int) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "track_genre": f"g{i}",
                "listen_date": pd.Timestamp("2024-01-15").date(),
                "listen_count": i + 1,
                "unique_listeners": i + 1,
                "total_listening_time_ms": (i + 1) * 100,
                "avg_listening_time_per_user_ms": 100.0,
            }
            for i in range(n)
        ]
    )


def test_ingest_writes_more_than_25_genre_kpis_in_one_call(
    kpi_table: boto3.resource,
) -> None:
    genre_kpis = _make_genre_kpis_df(30)
    top_songs = pd.DataFrame(
        columns=[
            "track_genre",
            "listen_date",
            "rank",
            "track_id",
            "track_name",
            "artists",
            "play_count",
        ]
    )
    top_genres = pd.DataFrame(columns=["listen_date", "rank", "track_genre", "listen_count"])

    ingest_dataframes(
        table=kpi_table,
        genre_kpis=genre_kpis,
        top_songs=top_songs,
        top_genres=top_genres,
    )
    assert kpi_table.scan()["Count"] == 30


def test_ingest_is_idempotent_on_rerun_same_pk_sk(kpi_table: boto3.resource) -> None:
    genre_kpis = pd.DataFrame(
        [
            {
                "track_genre": "pop",
                "listen_date": pd.Timestamp("2024-01-15").date(),
                "listen_count": 100,
                "unique_listeners": 10,
                "total_listening_time_ms": 1_000_000,
                "avg_listening_time_per_user_ms": 100_000.0,
            }
        ]
    )
    empty_songs = pd.DataFrame(
        columns=[
            "track_genre",
            "listen_date",
            "rank",
            "track_id",
            "track_name",
            "artists",
            "play_count",
        ]
    )
    empty_genres = pd.DataFrame(columns=["listen_date", "rank", "track_genre", "listen_count"])

    ingest_dataframes(
        table=kpi_table,
        genre_kpis=genre_kpis,
        top_songs=empty_songs,
        top_genres=empty_genres,
    )
    # Rewrite with a different listen_count — same pk+sk, value should be replaced
    genre_kpis.loc[0, "listen_count"] = 200
    ingest_dataframes(
        table=kpi_table,
        genre_kpis=genre_kpis,
        top_songs=empty_songs,
        top_genres=empty_genres,
    )
    response = kpi_table.get_item(Key={"pk": "GENRE_KPI#pop#2024-01-15", "sk": "METADATA"})
    assert response["Item"]["listen_count"] == 200
    assert kpi_table.scan()["Count"] == 1


def test_ingest_writes_all_three_item_types(kpi_table: boto3.resource) -> None:
    genre_kpis = _make_genre_kpis_df(1)
    top_songs = pd.DataFrame(
        [
            {
                "track_genre": "g0",
                "listen_date": pd.Timestamp("2024-01-15").date(),
                "rank": 1,
                "track_id": "t1",
                "track_name": "S1",
                "artists": "A",
                "play_count": 50,
            }
        ]
    )
    top_genres = pd.DataFrame(
        [
            {
                "listen_date": pd.Timestamp("2024-01-15").date(),
                "rank": 1,
                "track_genre": "g0",
                "listen_count": 50,
            }
        ]
    )
    ingest_dataframes(
        table=kpi_table,
        genre_kpis=genre_kpis,
        top_songs=top_songs,
        top_genres=top_genres,
    )
    items = kpi_table.scan()["Items"]
    pks = sorted(item["pk"] for item in items)
    assert any(pk.startswith("GENRE_KPI#") for pk in pks)
    assert any(pk.startswith("TOP_SONGS#") for pk in pks)
    assert any(pk.startswith("TOP_GENRES#") for pk in pks)


def test_batch_write_re_raises_unexpected_client_error(
    aws_credentials: None,  # — fixture sets env vars only
) -> None:
    """If the underlying batch_writer surfaces a non-retryable ClientError
    (e.g. table does not exist), the helper must propagate it so Step Functions
    can route to NotifyFailure rather than silently succeeding."""
    from src.utils.dynamodb_helpers import batch_write_items

    with mock_aws():
        resource = boto3.resource("dynamodb", region_name="us-east-1")
        # Intentionally point at a table that was never created
        missing_table = resource.Table("does-not-exist")
        with pytest.raises(ClientError) as exc:
            batch_write_items(
                missing_table,
                [{"pk": "X", "sk": "Y", "value": 1}],
            )
        assert exc.value.response["Error"]["Code"] in {
            "ResourceNotFoundException",
            "ValidationException",
        }
