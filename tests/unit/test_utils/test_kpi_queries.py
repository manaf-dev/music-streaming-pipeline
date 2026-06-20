"""Tests for DynamoDB KPI read helpers used by the Streamlit dashboard."""

from decimal import Decimal

import boto3
import pytest
from moto import mock_aws

from src.utils.kpi_queries import coerce_number, get_genre_kpi, get_top_genres, get_top_songs


def test_coerce_number_from_decimal() -> None:
    assert coerce_number(Decimal("42")) == 42
    assert coerce_number(Decimal("3.5")) == 3.5
    assert coerce_number(7) == 7
    assert coerce_number(2.5) == 2.5


def test_get_genre_kpi_missing(kpi_table: boto3.resource) -> None:
    assert get_genre_kpi(kpi_table, genre="jazz", date="2026-06-20") is None


@pytest.fixture
def kpi_table(aws_credentials: None) -> boto3.resource:
    with mock_aws():
        resource = boto3.resource("dynamodb", region_name="eu-central-1")
        table = resource.create_table(
            TableName="music-streaming-kpis",
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
        table.put_item(
            Item={
                "pk": "GENRE_KPI#pop#2026-06-20",
                "sk": "METADATA",
                "genre": "pop",
                "date": "2026-06-20",
                "listen_count": 100,
                "unique_listeners": 10,
                "total_listening_time_ms": 600_000,
                "avg_listening_time_per_user_ms": Decimal("60000"),
            }
        )
        table.put_item(
            Item={
                "pk": "TOP_SONGS#pop#2026-06-20",
                "sk": "RANK#01",
                "rank": 1,
                "track_id": "t1",
                "track_name": "Hit Song",
                "artists": "Artist A",
                "play_count": 50,
            }
        )
        table.put_item(
            Item={
                "pk": "TOP_GENRES#2026-06-20",
                "sk": "RANK#01",
                "rank": 1,
                "genre": "pop",
                "listen_count": 100,
            }
        )
        yield table


def test_get_genre_kpi(kpi_table: boto3.resource) -> None:
    row = get_genre_kpi(kpi_table, genre="pop", date="2026-06-20")
    assert row is not None
    assert row["listen_count"] == 100
    assert row["unique_listeners"] == 10


def test_get_top_songs(kpi_table: boto3.resource) -> None:
    rows = get_top_songs(kpi_table, genre="pop", date="2026-06-20")
    assert len(rows) == 1
    assert rows[0]["track_name"] == "Hit Song"


def test_get_top_genres(kpi_table: boto3.resource) -> None:
    rows = get_top_genres(kpi_table, date="2026-06-20")
    assert rows[0]["genre"] == "pop"
