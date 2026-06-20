from collections.abc import Generator
from decimal import Decimal

import boto3
import pytest
from moto import mock_aws

from src.utils.dynamodb_helpers import (
    batch_write_items,
    expires_at_for_date,
    recompute_top_songs,
    sanitize_for_dynamodb,
)

_TABLE = "test-kpi-table"


# ---------------------------------------------------------------------------
# sanitize_for_dynamodb — pure-function tests, no AWS required
# ---------------------------------------------------------------------------
def test_sanitize_converts_float_to_decimal() -> None:
    result = sanitize_for_dynamodb({"pk": "GENRE_KPI#pop#2026-05-18", "avg": 3.14})
    assert result["avg"] == Decimal("3.14")
    assert isinstance(result["avg"], Decimal)


def test_sanitize_preserves_int_str_and_decimal_unchanged() -> None:
    result = sanitize_for_dynamodb({"count": 42, "name": "pop", "ratio": Decimal("0.5")})
    assert result == {"count": 42, "name": "pop", "ratio": Decimal("0.5")}


def test_sanitize_preserves_bool_does_not_coerce_to_decimal() -> None:
    # bool is a subclass of int — must NOT be converted to Decimal
    result = sanitize_for_dynamodb({"flag": True, "other": False})
    assert result["flag"] is True
    assert result["other"] is False


def test_sanitize_recurses_into_nested_dict_and_list() -> None:
    item = {
        "pk": "X",
        "meta": {"score": 1.5, "tags": ["a", 2.25]},
    }
    out = sanitize_for_dynamodb(item)
    assert out["meta"]["score"] == Decimal("1.5")
    assert out["meta"]["tags"][1] == Decimal("2.25")
    assert out["meta"]["tags"][0] == "a"


# ---------------------------------------------------------------------------
# batch_write_items — exercises DynamoDB via moto
# ---------------------------------------------------------------------------
@pytest.fixture
def dynamodb_table(aws_credentials: None) -> Generator[boto3.resource, None, None]:
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


def test_batch_write_writes_more_than_25_items_in_one_call(
    dynamodb_table: boto3.resource,
) -> None:
    items = [
        {"pk": f"GENRE_KPI#g{i}#2026-05-18", "sk": "METADATA", "listen_count": i} for i in range(30)
    ]
    batch_write_items(dynamodb_table, items)
    scan_result = dynamodb_table.scan()
    assert scan_result["Count"] == 30


def test_batch_write_is_idempotent_on_same_pk_sk(
    dynamodb_table: boto3.resource,
) -> None:
    items = [
        {"pk": "GENRE_KPI#pop#2026-05-18", "sk": "METADATA", "listen_count": 100},
    ]
    batch_write_items(dynamodb_table, items)
    # rewrite with new value — must replace, not duplicate or error
    items[0]["listen_count"] = 200
    batch_write_items(dynamodb_table, items)
    response = dynamodb_table.get_item(Key={"pk": "GENRE_KPI#pop#2026-05-18", "sk": "METADATA"})
    assert response["Item"]["listen_count"] == 200
    assert dynamodb_table.scan()["Count"] == 1


def test_batch_write_with_duplicate_pkeys_in_same_batch_keeps_last_value(
    dynamodb_table: boto3.resource,
) -> None:
    # Two items with identical pk+sk in the same batch — overwrite_by_pkeys
    # must dedupe to the last value rather than raising ValidationException
    items = [
        {"pk": "X", "sk": "Y", "listen_count": 1},
        {"pk": "X", "sk": "Y", "listen_count": 2},
    ]
    batch_write_items(dynamodb_table, items)
    response = dynamodb_table.get_item(Key={"pk": "X", "sk": "Y"})
    assert response["Item"]["listen_count"] == 2


def test_expires_at_for_date_is_ninety_days_after_kpi_day() -> None:
    from datetime import UTC, datetime, timedelta

    ttl = expires_at_for_date("2024-01-15")
    expected = datetime(2024, 1, 15, tzinfo=UTC) + timedelta(days=90)
    assert ttl == int(expected.timestamp())


def test_recompute_top_songs_deletes_stale_rank_slots(dynamodb_table: boto3.resource) -> None:
    """When a genre drops from 3 ranked tracks to 2, RANK#03 must be removed."""
    pk = "SONG_PARTIAL#pop#2024-01-15"
    batch_write_items(
        dynamodb_table,
        [
            {
                "pk": pk,
                "sk": "EXEC#e1#TRACK#t1",
                "track_id": "t1",
                "track_name": "A",
                "artists": "X",
                "play_count": 5,
            },
            {
                "pk": pk,
                "sk": "EXEC#e1#TRACK#t2",
                "track_id": "t2",
                "track_name": "B",
                "artists": "Y",
                "play_count": 4,
            },
            {
                "pk": pk,
                "sk": "EXEC#e1#TRACK#t3",
                "track_id": "t3",
                "track_name": "C",
                "artists": "Z",
                "play_count": 3,
            },
        ],
    )
    recompute_top_songs(dynamodb_table, genre="pop", date="2024-01-15")
    assert dynamodb_table.get_item(Key={"pk": "TOP_SONGS#pop#2024-01-15", "sk": "RANK#03"})[
        "Item"
    ]

    # Drop t3 from partials and recompute — only two tracks remain.
    dynamodb_table.delete_item(Key={"pk": pk, "sk": "EXEC#e1#TRACK#t3"})
    recompute_top_songs(dynamodb_table, genre="pop", date="2024-01-15")

    assert (
        dynamodb_table.get_item(Key={"pk": "TOP_SONGS#pop#2024-01-15", "sk": "RANK#03"}).get(
            "Item"
        )
        is None
    )
    assert dynamodb_table.get_item(Key={"pk": "TOP_SONGS#pop#2024-01-15", "sk": "RANK#02"})[
        "Item"
    ]
