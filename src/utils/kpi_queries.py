"""Read served KPI items from the music-streaming-kpis DynamoDB table."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import boto3
from boto3.dynamodb.conditions import Key


def get_table(*, table_name: str, region: str) -> Any:
    """Return a boto3 Table handle."""
    return boto3.resource("dynamodb", region_name=region).Table(table_name)


def coerce_number(value: Any) -> int | float:
    """Convert DynamoDB Decimal values to plain int/float for charts and metrics."""
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, int | float):
        return value
    return int(value)


def get_genre_kpi(table: Any, *, genre: str, date: str) -> dict[str, Any] | None:
    """Fetch the served genre KPI row for ``(genre, date)``, or ``None`` if missing."""
    response = table.get_item(Key={"pk": f"GENRE_KPI#{genre}#{date}", "sk": "METADATA"})
    item = response.get("Item")
    if item is None:
        return None
    return {
        "genre": str(item["genre"]),
        "date": str(item["date"]),
        "listen_count": coerce_number(item["listen_count"]),
        "unique_listeners": coerce_number(item["unique_listeners"]),
        "total_listening_time_ms": coerce_number(item["total_listening_time_ms"]),
        "avg_listening_time_per_user_ms": coerce_number(item["avg_listening_time_per_user_ms"]),
    }


def get_top_songs(table: Any, *, genre: str, date: str) -> list[dict[str, Any]]:
    """Return ranked top songs for ``(genre, date)``."""
    response = table.query(
        KeyConditionExpression=(
            Key("pk").eq(f"TOP_SONGS#{genre}#{date}") & Key("sk").begins_with("RANK#")
        ),
    )
    rows = response.get("Items", [])
    return [
        {
            "rank": int(row["rank"]),
            "track_id": str(row["track_id"]),
            "track_name": str(row["track_name"]),
            "artists": str(row["artists"]),
            "play_count": coerce_number(row["play_count"]),
        }
        for row in sorted(rows, key=lambda r: int(r["rank"]))
    ]


def get_top_genres(table: Any, *, date: str) -> list[dict[str, Any]]:
    """Return ranked top genres for ``date``."""
    response = table.query(
        KeyConditionExpression=(
            Key("pk").eq(f"TOP_GENRES#{date}") & Key("sk").begins_with("RANK#")
        ),
    )
    rows = response.get("Items", [])
    return [
        {
            "rank": int(row["rank"]),
            "genre": str(row["genre"]),
            "listen_count": coerce_number(row["listen_count"]),
        }
        for row in sorted(rows, key=lambda r: int(r["rank"]))
    ]
