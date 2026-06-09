"""DynamoDB read helpers for the music streaming KPI dashboard."""

from __future__ import annotations

import re
from decimal import Decimal
from typing import Any

import boto3
import pandas as pd
from boto3.dynamodb.conditions import Attr

_TABLE_NAME = "prod-music-streaming-kpis"
_REGION = "eu-central-1"


def _coerce_float(value: Any) -> float:
    """Coerce a DynamoDB numeric (Decimal, str, int) to float."""
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _is_valid_genre(genre: str) -> bool:
    """Return True if genre contains at least one letter (filters numeric dirty data)."""
    return bool(re.search(r"[a-zA-Z]", genre))


def _paginated_scan(table: Any, prefix: str) -> list[dict[str, Any]]:
    """Scan table for items whose pk starts with prefix, handling pagination."""
    items: list[dict[str, Any]] = []
    kwargs: dict[str, Any] = {"FilterExpression": Attr("pk").begins_with(prefix)}
    while True:
        resp = table.scan(**kwargs)
        items.extend(resp.get("Items", []))
        if "LastEvaluatedKey" not in resp:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    return items


def get_genre_kpis(
    table_name: str = _TABLE_NAME,
    region: str = _REGION,
) -> pd.DataFrame:
    """Fetch all GENRE_KPI items and return as a DataFrame.

    Args:
        table_name: DynamoDB table name.
        region: AWS region.

    Returns:
        DataFrame with columns: genre, date, listen_count, unique_listeners,
        avg_listening_time_ms, total_listening_time_ms.
    """
    table = boto3.resource("dynamodb", region_name=region).Table(table_name)
    rows = []
    for item in _paginated_scan(table, "GENRE_KPI#"):
        genre = str(item.get("genre", ""))
        if not _is_valid_genre(genre):
            continue
        rows.append(
            {
                "genre": genre,
                "date": str(item.get("date", "")),
                "listen_count": int(_coerce_float(item.get("listen_count", 0))),
                "unique_listeners": int(_coerce_float(item.get("unique_listeners", 0))),
                "avg_listening_time_ms": _coerce_float(
                    item.get("avg_listening_time_per_user_ms", 0)
                ),
                "total_listening_time_ms": _coerce_float(
                    item.get("total_listening_time_ms", 0)
                ),
            }
        )
    return pd.DataFrame(rows)


def get_top_songs(
    table_name: str = _TABLE_NAME,
    region: str = _REGION,
) -> pd.DataFrame:
    """Fetch all TOP_SONGS items and return as a DataFrame.

    Args:
        table_name: DynamoDB table name.
        region: AWS region.

    Returns:
        DataFrame with columns: genre, date, rank, track_name, artists, play_count.
        Sorted by genre then rank.
    """
    table = boto3.resource("dynamodb", region_name=region).Table(table_name)
    rows = []
    for item in _paginated_scan(table, "TOP_SONGS#"):
        pk = str(item.get("pk", ""))
        parts = pk.split("#")  # TOP_SONGS#<genre>#<date>
        if len(parts) < 3:
            continue
        genre = parts[1]
        if not _is_valid_genre(genre):
            continue
        rows.append(
            {
                "genre": genre,
                "date": parts[2],
                "rank": int(_coerce_float(item.get("rank", 0))),
                "track_name": str(item.get("track_name", "")),
                "artists": str(item.get("artists", "")),
                "play_count": int(_coerce_float(item.get("play_count", 0))),
            }
        )
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(["genre", "rank"]).reset_index(drop=True)
    return df


def get_top_genres(
    table_name: str = _TABLE_NAME,
    region: str = _REGION,
) -> pd.DataFrame:
    """Fetch all TOP_GENRES items and return as a DataFrame.

    Args:
        table_name: DynamoDB table name.
        region: AWS region.

    Returns:
        DataFrame with columns: date, rank, genre, listen_count. Sorted by rank.
    """
    table = boto3.resource("dynamodb", region_name=region).Table(table_name)
    rows = []
    for item in _paginated_scan(table, "TOP_GENRES#"):
        pk = str(item.get("pk", ""))
        date = pk.split("#")[1] if "#" in pk else ""
        rows.append(
            {
                "date": date,
                "rank": int(_coerce_float(item.get("rank", 0))),
                "genre": str(item.get("genre", "")),
                "listen_count": int(_coerce_float(item.get("listen_count", 0))),
            }
        )
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("rank").reset_index(drop=True)
    return df
