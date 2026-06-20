"""DynamoDB helpers for sanitisation, batched writes, and idempotent KPI merge.

Provides Decimal sanitisation, batched ``put_item``, and the
partial-and-recompute primitives used to aggregate daily KPIs across many files.

Why ``overwrite_by_pkeys`` is non-negotiable for batch writes
-------------------------------------------------------------
``Table.batch_writer()`` refuses two items with the same primary key inside one
25-item batch unless you pass ``overwrite_by_pkeys`` — rank writers re-emit the
same ``(pk, sk)`` across a batch. Without the flag DynamoDB raises
``ValidationException: Provided list of item keys contains duplicates``.

Why we always pre-convert floats to Decimal
-------------------------------------------
DynamoDB rejects native ``float`` values — they MUST be ``decimal.Decimal``.
``Decimal(str(value))`` (not ``Decimal(value)``) avoids binary-float drift.

Idempotent daily aggregation (partials + recompute)
---------------------------------------------------
Daily KPIs must aggregate across *every* file for a day, yet each pipeline stage
must be safely re-runnable. A naive ``ADD`` counter would
double-count on retries and race on concurrent same-day files. Instead:

* Each execution writes its **own** contribution as overwrite-keyed partials:
  - ``GENRE_PARTIAL#<genre>#<date>`` / ``EXEC#<execution_id>``
    -> listen_count, total_listening_time_ms, user_ids (String Set)
  - ``SONG_PARTIAL#<genre>#<date>`` / ``EXEC#<execution_id>#TRACK#<id>``
    -> play_count, track_name, artists
  Re-running the same execution overwrites identical items -> idempotent.
* The **served** items are then *recomputed* (overwritten) from all partials for
  the day, so they always reflect every file:
  - ``GENRE_KPI#<genre>#<date>`` / ``METADATA``
  - ``TOP_SONGS#<genre>#<date>`` / ``RANK#nn``  (Top-3)
  - ``TOP_GENRES#<date>`` / ``RANK#nn``         (Top-5)
  - ``GENRECOUNT#<date>`` / ``GENRE#<genre>``   (ranking accumulator, queryable by day)

``unique_listeners`` is the size of the *union* of the per-execution user sets —
distinct counts are not additive, so the raw user ids are kept on the partials.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from boto3.dynamodb.conditions import Key

_DEFAULT_PKEYS: list[str] = ["pk", "sk"]

TOP_SONGS_PER_GENRE = 3
TOP_GENRES_PER_DAY = 5
TTL_RETENTION_DAYS = 90


def sanitize_for_dynamodb(item: dict[str, Any]) -> dict[str, Any]:
    """Recursively replace ``float`` values with ``Decimal`` for DynamoDB compatibility."""
    return {key: _convert_value(value) for key, value in item.items()}


def expires_at_for_date(date_str: str, *, retention_days: int = TTL_RETENTION_DAYS) -> int:
    """Return a Unix epoch TTL value ``retention_days`` after the KPI date (UTC)."""
    kpi_day = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=UTC)
    expire_at = kpi_day + timedelta(days=retention_days)
    return int(expire_at.timestamp())


def _attach_ttl(item: dict[str, Any], *, date: str) -> dict[str, Any]:
    item["expires_at"] = expires_at_for_date(date)
    return item


def _convert_value(value: Any) -> Any:
    if isinstance(value, bool):
        # bool is a subclass of int — keep its native form, never coerce to Decimal
        return value
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, dict):
        return {key: _convert_value(inner) for key, inner in value.items()}
    if isinstance(value, list):
        return [_convert_value(inner) for inner in value]
    return value


def batch_write_items(
    table: Any,
    items: list[dict[str, Any]],
    overwrite_by_pkeys: list[str] | None = None,
) -> None:
    """Sanitize and batch-write ``items`` to ``table`` (idempotent on ``overwrite_by_pkeys``)."""
    keys = overwrite_by_pkeys if overwrite_by_pkeys is not None else list(_DEFAULT_PKEYS)
    with table.batch_writer(overwrite_by_pkeys=keys) as batch:
        for raw_item in items:
            batch.put_item(Item=sanitize_for_dynamodb(raw_item))


# ---------------------------------------------------------------------------
# Partial writers (one execution's idempotent contribution)
# ---------------------------------------------------------------------------
def _delete_stale_rank_slots(table: Any, pk: str, *, keep_sks: set[str]) -> None:
    """Remove rank slots that fell out of the current Top-N."""
    for item in _query_all(table, pk):
        sk = str(item["sk"])
        if sk.startswith("RANK#") and sk not in keep_sks:
            table.delete_item(Key={"pk": pk, "sk": sk})


def _query_all(table: Any, pk: str) -> list[dict[str, Any]]:
    """Return every item under partition key ``pk`` (handling pagination)."""
    items: list[dict[str, Any]] = []
    kwargs: dict[str, Any] = {"KeyConditionExpression": Key("pk").eq(pk)}
    while True:
        resp = table.query(**kwargs)
        items.extend(resp.get("Items", []))
        last = resp.get("LastEvaluatedKey")
        if not last:
            return items
        kwargs["ExclusiveStartKey"] = last


def put_genre_partial(
    table: Any,
    *,
    genre: str,
    date: str,
    execution_id: str,
    listen_count: int,
    total_listening_time_ms: int,
    user_ids: set[str],
) -> None:
    """Overwrite this execution's genre contribution (idempotent on re-run)."""
    users = {str(u) for u in user_ids if str(u) != ""}
    item: dict[str, Any] = {
        "pk": f"GENRE_PARTIAL#{genre}#{date}",
        "sk": f"EXEC#{execution_id}",
        "genre": genre,
        "date": date,
        "listen_count": int(listen_count),
        "total_listening_time_ms": int(total_listening_time_ms),
    }
    # DynamoDB String Sets cannot be empty; only attach when non-empty.
    if users:
        item["user_ids"] = users
    table.put_item(Item=sanitize_for_dynamodb(_attach_ttl(item, date=date)))


def put_song_partials(table: Any, rows: list[dict[str, Any]]) -> None:
    """Overwrite this execution's per-track contributions (idempotent on re-run).

    ``rows`` items require: genre, date, execution_id, track_id, track_name,
    artists, play_count.
    """
    items: list[dict[str, Any]] = [
        _attach_ttl(
            {
                "pk": f"SONG_PARTIAL#{row['genre']}#{row['date']}",
                "sk": f"EXEC#{row['execution_id']}#TRACK#{row['track_id']}",
                "track_id": str(row["track_id"]),
                "track_name": str(row["track_name"]),
                "artists": str(row["artists"]),
                "play_count": int(row["play_count"]),
            },
            date=str(row["date"]),
        )
        for row in rows
    ]
    if items:
        batch_write_items(table, items)


# ---------------------------------------------------------------------------
# Recompute served KPIs from all partials for a day (overwrite = idempotent)
# ---------------------------------------------------------------------------
def recompute_genre_kpi(table: Any, *, genre: str, date: str) -> None:
    """Recompute the daily genre KPI from every execution's partials for the day."""
    partials = _query_all(table, f"GENRE_PARTIAL#{genre}#{date}")
    listen_count = sum(int(p["listen_count"]) for p in partials)
    total = sum(int(p["total_listening_time_ms"]) for p in partials)
    users: set[str] = set()
    for p in partials:
        users |= set(p.get("user_ids", set()))
    unique = len(users)
    avg = Decimal(str(round(total / unique, 2))) if unique else Decimal("0")
    batch_write_items(
        table,
        [
            _attach_ttl(
                {
                    "pk": f"GENRE_KPI#{genre}#{date}",
                    "sk": "METADATA",
                    "genre": genre,
                    "date": date,
                    "listen_count": listen_count,
                    "unique_listeners": unique,
                    "total_listening_time_ms": total,
                    "avg_listening_time_per_user_ms": avg,
                },
                date=date,
            ),
            _attach_ttl(
                {
                    "pk": f"GENRECOUNT#{date}",
                    "sk": f"GENRE#{genre}",
                    "genre": genre,
                    "listen_count": listen_count,
                },
                date=date,
            ),
        ],
    )


def recompute_top_songs(
    table: Any, *, genre: str, date: str, top_n: int = TOP_SONGS_PER_GENRE
) -> None:
    """Recompute Top-N songs for a genre+day from all per-execution song partials."""
    agg: dict[str, dict[str, Any]] = {}
    for p in _query_all(table, f"SONG_PARTIAL#{genre}#{date}"):
        tid = str(p["track_id"])
        entry = agg.setdefault(
            tid,
            {
                "play_count": 0,
                "track_name": str(p.get("track_name", "")),
                "artists": str(p.get("artists", "")),
            },
        )
        entry["play_count"] += int(p["play_count"])
    ranked = sorted(agg.items(), key=lambda kv: (-kv[1]["play_count"], kv[0]))[:top_n]
    pk = f"TOP_SONGS#{genre}#{date}"
    items: list[dict[str, Any]] = [
        _attach_ttl(
            {
                "pk": pk,
                "sk": f"RANK#{rank:02d}",
                "rank": rank,
                "track_id": tid,
                "track_name": entry["track_name"],
                "artists": entry["artists"],
                "play_count": int(entry["play_count"]),
            },
            date=date,
        )
        for rank, (tid, entry) in enumerate(ranked, start=1)
    ]
    keep_sks = {item["sk"] for item in items}
    _delete_stale_rank_slots(table, pk, keep_sks=keep_sks)
    if items:
        batch_write_items(table, items)


def recompute_top_genres(table: Any, *, date: str, top_n: int = TOP_GENRES_PER_DAY) -> None:
    """Recompute Top-N genres for a day from the GENRECOUNT ranking accumulator."""
    rows = sorted(
        _query_all(table, f"GENRECOUNT#{date}"),
        key=lambda r: (-int(r["listen_count"]), str(r["genre"])),
    )[:top_n]
    pk = f"TOP_GENRES#{date}"
    items: list[dict[str, Any]] = [
        _attach_ttl(
            {
                "pk": pk,
                "sk": f"RANK#{rank:02d}",
                "rank": rank,
                "genre": str(row["genre"]),
                "listen_count": int(row["listen_count"]),
            },
            date=date,
        )
        for rank, row in enumerate(rows, start=1)
    ]
    keep_sks = {item["sk"] for item in items}
    _delete_stale_rank_slots(table, pk, keep_sks=keep_sks)
    if items:
        batch_write_items(table, items)
