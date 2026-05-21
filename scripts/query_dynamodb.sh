#!/usr/bin/env bash
# Demo queries for the three KPI access patterns served from the single
# music-streaming-kpis DynamoDB table.
#
# Reads two env vars:
#   TABLE_NAME          (default: dev-music-streaming-kpis)
#   AWS_DEFAULT_REGION  (default: eu-central-1)
#
# Optional positional arg:
#   $1  Date (YYYY-MM-DD) to query against — defaults to today (UTC).

set -euo pipefail

TABLE_NAME="${TABLE_NAME:-dev-music-streaming-kpis}"
AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-eu-central-1}"
QUERY_DATE="${1:-$(date -u +%Y-%m-%d)}"

export TABLE_NAME AWS_DEFAULT_REGION QUERY_DATE

echo "Querying table=${TABLE_NAME} region=${AWS_DEFAULT_REGION} date=${QUERY_DATE}"
echo

uv run python - <<'PY'
"""Three boto3 query patterns against the single-table KPI store."""

import os
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key

TABLE_NAME = os.environ["TABLE_NAME"]
REGION = os.environ["AWS_DEFAULT_REGION"]
DATE = os.environ["QUERY_DATE"]
GENRE = os.environ.get("GENRE", "pop")  # override with: GENRE=rock ./scripts/query_dynamodb.sh

table = boto3.resource("dynamodb", region_name=REGION).Table(TABLE_NAME)


def fmt(value: object) -> object:
    """Render Decimal as int when whole, otherwise as float — cleaner output."""
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    return value


# ---------------------------------------------------------------------------
# Pattern 1 — Genre-Level KPI for one (genre, date)
# ---------------------------------------------------------------------------
print(f"--- Pattern 1: Genre KPI for {GENRE} on {DATE} ---")
resp = table.get_item(Key={"pk": f"GENRE_KPI#{GENRE}#{DATE}", "sk": "METADATA"})
item = resp.get("Item")
if item is None:
    print(f"  No KPI found for genre={GENRE} on {DATE}\n")
else:
    print(f"  listen_count                  = {fmt(item['listen_count'])}")
    print(f"  unique_listeners              = {fmt(item['unique_listeners'])}")
    print(f"  total_listening_time_ms       = {fmt(item['total_listening_time_ms'])}")
    print(f"  avg_listening_time_per_user_ms = {fmt(item['avg_listening_time_per_user_ms'])}")
    print()


# ---------------------------------------------------------------------------
# Pattern 2 — Top songs for one (genre, date)
# ---------------------------------------------------------------------------
print(f"--- Pattern 2: Top songs in {GENRE} on {DATE} ---")
resp = table.query(
    KeyConditionExpression=(
        Key("pk").eq(f"TOP_SONGS#{GENRE}#{DATE}") & Key("sk").begins_with("RANK#")
    )
)
items = sorted(resp.get("Items", []), key=lambda r: int(r["rank"]))
if not items:
    print(f"  No top-songs data for {GENRE} on {DATE}\n")
for row in items:
    print(
        f"  #{int(row['rank'])} {row['track_name']!r:30s} "
        f"by {row['artists']!r:20s} (plays={fmt(row['play_count'])})"
    )
print()


# ---------------------------------------------------------------------------
# Pattern 3 — Top genres for one date
# ---------------------------------------------------------------------------
print(f"--- Pattern 3: Top genres on {DATE} ---")
resp = table.query(
    KeyConditionExpression=(
        Key("pk").eq(f"TOP_GENRES#{DATE}") & Key("sk").begins_with("RANK#")
    )
)
items = sorted(resp.get("Items", []), key=lambda r: int(r["rank"]))
if not items:
    print(f"  No top-genres data for {DATE}\n")
for row in items:
    print(f"  #{int(row['rank'])} {row['genre']!r:20s} (listens={fmt(row['listen_count'])})")
PY
