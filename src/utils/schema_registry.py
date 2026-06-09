"""Column schemas used to validate inputs and project reference data.

These constants are the single source of truth for which columns each dataset
must expose. The validator (Glue Python Shell) checks ``REQUIRED_STREAM_COLUMNS``
against the incoming CSV header, and the PySpark transform selects only the
columns it actually needs from the large reference tables to keep the broadcast
joins small.
"""

from __future__ import annotations

#: Columns every ``streams*.csv`` upload MUST contain — validated before transform.
REQUIRED_STREAM_COLUMNS: list[str] = ["user_id", "track_id", "listen_time"]

#: Columns projected from ``songs.csv`` after the broadcast join.
SONGS_SELECT_COLUMNS: list[str] = [
    "track_id",
    "track_name",
    "artists",
    "track_genre",
    "duration_ms",
]

#: Columns projected from ``users.csv`` — only ``user_id`` is needed for the join.
USERS_SELECT_COLUMNS: list[str] = ["user_id"]
