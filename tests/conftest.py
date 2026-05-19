"""Shared pytest fixtures.

* ``spark`` — a SparkSession bound to ``local[*]``, scoped to the whole test
  session so we only pay the JVM startup cost once.
* ``aws_credentials`` — dummy AWS env vars so botocore never accidentally
  reaches the real cloud. Tests that touch AWS depend on this fixture
  alongside their own ``@mock_aws`` context.
* ``make_streams_df`` / ``make_songs_df`` / ``make_users_df`` — tiny fixed
  DataFrames whose schemas mirror the production datasets so KPI tests can
  assert exact aggregate values.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Generator
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

_AWS_ENV_KEYS = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SECURITY_TOKEN",
    "AWS_SESSION_TOKEN",
    "AWS_DEFAULT_REGION",
)


# ---------------------------------------------------------------------------
# Spark
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """Provide a local SparkSession for the duration of the test session."""
    from pyspark.sql import SparkSession  # local import — heavy

    session = (
        SparkSession.builder.master("local[*]")
        .appName("music-streaming-pipeline-tests")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    try:
        yield session
    finally:
        session.stop()


# ---------------------------------------------------------------------------
# AWS credentials — set before any test that touches boto3 / moto
# ---------------------------------------------------------------------------
@pytest.fixture
def aws_credentials() -> Generator[None, None, None]:
    """Set dummy AWS credentials in os.environ for the duration of a test."""
    previous = {key: os.environ.get(key) for key in _AWS_ENV_KEYS}
    os.environ.update(
        {
            "AWS_ACCESS_KEY_ID": "testing",
            "AWS_SECRET_ACCESS_KEY": "testing",
            "AWS_SECURITY_TOKEN": "testing",
            "AWS_SESSION_TOKEN": "testing",
            "AWS_DEFAULT_REGION": "us-east-1",
        }
    )
    try:
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


# ---------------------------------------------------------------------------
# Sample DataFrame factories
# ---------------------------------------------------------------------------
@pytest.fixture
def make_streams_df(spark: SparkSession) -> Callable[[], DataFrame]:
    """Return a factory that builds a tiny streams DataFrame.

    Two users, three tracks, three calendar days — small enough to hand-verify
    every KPI value, large enough to exercise group-by + window logic.
    """
    from datetime import datetime

    from pyspark.sql.types import StringType, StructField, StructType, TimestampType

    schema = StructType(
        [
            StructField("user_id", StringType(), nullable=False),
            StructField("track_id", StringType(), nullable=False),
            StructField("listen_time", TimestampType(), nullable=False),
        ]
    )
    rows = [
        ("u1", "t1", datetime(2026, 5, 18, 10, 0, 0)),
        ("u2", "t1", datetime(2026, 5, 18, 11, 0, 0)),
        ("u1", "t2", datetime(2026, 5, 18, 12, 0, 0)),
        ("u2", "t3", datetime(2026, 5, 19, 9, 0, 0)),
        ("u1", "t3", datetime(2026, 5, 19, 10, 0, 0)),
    ]

    def _build() -> DataFrame:
        return spark.createDataFrame(rows, schema=schema)

    return _build


@pytest.fixture
def make_songs_df(spark: SparkSession) -> Callable[[], DataFrame]:
    """Return a factory that builds a tiny songs reference DataFrame."""
    from pyspark.sql.types import (
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    schema = StructType(
        [
            StructField("track_id", StringType(), nullable=False),
            StructField("track_name", StringType(), nullable=False),
            StructField("artists", StringType(), nullable=False),
            StructField("track_genre", StringType(), nullable=False),
            StructField("duration_ms", IntegerType(), nullable=False),
        ]
    )
    rows = [
        ("t1", "Song One", "Artist A", "pop", 180_000),
        ("t2", "Song Two", "Artist B", "pop", 200_000),
        ("t3", "Song Three", "Artist C", "rock", 240_000),
    ]

    def _build() -> DataFrame:
        return spark.createDataFrame(rows, schema=schema)

    return _build


@pytest.fixture
def make_users_df(spark: SparkSession) -> Callable[[], DataFrame]:
    """Return a factory that builds a tiny users reference DataFrame."""
    from pyspark.sql.types import StringType, StructField, StructType

    schema = StructType([StructField("user_id", StringType(), nullable=False)])
    rows = [("u1",), ("u2",)]

    def _build() -> DataFrame:
        return spark.createDataFrame(rows, schema=schema)

    return _build
