"""Glue Python Shell — validate an incoming streams CSV before transform.

Reads the CSV from S3 (lazy boto3 client — injectable for moto-based tests)
and enforces three rules before the pipeline proceeds:

1. Every column in :data:`REQUIRED_STREAM_COLUMNS` must be present.
2. The file must contain at least one data row.
3. The first 100 ``listen_time`` values must parse as ISO-8601 timestamps.

On any failure the job emits a structured-JSON error log and exits with
status 1, which Step Functions catches and routes to ``NotifyFailure`` ->
``PipelineFailed``. On success it returns normally; the Glue runner then
exits 0 and Step Functions transitions to ``TransformKPIs``.
"""

from __future__ import annotations

import io
import os
import sys
from typing import Any

import pandas as pd

from src.utils.logger import get_logger, log
from src.utils.s3_helpers import get_s3_client, read_csv_bytes
from src.utils.schema_registry import REQUIRED_STREAM_COLUMNS

_JOB_NAME = "validate_schema"
_LISTEN_TIME_SAMPLE_SIZE = 100


def validate(*, bucket: str, s3_key: str, s3_client: Any | None = None) -> None:
    """Run all schema checks against ``s3://{bucket}/{s3_key}`` or raise SystemExit(1)."""
    logger = get_logger(_JOB_NAME)
    log(logger, "info", "Starting validation", bucket=bucket, s3_key=s3_key)

    client = s3_client if s3_client is not None else get_s3_client()
    body = read_csv_bytes(client, bucket, s3_key)

    try:
        df = pd.read_csv(io.BytesIO(body))
    except Exception as exc:  # pandas parse errors are diverse
        log(logger, "error", "Failed to parse CSV", error=repr(exc))
        sys.exit(1)

    missing = [column for column in REQUIRED_STREAM_COLUMNS if column not in df.columns]
    if missing:
        log(
            logger,
            "error",
            "CSV is missing required columns",
            missing=missing,
            found=list(df.columns),
        )
        sys.exit(1)

    row_count = len(df)
    if row_count == 0:
        log(logger, "error", "CSV has zero data rows (header only)")
        sys.exit(1)

    sample = df["listen_time"].head(_LISTEN_TIME_SAMPLE_SIZE)
    try:
        pd.to_datetime(sample, errors="raise", format="ISO8601")
    except (ValueError, TypeError) as exc:
        log(
            logger,
            "error",
            "listen_time values are not parseable as ISO-8601 timestamps",
            sample_size=len(sample),
            error=repr(exc),
        )
        sys.exit(1)

    log(
        logger,
        "info",
        "Validation passed",
        row_count=row_count,
        columns=list(df.columns),
    )


def main() -> None:
    """Glue entrypoint — parse args, propagate execution_id, run :func:`validate`."""
    # Imported lazily so unit tests don't need the awsglue runtime.
    from awsglue.utils import getResolvedOptions

    args = getResolvedOptions(sys.argv, ["s3_key", "bucket", "execution_id"])
    os.environ["EXECUTION_ID"] = args["execution_id"]
    validate(bucket=args["bucket"], s3_key=args["s3_key"])


if __name__ == "__main__":
    main()
