"""Glue Python Shell — move processed raw CSVs into the archive/ prefix.

Copies ``s3://<bucket>/raw/streams/<filename>`` to
``s3://<bucket>/archive/streams/<filename>`` and then deletes the raw key.

The job is idempotent: if the source key is already gone (a previous run
succeeded but the rest of the pipeline failed, prompting a re-run), the
delete step swallows ``NoSuchKey`` / ``404`` and logs a warning rather than
failing the execution.
"""

from __future__ import annotations

import os
import sys
from typing import Any

from botocore.exceptions import ClientError

from src.utils.logger import get_logger, log
from src.utils.s3_helpers import copy_s3_object, delete_s3_object, get_s3_client

_JOB_NAME = "archive_files"
_RAW_PREFIX = "raw/"
_ARCHIVE_PREFIX = "archive/"


def derive_archive_key(s3_key: str) -> str:
    """Map a raw/-prefixed key to its archive/-prefixed counterpart."""
    if not s3_key.startswith(_RAW_PREFIX):
        msg = f"Expected key under {_RAW_PREFIX!r}, got {s3_key!r}"
        raise ValueError(msg)
    return _ARCHIVE_PREFIX + s3_key[len(_RAW_PREFIX) :]


def archive(*, bucket: str, s3_key: str, s3_client: Any | None = None) -> None:
    """Copy ``s3_key`` to its archive path then delete the source (idempotent)."""
    logger = get_logger(_JOB_NAME)
    archive_key = derive_archive_key(s3_key)
    client = s3_client if s3_client is not None else get_s3_client()

    log(
        logger,
        "info",
        "Archiving file",
        bucket=bucket,
        source_key=s3_key,
        archive_key=archive_key,
    )

    # Copy first. Idempotent: re-copying overwrites the existing object with
    # identical bytes, which is harmless.
    try:
        copy_s3_object(client, bucket, s3_key, archive_key)
    except ClientError as exc:
        # If the source key is already gone (idempotent re-run), the archive
        # copy already happened on the previous run — treat as success.
        code = exc.response.get("Error", {}).get("Code")
        if code in {"NoSuchKey", "404"}:
            log(
                logger,
                "warning",
                "Source already deleted; archive copy already exists on prior run",
                source_key=s3_key,
            )
            return
        raise

    # Delete the raw key. Idempotent: if it's already gone, log and continue.
    try:
        delete_s3_object(client, bucket, s3_key)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code in {"NoSuchKey", "404"}:
            log(logger, "warning", "Source already deleted", source_key=s3_key)
            return
        raise

    log(logger, "info", "Archive complete", archive_key=archive_key)


def main() -> None:  # pragma: no cover
    """Glue entrypoint — parse args, propagate execution_id, run :func:`archive`."""
    from awsglue.utils import getResolvedOptions

    args = getResolvedOptions(sys.argv, ["s3_key", "bucket", "execution_id"])
    os.environ["EXECUTION_ID"] = args["execution_id"]
    archive(bucket=args["bucket"], s3_key=args["s3_key"])


if __name__ == "__main__":
    main()
