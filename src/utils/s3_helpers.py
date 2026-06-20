"""Thin wrappers around boto3 S3 calls so tests can inject a moto-mocked client.

Each function accepts the client as the first positional argument — never
constructs its own — which makes the helpers trivially testable under
``@mock_aws`` without monkey-patching the boto3 module.
"""

from __future__ import annotations

from typing import Any

from botocore.exceptions import ClientError


def get_s3_client() -> Any:
    """Return a default-configured boto3 S3 client."""
    import boto3  # local import — keeps module importable in environments without boto3

    return boto3.client("s3")


def read_csv_bytes(client: Any, bucket: str, key: str) -> bytes:
    """Download ``s3://{bucket}/{key}`` and return the raw bytes."""
    response = client.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read()
    if not isinstance(body, bytes):
        # botocore returns bytes; cast defensively for type-checkers
        body = bytes(body)
    return body


def copy_s3_object(client: Any, bucket: str, source_key: str, dest_key: str) -> None:
    """Copy ``source_key`` to ``dest_key`` within the same ``bucket``."""
    client.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": source_key},
        Key=dest_key,
    )


def delete_s3_object(client: Any, bucket: str, key: str) -> None:
    """Delete ``s3://{bucket}/{key}`` (no-op when the key is already absent)."""
    client.delete_object(Bucket=bucket, Key=key)


def object_exists(client: Any, bucket: str, key: str) -> bool:
    """Return True when ``s3://{bucket}/{key}`` exists."""
    try:
        client.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise
    return True


def derive_archive_key(s3_key: str, *, raw_prefix: str = "raw/") -> str:
    """Map a ``raw/``-prefixed key to its ``archive/`` counterpart."""
    if not s3_key.startswith(raw_prefix):
        msg = f"Expected key under {raw_prefix!r}, got {s3_key!r}"
        raise ValueError(msg)
    return "archive/" + s3_key[len(raw_prefix) :]
