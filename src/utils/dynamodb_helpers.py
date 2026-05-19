"""DynamoDB-write helpers: Decimal sanitisation + idempotent batched put_item.

Why ``overwrite_by_pkeys`` is non-negotiable here
-------------------------------------------------
``Table.batch_writer()`` refuses to accept two items with the same primary key
inside one 25-item batch unless you pass ``overwrite_by_pkeys`` — and the KPI
loader regularly re-emits the same ``(pk, sk)`` across a single batch when the
ranked frames are concatenated. Without this flag the loader raises
``ValidationException: Provided list of item keys contains duplicates`` and
breaks idempotency.

Why we always pre-convert floats to Decimal
-------------------------------------------
DynamoDB rejects native ``float`` values — they MUST be ``decimal.Decimal``.
Using ``Decimal(str(value))`` (not ``Decimal(value)``) avoids the binary-float
precision drift that would otherwise corrupt the stored numbers.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

_DEFAULT_PKEYS: list[str] = ["pk", "sk"]


def sanitize_for_dynamodb(item: dict[str, Any]) -> dict[str, Any]:
    """Recursively replace ``float`` values with ``Decimal`` for DynamoDB compatibility."""
    return {key: _convert_value(value) for key, value in item.items()}


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
