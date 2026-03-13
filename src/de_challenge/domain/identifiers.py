from __future__ import annotations

import hashlib
from typing import Any, Mapping

from dateutil import parser, tz


_HASH_FIELDS = (
    "region",
    "origin_lon",
    "origin_lat",
    "destination_lon",
    "destination_lat",
    "datetime",
    "datasource",
)


def _normalize_datetime(value: str) -> str:
    parsed = parser.isoparse(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=tz.UTC)
    return parsed.astimezone(tz.UTC).isoformat().replace("+00:00", "Z")


def _normalize_float(value: Any) -> str:
    return f"{float(value):.6f}"


def compute_trip_id(payload: Mapping[str, Any]) -> str:
    normalized: list[str] = []
    for field in _HASH_FIELDS:
        if field not in payload:
            raise ValueError(f"Missing field for trip_id: {field}")
        value = payload[field]
        if field in ("origin_lon", "origin_lat", "destination_lon", "destination_lat"):
            normalized.append(_normalize_float(value))
        elif field == "datetime":
            normalized.append(_normalize_datetime(str(value)))
        else:
            normalized.append(str(value).strip().lower())

    raw = "|".join(normalized).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()
