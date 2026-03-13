from __future__ import annotations

from typing import Mapping, Tuple

from dateutil import parser, tz

from de_challenge.contract.events import TripEvent
from de_challenge.domain.identifiers import compute_trip_id
from de_challenge.domain.validation import validate_coordinates


def _parse_point(value: str) -> Tuple[float, float]:
    raw = value.strip()
    if not raw.upper().startswith("POINT"):
        raise ValueError(f"Invalid point value: {value}")
    coords = raw[raw.find("(") + 1 : raw.find(")")].strip()
    parts = coords.split()
    if len(parts) != 2:
        raise ValueError(f"Invalid point value: {value}")
    lon, lat = float(parts[0]), float(parts[1])
    validate_coordinates(lon, lat)
    return lon, lat


def _normalize_datetime(value: str) -> str:
    parsed = parser.isoparse(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=tz.UTC)
    return parsed.astimezone(tz.UTC).isoformat().replace("+00:00", "Z")


def parse_trip_row(row: Mapping[str, str], ingestion_id: str, row_number: int) -> TripEvent:
    origin_lon, origin_lat = _parse_point(row["origin_coord"])
    destination_lon, destination_lat = _parse_point(row["destination_coord"])
    normalized_datetime = _normalize_datetime(row["datetime"])

    payload = {
        "region": row["region"],
        "origin_lon": origin_lon,
        "origin_lat": origin_lat,
        "destination_lon": destination_lon,
        "destination_lat": destination_lat,
        "datetime": normalized_datetime,
        "datasource": row["datasource"],
    }

    trip_id = compute_trip_id(payload)

    return TripEvent(
        ingestion_id=ingestion_id,
        row_number=row_number,
        trip_id=trip_id,
        region=payload["region"],
        origin_lon=origin_lon,
        origin_lat=origin_lat,
        destination_lon=destination_lon,
        destination_lat=destination_lat,
        datetime=normalized_datetime,
        datasource=payload["datasource"],
    )
