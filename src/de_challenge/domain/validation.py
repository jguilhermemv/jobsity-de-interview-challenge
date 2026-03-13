from __future__ import annotations


def validate_coordinates(lon: float, lat: float) -> None:
    if not -180.0 <= lon <= 180.0:
        raise ValueError(f"Longitude out of range: {lon}")
    if not -90.0 <= lat <= 90.0:
        raise ValueError(f"Latitude out of range: {lat}")
