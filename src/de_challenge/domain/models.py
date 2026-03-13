from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Trip:
    trip_id: str
    region: str
    origin_lon: float
    origin_lat: float
    destination_lon: float
    destination_lat: float
    datetime: str
    datasource: str
    ingestion_id: str
    row_number: int


@dataclass(frozen=True)
class Ingestion:
    ingestion_id: str
    status: str


@dataclass(frozen=True)
class Region:
    region_id: int
    region_name: str


@dataclass(frozen=True)
class Datasource:
    datasource_id: int
    datasource_name: str
