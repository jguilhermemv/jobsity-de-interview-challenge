from __future__ import annotations

from pydantic import BaseModel, Field


class TripEvent(BaseModel):
    ingestion_id: str
    row_number: int = Field(ge=1)
    trip_id: str
    region: str
    origin_lon: float
    origin_lat: float
    destination_lon: float
    destination_lat: float
    datetime: str
    datasource: str
