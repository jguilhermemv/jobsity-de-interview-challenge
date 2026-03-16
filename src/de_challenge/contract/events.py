from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class TripEvent(BaseModel):
    """Business event for a single trip row. Use record_type='trip' or omit for compatibility."""

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
    record_type: Literal["trip"] | None = None  # Optional; omit or "trip" = business row


class IngestionEndEvent(BaseModel):
    """Control event marking end of an ingestion batch. Not a business trip row."""

    record_type: Literal["ingestion_end"]
    ingestion_id: str
    control_id: str
    total_rows: int = Field(ge=0)
