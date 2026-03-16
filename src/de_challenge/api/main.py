from __future__ import annotations

import csv
import io
import logging
import os
import time
import uuid
from typing import Any, Generator

from fastapi import BackgroundTasks, FastAPI, File, HTTPException, Query, UploadFile
from fastapi.responses import StreamingResponse
import json
from pydantic import BaseModel, Field

from kafka import KafkaConsumer
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.logging import LoggingInstrumentor

from de_challenge.api.sse import format_sse_event
from de_challenge.db.postgres import weekly_average_by_bbox, weekly_average_by_region
from de_challenge.ingestion.csv_parser import parse_trip_row
from de_challenge.ingestion.producer import KafkaProducerWrapper
from de_challenge.ingestion.status import STATUS_TOPIC, publish_status
from de_challenge.observability.logging import configure_logging

logger = logging.getLogger(__name__)

TRIPS_TOPIC = os.getenv("TRIPS_TOPIC", "trips.raw")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
PUBLISH_DELAY_SECONDS = float(os.getenv("PUBLISH_DELAY_SECONDS", "0.5"))

# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class HealthResponse(BaseModel):
    status: str = Field(..., examples=["ok"])


class IngestionResponse(BaseModel):
    ingestion_id: str = Field(
        ...,
        description="UUID that identifies this ingestion batch.",
        examples=["3fa85f64-5717-4562-b3fc-2c963f66afa6"],
    )
    rows: int = Field(
        ...,
        description="Number of valid rows accepted and queued to Kafka.",
        examples=[101],
    )


class BoundingBoxFilter(BaseModel):
    min_lon: float = Field(..., examples=[14.0])
    min_lat: float = Field(..., examples=[49.9])
    max_lon: float = Field(..., examples=[14.7])
    max_lat: float = Field(..., examples=[50.2])


class WeeklyAverageFilter(BaseModel):
    region: str | None = Field(None, examples=["Prague"])
    bounding_box: BoundingBoxFilter | None = None


class WeeklyAverageResponse(BaseModel):
    weekly_average: float = Field(
        ...,
        description="Average number of trips per ISO week for the filtered area.",
        examples=[12.5],
    )
    num_weeks: int = Field(
        ...,
        description="Number of distinct ISO weeks that contained at least one trip.",
        examples=[4],
    )
    total_trips: int = Field(
        ...,
        description="Total trips matching the filter.",
        examples=[50],
    )
    filter: WeeklyAverageFilter = Field(
        ...,
        description="Echo of the filter actually applied to the query.",
    )


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Jobsity Data Engineering Challenge",
    version="1.0.0",
    description=(
        "Local, containerized data pipeline: CSV upload → Kafka → Spark Structured Streaming "
        "→ Delta Lake (Bronze / Silver / Gold) → PostgreSQL/PostGIS.\n\n"
        "## Endpoints\n\n"
        "- **POST /ingestions** — Upload a CSV file; each row becomes one Kafka event.\n"
        "- **GET /ingestions/{id}/events** — Stream ingestion status via SSE (no polling).\n"
        "- **GET /trips/weekly-average** — Query the weekly average trip count for an area "
        "(by region name or bounding box coordinates).\n"
    ),
    contact={"name": "Jobsity DE Challenge"},
    license_info={"name": "MIT"},
)
FastAPIInstrumentor.instrument_app(app)


@app.on_event("startup")
def on_startup() -> None:
    configure_logging()
    LoggingInstrumentor().instrument(set_logging_format=True)


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


@app.get(
    "/healthz",
    response_model=HealthResponse,
    tags=["ops"],
    summary="Health check",
)
def healthz() -> HealthResponse:
    """Returns `{"status": "ok"}` when the API is running."""
    return HealthResponse(status="ok")


# ---------------------------------------------------------------------------
# Query — weekly average
# ---------------------------------------------------------------------------


@app.get(
    "/trips/weekly-average",
    response_model=WeeklyAverageResponse,
    response_model_exclude_none=True,
    tags=["trips"],
    summary="Weekly average trip count for an area",
    responses={
        400: {"description": "No filter provided, or bounding-box params are incomplete."},
        503: {"description": "PostgreSQL is unreachable."},
    },
)
def weekly_average(
    region: str | None = Query(
        None,
        description="Filter by region name (e.g. `Prague`, `Turin`).",
        examples={"Prague": {"value": "Prague"}, "Turin": {"value": "Turin"}},
    ),
    min_lon: float | None = Query(None, description="Bounding box — minimum longitude.", examples={14.0: {"value": 14.0}}),
    min_lat: float | None = Query(None, description="Bounding box — minimum latitude.", examples={49.9: {"value": 49.9}}),
    max_lon: float | None = Query(None, description="Bounding box — maximum longitude.", examples={14.7: {"value": 14.7}}),
    max_lat: float | None = Query(None, description="Bounding box — maximum latitude.", examples={50.2: {"value": 50.2}}),
) -> WeeklyAverageResponse:
    """
    Return the **weekly average number of trips** for a given area.

    Provide **either** `region` (name) **or** all four bounding-box coordinates
    (`min_lon`, `min_lat`, `max_lon`, `max_lat`). When both are supplied, `region`
    takes priority.

    The bounding-box query uses PostGIS `ST_Within(origin_geom, ST_MakeEnvelope(...))`
    with a GIST spatial index — sub-millisecond at scale.
    """
    bbox_params = (min_lon, min_lat, max_lon, max_lat)
    has_bbox = all(p is not None for p in bbox_params)

    if region is None and not has_bbox:
        raise HTTPException(
            status_code=400,
            detail=(
                "Provide 'region' or all four bounding-box coordinates "
                "(min_lon, min_lat, max_lon, max_lat)."
            ),
        )

    try:
        if region is not None:
            result = weekly_average_by_region(region)
            filter_used = WeeklyAverageFilter(region=region)
        else:
            result = weekly_average_by_bbox(
                float(min_lon),  # type: ignore[arg-type]
                float(min_lat),  # type: ignore[arg-type]
                float(max_lon),  # type: ignore[arg-type]
                float(max_lat),  # type: ignore[arg-type]
            )
            filter_used = WeeklyAverageFilter(
                bounding_box=BoundingBoxFilter(
                    min_lon=float(min_lon),  # type: ignore[arg-type]
                    min_lat=float(min_lat),  # type: ignore[arg-type]
                    max_lon=float(max_lon),  # type: ignore[arg-type]
                    max_lat=float(max_lat),  # type: ignore[arg-type]
                )
            )
    except Exception as exc:
        logger.exception("weekly_average query failed", extra={"error": str(exc)})
        raise HTTPException(status_code=503, detail="Database unavailable") from exc

    return WeeklyAverageResponse(
        weekly_average=float(result["weekly_average"]),
        num_weeks=int(result["num_weeks"]),
        total_trips=int(result["total_trips"]),
        filter=filter_used,
    )


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------


def _publish_events(
    producer: KafkaProducerWrapper,
    ingestion_id: str,
    events: list[tuple[int, Any]],
) -> None:
    row_count = 0
    publish_status(producer, ingestion_id, "STARTED")
    for row_number, event in events:
        try:
            # Use ingestion_id as Kafka key so trip rows and end marker preserve order per ingestion
            producer.send(
                TRIPS_TOPIC,
                value=event.model_dump(exclude_none=True),
                key=ingestion_id,
            )
            row_count += 1
            if row_count % 100 == 0:
                publish_status(
                    producer,
                    ingestion_id,
                    "IN_PROGRESS",
                    details={"rows": row_count},
                )
        except Exception as exc:
            logger.warning(
                "Failed to send event",
                extra={
                    "ingestion_id": ingestion_id,
                    "row_number": row_number,
                    "error": str(exc),
                    "trace_id": ingestion_id,
                },
            )
        if PUBLISH_DELAY_SECONDS > 0:
            time.sleep(PUBLISH_DELAY_SECONDS)

    # End-of-ingestion marker: deterministic boundary for downstream consumers
    marker = {
        "record_type": "ingestion_end",
        "ingestion_id": ingestion_id,
        "control_id": f"{ingestion_id}:end",
        "total_rows": row_count,
    }
    producer.send(TRIPS_TOPIC, value=marker, key=ingestion_id)
    producer.flush()
    publish_status(producer, ingestion_id, "COMPLETED", details={"rows": row_count})


@app.post(
    "/ingestions",
    status_code=202,
    response_model=IngestionResponse,
    tags=["ingestion"],
    summary="Upload a CSV file and start ingestion",
    responses={
        202: {"description": "File accepted; events are being published to Kafka."},
        400: {"description": "Uploaded file is not a CSV."},
    },
)
async def ingest_csv(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(..., description="CSV file with trip data."),
) -> IngestionResponse:
    """
    Upload a CSV file containing trip records.

    Each valid row is published as a **Kafka event** to the `trips.raw` topic.
    Parsing and publishing happen asynchronously — the response is returned
    immediately with an `ingestion_id` you can use to track progress via SSE.

    Invalid rows (out-of-range coordinates, missing fields) are skipped and
    logged; they do **not** abort the ingestion.
    """
    if file.content_type not in {"text/csv", "application/vnd.ms-excel"}:
        raise HTTPException(status_code=400, detail="File must be CSV")

    ingestion_id = str(uuid.uuid4())
    raw = await file.read()
    reader = csv.DictReader(io.StringIO(raw.decode("utf-8")))

    parsed_events: list[tuple[int, Any]] = []
    for row_number, row in enumerate(reader, start=1):
        try:
            event = parse_trip_row(row, ingestion_id=ingestion_id, row_number=row_number)
            parsed_events.append((row_number, event))
        except ValueError as exc:
            logger.warning(
                "Invalid row",
                extra={
                    "ingestion_id": ingestion_id,
                    "row_number": row_number,
                    "error": str(exc),
                    "trace_id": ingestion_id,
                },
            )

    producer = KafkaProducerWrapper(KAFKA_BOOTSTRAP)
    background_tasks.add_task(_publish_events, producer, ingestion_id, parsed_events)

    return IngestionResponse(ingestion_id=ingestion_id, rows=len(parsed_events))


# ---------------------------------------------------------------------------
# SSE status stream
# ---------------------------------------------------------------------------


def _status_stream(ingestion_id: str) -> Generator[str, None, None]:
    consumer = KafkaConsumer(
        STATUS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=f"ingestion-status-{ingestion_id}",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    for message in consumer:
        try:
            payload = message.value
            if payload.get("ingestion_id") != ingestion_id:
                continue
            yield format_sse_event("status", payload)
        except Exception as exc:  # pragma: no cover - defensive
            logger.exception("Failed to stream status", extra={"error": str(exc)})
            yield format_sse_event("error", {"message": str(exc)})


@app.get(
    "/ingestions/{ingestion_id}/events",
    tags=["ingestion"],
    summary="Stream ingestion status via Server-Sent Events",
    response_class=StreamingResponse,
    responses={
        200: {
            "content": {"text/event-stream": {}},
            "description": (
                "SSE stream of status events. Each event has the shape "
                "`{ingestion_id, status, details?}`. "
                "Status sequence: `STARTED` → `IN_PROGRESS` → `COMPLETED` "
                "(API finished publishing to Kafka) → `BRONZE_COMPLETED` → "
                "`SILVER_COMPLETED` → `TRIPS_PG_COMPLETED`. On failure: `FAILED`."
            ),
        }
    },
)
def stream_ingestion_events(
    ingestion_id: str = ...,  # type: ignore[assignment]
) -> StreamingResponse:
    """
    Open a **Server-Sent Events** stream for the given `ingestion_id`.

    The stream emits one event per status change — no polling required.
    Connect with `curl -N http://localhost:8000/ingestions/<id>/events` or
    any `EventSource`-compatible client.

    Status sequence (Phase 1): STARTED → IN_PROGRESS → COMPLETED (API done) →
    BRONZE_COMPLETED → SILVER_COMPLETED → TRIPS_PG_COMPLETED. Each stage
    emits only after the corresponding sink has committed successfully.

    The stream stays open until the Kafka consumer is closed by the client.
    """
    return StreamingResponse(_status_stream(ingestion_id), media_type="text/event-stream")
