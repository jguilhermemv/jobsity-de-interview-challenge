from __future__ import annotations

import csv
import io
import logging
import os
import uuid
from typing import Generator

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
import json

from kafka import KafkaConsumer
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.logging import LoggingInstrumentor

from de_challenge.api.sse import format_sse_event
from de_challenge.ingestion.csv_parser import parse_trip_row
from de_challenge.ingestion.producer import KafkaProducerWrapper
from de_challenge.ingestion.status import STATUS_TOPIC, publish_status
from de_challenge.observability.logging import configure_logging

logger = logging.getLogger(__name__)

TRIPS_TOPIC = os.getenv("TRIPS_TOPIC", "trips.raw")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

app = FastAPI(title="Jobsity DE Challenge")
FastAPIInstrumentor.instrument_app(app)


@app.on_event("startup")
def on_startup() -> None:
    configure_logging()
    LoggingInstrumentor().instrument(set_logging_format=True)


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/ingestions")
async def ingest_csv(file: UploadFile = File(...)) -> dict[str, str]:
    if file.content_type not in {"text/csv", "application/vnd.ms-excel"}:
        raise HTTPException(status_code=400, detail="File must be CSV")

    ingestion_id = str(uuid.uuid4())
    producer = KafkaProducerWrapper(KAFKA_BOOTSTRAP)
    publish_status(producer, ingestion_id, "STARTED")

    raw = await file.read()
    reader = csv.DictReader(io.StringIO(raw.decode("utf-8")))

    row_count = 0
    for row_number, row in enumerate(reader, start=1):
        try:
            event = parse_trip_row(row, ingestion_id=ingestion_id, row_number=row_number)
            producer.send(TRIPS_TOPIC, value=event.model_dump(), key=event.trip_id)
            row_count += 1
            if row_count % 100 == 0:
                publish_status(
                    producer,
                    ingestion_id,
                    "IN_PROGRESS",
                    details={"rows": row_count},
                )
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
            producer.send(
                "trips.rejected",
                value={"ingestion_id": ingestion_id, "row_number": row_number, "error": str(exc)},
                key=ingestion_id,
            )

    publish_status(producer, ingestion_id, "COMPLETED", details={"rows": row_count})
    return {"ingestion_id": ingestion_id, "rows": str(row_count)}


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


@app.get("/ingestions/{ingestion_id}/events")
def stream_ingestion_events(ingestion_id: str) -> StreamingResponse:
    return StreamingResponse(_status_stream(ingestion_id), media_type="text/event-stream")
