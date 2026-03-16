"""
Shared helper for Spark-side ingestion status emission.

Publishes status events to ingestion.status topic so the API SSE consumer
can stream pipeline progress (BRONZE_COMPLETED, SILVER_COMPLETED, etc.)
without polling.
"""

from __future__ import annotations

import json
import os
from typing import Any

_STATUS_TOPIC = os.getenv("STATUS_TOPIC", "ingestion.status")
_KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

_producer: Any = None


def _get_producer() -> Any:
    """Lazy-create Kafka producer in driver (safe for Spark foreachBatch)."""
    global _producer
    if _producer is None:
        from kafka import KafkaProducer

        _producer = KafkaProducer(
            bootstrap_servers=_KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda v: v.encode("utf-8") if v else None,
        )
    return _producer


def publish_ingestion_status(
    ingestion_id: str,
    status: str,
    details: dict[str, Any] | None = None,
) -> None:
    """
    Publish an ingestion status event to ingestion.status topic.

    Compatible with API SSE consumer. Call from Spark foreachBatch after
    sink commit succeeds.
    """
    payload: dict[str, Any] = {"ingestion_id": ingestion_id, "status": status}
    if details:
        payload.update(details)
    producer = _get_producer()
    producer.send(_STATUS_TOPIC, value=payload, key=ingestion_id)
    producer.flush()
