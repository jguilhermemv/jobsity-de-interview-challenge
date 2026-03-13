from __future__ import annotations

from typing import Any, Mapping

import os

from de_challenge.ingestion.producer import KafkaProducerWrapper


STATUS_TOPIC = os.getenv("STATUS_TOPIC", "ingestion.status")


def publish_status(
    producer: KafkaProducerWrapper,
    ingestion_id: str,
    status: str,
    details: Mapping[str, Any] | None = None,
) -> None:
    payload: dict[str, Any] = {"ingestion_id": ingestion_id, "status": status}
    if details:
        payload.update(details)
    producer.send(STATUS_TOPIC, value=payload, key=ingestion_id)
