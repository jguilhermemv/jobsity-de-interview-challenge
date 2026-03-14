from __future__ import annotations

import json
from typing import Any, Mapping

from kafka import KafkaProducer


class KafkaProducerWrapper:
    def __init__(self, bootstrap_servers: str) -> None:
        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda v: v.encode("utf-8") if v else None,
        )

    def send(self, topic: str, value: Mapping[str, Any], key: str | None = None) -> None:
        self._producer.send(topic, value=value, key=key)

    def flush(self) -> None:
        self._producer.flush()
