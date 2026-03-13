from __future__ import annotations

import json
from typing import Any, Mapping


def format_sse_event(event_type: str, data: Mapping[str, Any]) -> str:
    payload = json.dumps(data)
    return f"event: {event_type}\n" f"data: {payload}\n\n"
