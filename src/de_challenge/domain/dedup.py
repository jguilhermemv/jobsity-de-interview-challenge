from __future__ import annotations

from typing import Iterable, List

from de_challenge.contract.events import TripEvent


def deduplicate_by_trip_id(events: Iterable[TripEvent]) -> List[TripEvent]:
    seen: set[str] = set()
    unique: list[TripEvent] = []
    for event in events:
        if event.trip_id in seen:
            continue
        seen.add(event.trip_id)
        unique.append(event)
    return unique
