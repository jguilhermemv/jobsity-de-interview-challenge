from de_challenge.contract.events import TripEvent
from de_challenge.domain.dedup import deduplicate_by_trip_id


def test_deduplicate_by_trip_id():
    event_a = TripEvent(
        ingestion_id="ing-1",
        row_number=1,
        trip_id="trip-1",
        region="Prague",
        origin_lon=14.4,
        origin_lat=50.0,
        destination_lon=14.5,
        destination_lat=50.1,
        datetime="2018-05-28T09:03:40Z",
        datasource="funny_car",
    )
    event_b = TripEvent(
        ingestion_id="ing-2",
        row_number=2,
        trip_id="trip-1",
        region="Prague",
        origin_lon=14.4,
        origin_lat=50.0,
        destination_lon=14.5,
        destination_lat=50.1,
        datetime="2018-05-28T09:03:40Z",
        datasource="funny_car",
    )

    unique = deduplicate_by_trip_id([event_a, event_b])

    assert len(unique) == 1
    assert unique[0].trip_id == "trip-1"
