from de_challenge.contract.events import TripEvent


def test_trip_event_schema_contains_required_fields():
    event = TripEvent(
        ingestion_id="ing-1",
        row_number=1,
        trip_id="trip-1",
        region="Prague",
        origin_lon=14.4973794,
        origin_lat=50.0013687,
        destination_lon=14.4310948,
        destination_lat=50.0405293,
        datetime="2018-05-28T09:03:40Z",
        datasource="funny_car",
    )

    payload = event.model_dump()
    assert set(payload.keys()) == {
        "ingestion_id",
        "row_number",
        "trip_id",
        "region",
        "origin_lon",
        "origin_lat",
        "destination_lon",
        "destination_lat",
        "datetime",
        "datasource",
    }
