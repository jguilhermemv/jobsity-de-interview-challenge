from de_challenge.contract.events import IngestionEndEvent, TripEvent


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

    # exclude_none matches API publish behavior (record_type omitted for trips)
    payload = event.model_dump(exclude_none=True)
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


def test_ingestion_end_event_schema():
    event = IngestionEndEvent(
        record_type="ingestion_end",
        ingestion_id="ing-1",
        control_id="ing-1:end",
        total_rows=42,
    )
    payload = event.model_dump()
    assert payload == {
        "record_type": "ingestion_end",
        "ingestion_id": "ing-1",
        "control_id": "ing-1:end",
        "total_rows": 42,
    }
