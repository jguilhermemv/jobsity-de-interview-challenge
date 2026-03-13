from de_challenge.domain.identifiers import compute_trip_id


def test_trip_id_deterministic_and_excludes_ingestion_id():
    base = {
        "region": "Prague",
        "origin_lon": 14.4973794,
        "origin_lat": 50.0013687,
        "destination_lon": 14.4310948,
        "destination_lat": 50.0405293,
        "datetime": "2018-05-28T09:03:40Z",
        "datasource": "funny_car",
    }
    trip_id_a = compute_trip_id({**base, "ingestion_id": "ing-1"})
    trip_id_b = compute_trip_id({**base, "ingestion_id": "ing-2"})

    assert trip_id_a == trip_id_b


def test_trip_id_changes_when_payload_changes():
    base = {
        "region": "Prague",
        "origin_lon": 14.4973794,
        "origin_lat": 50.0013687,
        "destination_lon": 14.4310948,
        "destination_lat": 50.0405293,
        "datetime": "2018-05-28T09:03:40Z",
        "datasource": "funny_car",
    }
    trip_id_a = compute_trip_id(base)
    trip_id_b = compute_trip_id({**base, "origin_lat": 50.1013687})

    assert trip_id_a != trip_id_b
