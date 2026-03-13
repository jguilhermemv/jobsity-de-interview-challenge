import pytest

from de_challenge.ingestion.csv_parser import parse_trip_row


def test_parse_trip_row_builds_event():
    row = {
        "region": "Prague",
        "origin_coord": "POINT (14.4973794438195 50.00136875782316)",
        "destination_coord": "POINT (14.43109483523328 50.04052930943246)",
        "datetime": "2018-05-28 09:03:40",
        "datasource": "funny_car",
    }

    event = parse_trip_row(row, ingestion_id="ing-1", row_number=1)

    assert event.region == "Prague"
    assert event.origin_lon == pytest.approx(14.4973794438195)
    assert event.origin_lat == pytest.approx(50.00136875782316)
    assert event.destination_lon == pytest.approx(14.43109483523328)
    assert event.destination_lat == pytest.approx(50.04052930943246)
    assert event.datetime.endswith("Z")
    assert event.row_number == 1
    assert event.ingestion_id == "ing-1"


def test_parse_trip_row_rejects_invalid_coordinates():
    row = {
        "region": "Prague",
        "origin_coord": "POINT (200 95)",
        "destination_coord": "POINT (14.43109483523328 50.04052930943246)",
        "datetime": "2018-05-28 09:03:40",
        "datasource": "funny_car",
    }

    with pytest.raises(ValueError):
        parse_trip_row(row, ingestion_id="ing-1", row_number=1)
