from __future__ import annotations

from unittest.mock import patch

from fastapi.testclient import TestClient

from de_challenge.api.main import app

_MOCK_RESULT = {"weekly_average": 12.5, "num_weeks": 4, "total_trips": 50}

client = TestClient(app)


def test_weekly_average_by_region() -> None:
    with patch("de_challenge.api.main.weekly_average_by_region", return_value=_MOCK_RESULT):
        response = client.get("/trips/weekly-average", params={"region": "Prague"})

    assert response.status_code == 200
    body = response.json()
    assert body["weekly_average"] == 12.5
    assert body["num_weeks"] == 4
    assert body["total_trips"] == 50
    assert body["filter"] == {"region": "Prague"}


def test_weekly_average_by_bbox() -> None:
    params = {
        "min_lon": 14.0,
        "min_lat": 49.9,
        "max_lon": 14.6,
        "max_lat": 50.2,
    }
    with patch("de_challenge.api.main.weekly_average_by_bbox", return_value=_MOCK_RESULT):
        response = client.get("/trips/weekly-average", params=params)

    assert response.status_code == 200
    body = response.json()
    assert body["weekly_average"] == 12.5
    assert body["filter"]["bounding_box"]["min_lon"] == 14.0


def test_weekly_average_no_filter_returns_400() -> None:
    response = client.get("/trips/weekly-average")
    assert response.status_code == 400
    assert "region" in response.json()["detail"]


def test_weekly_average_partial_bbox_returns_400() -> None:
    """Only two of four bbox params provided — must return 400."""
    response = client.get(
        "/trips/weekly-average", params={"min_lon": 14.0, "min_lat": 49.9}
    )
    assert response.status_code == 400


def test_weekly_average_db_error_returns_503() -> None:
    with patch(
        "de_challenge.api.main.weekly_average_by_region",
        side_effect=Exception("connection refused"),
    ):
        response = client.get("/trips/weekly-average", params={"region": "Prague"})

    assert response.status_code == 503
    assert "unavailable" in response.json()["detail"].lower()


def test_weekly_average_region_takes_priority_over_bbox() -> None:
    """When both region and bbox are provided, region filter is used."""
    params = {
        "region": "Prague",
        "min_lon": 14.0,
        "min_lat": 49.9,
        "max_lon": 14.6,
        "max_lat": 50.2,
    }
    with patch(
        "de_challenge.api.main.weekly_average_by_region", return_value=_MOCK_RESULT
    ) as mock_region:
        response = client.get("/trips/weekly-average", params=params)

    assert response.status_code == 200
    mock_region.assert_called_once_with("Prague")
