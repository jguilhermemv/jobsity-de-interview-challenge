from __future__ import annotations

import io
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from de_challenge.api.main import app

CSV_CONTENT = (
    "region,origin_coord,destination_coord,datetime,datasource\n"
    "Prague,POINT(14.4973 50.0755),POINT(14.4800 50.0900),2018-05-28 09:03:40,funny_car\n"
    "Turin,POINT(7.6869 45.0703),POINT(7.6950 45.0800),2018-05-28 09:04:00,bad_diesel_vehicles\n"
)


def _make_csv_file(content: str = CSV_CONTENT) -> tuple[bytes, str]:
    return content.encode("utf-8"), "text/csv"


def test_ingest_returns_202() -> None:
    with patch("de_challenge.api.main.KafkaProducerWrapper") as mock_cls:
        mock_producer = MagicMock()
        mock_cls.return_value = mock_producer

        with TestClient(app) as client:
            data, ctype = _make_csv_file()
            response = client.post(
                "/ingestions",
                files={"file": ("trips.csv", io.BytesIO(data), ctype)},
            )

    assert response.status_code == 202


def test_ingest_response_contains_ingestion_id_and_rows() -> None:
    with patch("de_challenge.api.main.KafkaProducerWrapper") as mock_cls:
        mock_producer = MagicMock()
        mock_cls.return_value = mock_producer

        with TestClient(app) as client:
            data, ctype = _make_csv_file()
            response = client.post(
                "/ingestions",
                files={"file": ("trips.csv", io.BytesIO(data), ctype)},
            )

    body = response.json()
    assert "ingestion_id" in body
    assert isinstance(body["ingestion_id"], str)
    assert len(body["ingestion_id"]) == 36  # UUID format
    assert body["rows"] == 2


def test_ingest_background_task_registered() -> None:
    """Background task must be registered (not executed synchronously)."""
    with patch("de_challenge.api.main.KafkaProducerWrapper") as mock_cls:
        mock_producer = MagicMock()
        mock_cls.return_value = mock_producer

        with patch("de_challenge.api.main._publish_events") as mock_publish:
            with TestClient(app) as client:
                data, ctype = _make_csv_file()
                client.post(
                    "/ingestions",
                    files={"file": ("trips.csv", io.BytesIO(data), ctype)},
                )

    # TestClient runs background tasks synchronously after the response;
    # assert the publish function was called exactly once.
    mock_publish.assert_called_once()
    call_args = mock_publish.call_args
    _, ingestion_id, events = call_args.args
    assert isinstance(ingestion_id, str)
    assert len(events) == 2


def test_ingest_rejects_non_csv() -> None:
    with TestClient(app) as client:
        response = client.post(
            "/ingestions",
            files={"file": ("data.json", io.BytesIO(b"{}"), "application/json")},
        )
    assert response.status_code == 400
