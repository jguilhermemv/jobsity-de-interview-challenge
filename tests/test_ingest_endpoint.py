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


def test_ingest_publishes_with_ingestion_id_as_kafka_key() -> None:
    """Trip events must use ingestion_id as Kafka key for ordering."""
    with patch("de_challenge.api.main.KafkaProducerWrapper") as mock_cls:
        mock_producer = MagicMock()
        mock_cls.return_value = mock_producer

        with patch("de_challenge.api.main.publish_status"):
            with TestClient(app) as client:
                data, ctype = _make_csv_file()
                resp = client.post(
                    "/ingestions",
                    files={"file": ("trips.csv", io.BytesIO(data), ctype)},
                )
                ingestion_id = resp.json()["ingestion_id"]

    # 2 trip events + 1 marker = 3 sends, all with key=ingestion_id
    assert mock_producer.send.call_count >= 3
    for call in mock_producer.send.call_args_list:
        args, kwargs = call
        assert kwargs.get("key") == ingestion_id


def test_ingest_publishes_exactly_one_end_marker_after_trip_events() -> None:
    """API must publish one ingestion_end marker after all trip events."""
    with patch("de_challenge.api.main.KafkaProducerWrapper") as mock_cls:
        mock_producer = MagicMock()
        mock_cls.return_value = mock_producer

        with patch("de_challenge.api.main.publish_status"):
            with TestClient(app) as client:
                data, ctype = _make_csv_file()
                resp = client.post(
                    "/ingestions",
                    files={"file": ("trips.csv", io.BytesIO(data), ctype)},
                )
                ingestion_id = resp.json()["ingestion_id"]

    # Last send must be the marker
    last_call = mock_producer.send.call_args_list[-1]
    value = last_call.kwargs.get("value") or last_call[0][1]
    assert value.get("record_type") == "ingestion_end"
    assert value.get("ingestion_id") == ingestion_id
    assert value.get("control_id") == f"{ingestion_id}:end"
    assert value.get("total_rows") == 2


def test_ingest_empty_csv_still_publishes_marker() -> None:
    """Empty ingestion (0 rows) still publishes one marker with total_rows=0."""
    csv_empty = "region,origin_coord,destination_coord,datetime,datasource\n"
    with patch("de_challenge.api.main.KafkaProducerWrapper") as mock_cls:
        mock_producer = MagicMock()
        mock_cls.return_value = mock_producer

        with patch("de_challenge.api.main.publish_status"):
            with TestClient(app) as client:
                resp = client.post(
                    "/ingestions",
                    files={"file": ("empty.csv", io.BytesIO(csv_empty.encode()), "text/csv")},
                )
                ingestion_id = resp.json()["ingestion_id"]

    assert resp.json()["rows"] == 0
    # Only the marker is sent
    assert mock_producer.send.call_count == 1
    value = mock_producer.send.call_args_list[0].kwargs.get("value")
    assert value.get("record_type") == "ingestion_end"
    assert value.get("total_rows") == 0


def test_ingest_rejects_non_csv() -> None:
    with TestClient(app) as client:
        response = client.post(
            "/ingestions",
            files={"file": ("data.json", io.BytesIO(b"{}"), "application/json")},
        )
    assert response.status_code == 400
