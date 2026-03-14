from fastapi.testclient import TestClient

from de_challenge.api.main import app


def test_healthz_startup() -> None:
    with TestClient(app) as client:
        response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
