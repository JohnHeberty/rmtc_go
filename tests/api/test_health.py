from fastapi.testclient import TestClient

from rmtc_go.api.main import app


def test_health():
    response = TestClient(app).get("/health")
    assert response.status_code == 200
    assert response.json() == {"message": "System Online!"}
