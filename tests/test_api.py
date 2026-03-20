"""API integration tests."""
import pytest
from datetime import date
from fastapi.testclient import TestClient
import os


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("CADE_DATA_DIR", str(tmp_path))
    # Re-import to pick up env var
    import importlib
    import cade.api as api_module
    importlib.reload(api_module)
    return TestClient(api_module.app)


def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_ingest_and_get(client, sample_snapshot):
    payload = {
        "snapshot": sample_snapshot.model_dump(mode="json"),
        "exposure_total": 1_000_000.0,
    }
    r = client.post("/ingest", json=payload)
    assert r.status_code == 201
    data = r.json()
    assert data["data_hash"].startswith("sha256-v1:")

    r2 = client.get(
        f"/counterparties/{sample_snapshot.counterparty_id}"
        f"/agreements/{sample_snapshot.netting_set_id}"
        f"/{sample_snapshot.cob_date}"
    )
    assert r2.status_code == 200
    assert r2.json()["counterparty_id"] == sample_snapshot.counterparty_id


def test_get_missing_returns_404(client):
    r = client.get("/counterparties/NOBODY/agreements/RATES/2024-01-01")
    assert r.status_code == 404


def test_path_traversal_rejected(client):
    r = client.get("/counterparties/../etc/agreements/RATES/2024-01-01")
    # FastAPI path handling rejects this at routing level or Pydantic validation
    assert r.status_code in (400, 404, 422)


def test_duplicate_ingest_returns_409(client, sample_snapshot):
    payload = {"snapshot": sample_snapshot.model_dump(mode="json"), "exposure_total": 1e6}
    client.post("/ingest", json=payload)
    r = client.post("/ingest", json=payload)
    assert r.status_code == 409


def test_portfolio(client, sample_snapshot):
    payload = {"snapshot": sample_snapshot.model_dump(mode="json"), "exposure_total": 1_000_000.0}
    client.post("/ingest", json=payload)
    r = client.get(f"/portfolio/exposure?cob_date={sample_snapshot.cob_date}")
    assert r.status_code == 200
    data = r.json()
    assert len(data) == 1
    assert data[0]["exposure_total"] == 1_000_000.0


def test_diff_invalid_range(client, sample_snapshot):
    payload = {"snapshot": sample_snapshot.model_dump(mode="json"), "exposure_total": 1e6}
    client.post("/ingest", json=payload)
    r = client.get(
        f"/counterparties/{sample_snapshot.counterparty_id}"
        f"/agreements/{sample_snapshot.netting_set_id}/diff"
        f"?from_date=2024-03-16&to_date=2024-03-15"
    )
    assert r.status_code == 400
