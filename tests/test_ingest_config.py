"""Tests for cade.ingest.config."""
import pytest
from pathlib import Path
from cade.ingest.config import load
from cade.ingest.exceptions import ConfigError


def test_load_missing_all_env_vars(monkeypatch, tmp_path):
    for var in ("STAGING_DB_URL", "S3_BUCKET", "CADE_DATA_DIR", "CADE_API_URL"):
        monkeypatch.delenv(var, raising=False)
    with pytest.raises(ConfigError) as exc_info:
        load()
    err = str(exc_info.value)
    assert "STAGING_DB_URL" in err
    assert "S3_BUCKET" in err
    assert "CADE_DATA_DIR" in err
    assert "CADE_API_URL" in err


def test_load_inaccessible_data_dir(monkeypatch):
    monkeypatch.setenv("STAGING_DB_URL", "postgresql://localhost/test")
    monkeypatch.setenv("S3_BUCKET", "my-bucket")
    monkeypatch.setenv("CADE_DATA_DIR", "/nonexistent/path/xyz")
    monkeypatch.setenv("CADE_API_URL", "http://localhost:8000")
    with pytest.raises(ConfigError) as exc_info:
        load()
    assert "not accessible" in str(exc_info.value)


def test_load_success(monkeypatch, tmp_path):
    monkeypatch.setenv("STAGING_DB_URL", "postgresql://localhost/test")
    monkeypatch.setenv("S3_BUCKET", "my-bucket")
    monkeypatch.setenv("CADE_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("CADE_API_URL", "http://localhost:8000")
    conf = load()
    assert conf.s3_bucket == "my-bucket"
    assert conf.data_dir == tmp_path
    assert conf.api_url == "http://localhost:8000"


def test_load_strips_trailing_slash_from_api_url(monkeypatch, tmp_path):
    monkeypatch.setenv("STAGING_DB_URL", "postgresql://localhost/test")
    monkeypatch.setenv("S3_BUCKET", "my-bucket")
    monkeypatch.setenv("CADE_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("CADE_API_URL", "http://localhost:8000/")
    conf = load()
    assert conf.api_url == "http://localhost:8000"
