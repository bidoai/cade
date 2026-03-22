"""Configuration for the cade ingestion pipeline.

All settings are read from environment variables. Call validate() at
startup to fail fast with a clear error if required settings are missing.

Required env vars:
  STAGING_DB_URL   — PostgreSQL DSN, e.g. postgresql://user:pass@host/db
  S3_BUCKET        — S3 bucket name for matrix files
  CADE_DATA_DIR    — Local path to cade data directory (must be accessible)
  CADE_API_URL     — cade API base URL, e.g. http://localhost:8000

Optional:
  AWS_REGION       — AWS region (default: us-east-1)
"""
from __future__ import annotations
import os
from dataclasses import dataclass
from pathlib import Path
from cade.ingest.exceptions import ConfigError


@dataclass
class Config:
    staging_db_url: str
    s3_bucket: str
    data_dir: Path
    api_url: str
    aws_region: str = "us-east-1"


def load() -> Config:
    """Load and validate configuration from environment variables."""
    errors = []

    staging_db_url = os.environ.get("STAGING_DB_URL", "")
    if not staging_db_url:
        errors.append("STAGING_DB_URL is not set")

    s3_bucket = os.environ.get("S3_BUCKET", "")
    if not s3_bucket:
        errors.append("S3_BUCKET is not set")

    data_dir_str = os.environ.get("CADE_DATA_DIR", "")
    if not data_dir_str:
        errors.append("CADE_DATA_DIR is not set")
    else:
        data_dir = Path(data_dir_str)
        if not data_dir.is_dir():
            errors.append(
                f"CADE_DATA_DIR '{data_dir}' is not accessible. "
                "The pipeline must run on the same host as cade, "
                "or CADE_DATA_DIR must be a shared mount."
            )

    api_url = os.environ.get("CADE_API_URL", "")
    if not api_url:
        errors.append("CADE_API_URL is not set")

    if errors:
        raise ConfigError("Pipeline configuration errors:\n" + "\n".join(f"  - {e}" for e in errors))

    return Config(
        staging_db_url=staging_db_url,
        s3_bucket=s3_bucket,
        data_dir=Path(data_dir_str),
        api_url=api_url.rstrip("/"),
        aws_region=os.environ.get("AWS_REGION", "us-east-1"),
    )
