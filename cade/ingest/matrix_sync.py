"""Download matrix files from S3 to the local CADE_DATA_DIR.

Download flow:
  1. Download S3 object to a .tmp file
  2. Compute sha256-v1: hash of the downloaded bytes
  3. Compare against expected_hash (from staging or S3 metadata)
  4. On match: atomic rename to final path
  5. On mismatch: delete temp file, raise MatrixHashMismatch

The final path is relative to data_dir:
  matrices/{cob_date}/{counterparty_id}/{instrument}.{ext}

This path is what gets stored in COBSnapshot.market_data.price_matrices.
"""
from __future__ import annotations

import hashlib
import logging
import os
from pathlib import Path

from cade.ingest.exceptions import MatrixHashMismatch

logger = logging.getLogger(__name__)


def compute_file_hash(path: Path) -> str:
    """Compute sha256-v1: hash of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return "sha256-v1:" + h.hexdigest()


def download_matrix(
    s3_client,
    bucket: str,
    s3_key: str,
    dest: Path,
    expected_hash: str,
) -> str:
    """Download an S3 object to dest, verify hash, return final path string.

    Uses atomic temp-file + rename to ensure cade never sees a partial file.
    Raises MatrixHashMismatch if the downloaded file does not match expected_hash.

    Args:
        s3_client: boto3 S3 client.
        bucket: S3 bucket name.
        s3_key: S3 object key.
        dest: Absolute destination path (final, not temp).
        expected_hash: Expected sha256-v1: hash string.

    Returns:
        The hash of the downloaded file (same as expected_hash on success).
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".tmp")

    try:
        logger.debug("Downloading s3://%s/%s → %s", bucket, s3_key, tmp)
        s3_client.download_file(bucket, s3_key, str(tmp))

        actual_hash = compute_file_hash(tmp)
        if actual_hash != expected_hash:
            tmp.unlink(missing_ok=True)
            raise MatrixHashMismatch(s3_key, expected_hash, actual_hash)

        tmp.rename(dest)
        logger.debug("Matrix saved: %s (hash OK)", dest)
        return actual_hash

    except MatrixHashMismatch:
        raise
    except Exception:
        tmp.unlink(missing_ok=True)
        raise


def matrix_dest_path(data_dir: Path, cob_date, counterparty_id: str, s3_key: str) -> Path:
    """Derive the local destination path from the S3 key.

    S3 key convention: matrices/{cob_date}/{counterparty_id}/{filename}
    Local path:        {data_dir}/matrices/{cob_date}/{counterparty_id}/{filename}
    """
    # Strip any leading prefix to get just the relative part
    rel = s3_key  # already in the right format
    return data_dir / rel
