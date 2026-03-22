"""Tests for cade.ingest.matrix_sync — uses moto to mock S3."""
import hashlib
import pytest
from pathlib import Path

boto3 = pytest.importorskip("boto3")
moto = pytest.importorskip("moto")

from moto import mock_aws
from cade.ingest.matrix_sync import compute_file_hash, download_matrix, matrix_dest_path
from cade.ingest.exceptions import MatrixHashMismatch


BUCKET = "test-bucket"
REGION = "us-east-1"


def _sha256(data: bytes) -> str:
    return "sha256-v1:" + hashlib.sha256(data).hexdigest()


@pytest.fixture
def s3_bucket(tmp_path):
    with mock_aws():
        import boto3
        s3 = boto3.client("s3", region_name=REGION)
        s3.create_bucket(Bucket=BUCKET)
        yield s3, tmp_path


def test_compute_file_hash(tmp_path):
    f = tmp_path / "test.npy"
    f.write_bytes(b"hello matrix")
    h = compute_file_hash(f)
    assert h == _sha256(b"hello matrix")


def test_download_matrix_success(s3_bucket):
    s3, tmp_path = s3_bucket
    data = b"matrix data"
    key = "matrices/2024-03-15/ACME/RATES-USD/IR_SWAP.npy"
    s3.put_object(Bucket=BUCKET, Key=key, Body=data)
    expected = _sha256(data)
    dest = tmp_path / "matrices/2024-03-15/ACME/IR_SWAP.npy"
    download_matrix(s3, BUCKET, key, dest, expected)
    assert dest.exists()
    assert dest.read_bytes() == data


def test_download_matrix_hash_mismatch(s3_bucket):
    s3, tmp_path = s3_bucket
    data = b"matrix data"
    key = "matrices/2024-03-15/ACME/RATES-USD/IR_SWAP.npy"
    s3.put_object(Bucket=BUCKET, Key=key, Body=data)
    wrong_hash = "sha256-v1:" + "0" * 64
    dest = tmp_path / "matrices/2024-03-15/ACME/IR_SWAP.npy"
    with pytest.raises(MatrixHashMismatch):
        download_matrix(s3, BUCKET, key, dest, wrong_hash)
    # Temp file should be cleaned up
    assert not dest.with_suffix(".npy.tmp").exists()


def test_download_matrix_no_temp_file_on_mismatch(s3_bucket):
    s3, tmp_path = s3_bucket
    key = "matrices/2024-03-15/ACME/RATES-USD/IR_SWAP.npy"
    s3.put_object(Bucket=BUCKET, Key=key, Body=b"data")
    dest = tmp_path / "IR_SWAP.npy"
    try:
        download_matrix(s3, BUCKET, key, dest, "sha256-v1:" + "0" * 64)
    except MatrixHashMismatch:
        pass
    assert not dest.with_suffix(".npy.tmp").exists()
    assert not dest.exists()  # not renamed


def test_already_downloaded_file_not_overwritten(s3_bucket, tmp_path):
    """If dest already exists, download_matrix should overwrite safely via temp+rename."""
    s3, _ = s3_bucket
    data = b"new matrix data"
    key = "matrices/2024-03-15/ACME/RATES-USD/IR_SWAP.npy"
    s3.put_object(Bucket=BUCKET, Key=key, Body=data)
    expected = _sha256(data)
    dest = tmp_path / "IR_SWAP.npy"
    dest.write_bytes(b"old data")  # pre-existing file
    download_matrix(s3, BUCKET, key, dest, expected)
    assert dest.read_bytes() == data
