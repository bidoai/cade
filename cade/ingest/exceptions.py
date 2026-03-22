"""Exceptions specific to the cade ingestion pipeline."""

class IngestError(Exception):
    """Base class for ingestion pipeline errors."""

class ConfigError(IngestError):
    """Raised when required configuration is missing or invalid."""

class MatrixHashMismatch(IngestError):
    """Raised when a downloaded matrix file's hash does not match the expected hash."""
    def __init__(self, s3_key: str, expected: str, actual: str):
        self.s3_key = s3_key
        self.expected = expected
        self.actual = actual
        super().__init__(
            f"Matrix hash mismatch for {s3_key}: expected {expected}, got {actual}"
        )

class StagingDataError(IngestError):
    """Raised when staging data is malformed or incomplete."""
