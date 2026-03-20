class CadeError(Exception):
    """Base class for all cade exceptions."""

class SnapshotNotFound(CadeError):
    def __init__(self, counterparty_id: str, netting_set_id: str, cob_date):
        self.counterparty_id = counterparty_id
        self.netting_set_id = netting_set_id
        self.cob_date = cob_date
        super().__init__(f"No snapshot for {counterparty_id}/{netting_set_id}/{cob_date}")

class IntegrityError(CadeError):
    def __init__(self, stored_hash: str, computed_hash: str):
        self.stored_hash = stored_hash
        self.computed_hash = computed_hash
        super().__init__(f"Hash mismatch: stored={stored_hash!r}, computed={computed_hash!r}")

class DuplicateSnapshotError(CadeError):
    def __init__(self, counterparty_id: str, netting_set_id: str, cob_date):
        super().__init__(f"Snapshot already exists for {counterparty_id}/{netting_set_id}/{cob_date}")

class MatrixReferenceError(CadeError):
    def __init__(self, path: str):
        self.path = path
        super().__init__(f"Matrix path does not exist: {path}")

class InvalidRangeError(CadeError):
    def __init__(self, from_date, to_date):
        super().__init__(f"from_date ({from_date}) must precede to_date ({to_date})")

class InvalidIdError(CadeError):
    def __init__(self, value: str):
        super().__init__(f"Invalid ID (must match [A-Za-z0-9_-]+): {value!r}")
