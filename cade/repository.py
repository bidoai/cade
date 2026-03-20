from abc import ABC, abstractmethod
from datetime import date

from cade.models import COBSnapshot, ExposureSummary, SnapshotDiff


class AgreementRepository(ABC):
    """Abstract storage interface for cade.

    All backends implement this interface. All tests bind to it.

    Contract:
    - Snapshots are immutable. Re-writing raises DuplicateSnapshotError.
    - data_hash is set by store_snapshot(), never by the caller.
    - exposure_total is provided by the caller; cade does not compute it.
    """

    @abstractmethod
    def store_snapshot(self, snapshot: COBSnapshot, exposure_total: float) -> COBSnapshot:
        """Persist snapshot. Sets data_hash. Returns snapshot with hash populated.

        Raises: DuplicateSnapshotError, MatrixReferenceError, ValueError (NaN/Inf).
        """

    @abstractmethod
    def get_snapshot(self, counterparty_id: str, netting_set_id: str, cob_date: date) -> COBSnapshot:
        """Retrieve and hash-verify a snapshot.

        Raises: SnapshotNotFound, IntegrityError.
        """

    @abstractmethod
    def list_cob_dates(self, counterparty_id: str, netting_set_id: str) -> list[date]:
        """All COB dates with snapshots, sorted ascending."""

    @abstractmethod
    def list_netting_sets(self, counterparty_id: str) -> list[str]:
        """All netting set IDs for a counterparty."""

    @abstractmethod
    def get_portfolio(
        self,
        cob_date: date,
        threshold: float | None = None,
        top_n: int | None = None,
    ) -> tuple[list[ExposureSummary], bool]:
        """Portfolio exposure ranking. Returns (summaries, index_stale)."""

    def get_diff(
        self,
        counterparty_id: str,
        netting_set_id: str,
        from_date: date,
        to_date: date,
    ) -> SnapshotDiff:
        """Diff two snapshots. Implemented via get_snapshot() — no backend override needed.

        Raises: InvalidRangeError, SnapshotNotFound.
        """
        from cade.exceptions import InvalidRangeError
        from cade.diff import compute_diff

        if from_date >= to_date:
            raise InvalidRangeError(from_date, to_date)

        snap_from = self.get_snapshot(counterparty_id, netting_set_id, from_date)
        snap_to = self.get_snapshot(counterparty_id, netting_set_id, to_date)
        return compute_diff(snap_from, snap_to)
